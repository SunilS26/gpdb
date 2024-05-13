package hub

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"time"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpdb/gp/constants"
	"github.com/greenplum-db/gpdb/gp/idl"
	"github.com/greenplum-db/gpdb/gp/utils"
	"github.com/robfig/cron/v3"
)

// Schedule the task to be run on configured interval/time.
func (s *Server) ScheduleTasks(ctx context.Context, in *idl.ScheduleTasksRequest) (*idl.ScheduleTasksReply, error) {
	gplog.Info("ScheduleTasks operation requested")

	// Schedule all the task given in the task list
	scheduleTasksStatus := make([]string, len(in.TaskList))
	for index, taskName := range in.TaskList {
		err := s.InitializeAndStartTask(taskName, "", "", NA, NA, NA)
		if err != nil {
			gplog.Error("failed to start the task %s: %s", taskName, err)
			scheduleTasksStatus[index] = "FAILED"
		} else {
			gplog.Info("Successfully scheduled tasks %s", taskName)
			scheduleTasksStatus[index] = "SUCCESS"
		}
	}

	return &idl.ScheduleTasksReply{Status: scheduleTasksStatus}, nil
}

// Cron schedule parser to get the next scheduled time for task to run
// Note: Cron has the capability to schedule the task by itself , but here we are only limiting
// cron package to get the next scheduled time, to keep lower dependecy and higher control
// on the task operation.
// TODO: Check if using the cron package as a task manager to schedule and unschedule taks would be better or not.
// This might create a dependency with cron package.
// Whereas with current usage we can change the cron package to any other or implement our own.
func (s *Server) NextScheduledTime(taskName string, schedule string) (time.Time, error) {
	gplog.Debug("Received schedule value %s for task %s", schedule, taskName)

	// Parse the cron schedule string
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	scheduleSpec, err := parser.Parse(schedule)
	if err != nil {
		return time.Time{}, fmt.Errorf("error parsing task %s schedule: %s", taskName, err)
	}

	// Get next schedule taking current time as the base
	next := time.Now()
	next = scheduleSpec.Next(next)

	gplog.Info("Task[%s] : Next Schedule %s", taskName, next.Format(time.RFC3339))
	return next, nil
}

func ExecuteCommandInBackground(cmdStr string, outputBuffer *bytes.Buffer) (*exec.Cmd, error) {
	gplog.Debug("Received command to run %s", cmdStr)

	cmd := exec.Command("bash", "-c", cmdStr)
	cmd.Stdout = outputBuffer
	cmd.Stderr = outputBuffer

	// Execute the given command in background
	err := cmd.Start()
	if err != nil {
		return nil, fmt.Errorf("command `%s` failed with error: %s and output: %s", cmdStr, err, outputBuffer.String())
	}

	// Get the command process ID
	pid := cmd.Process.Pid
	gplog.Debug("Command %s process ID: %d", cmdStr, pid)

	return cmd, nil
}

func (s *Server) ExecuteTaskCommand(cmdStr string, maxRetries uint16, outputBuffer *bytes.Buffer) (*exec.Cmd, error) {
	gplog.Debug("Executing command \"%s\"", cmdStr)

	// Run the command till maxRetries. For any error till maxRetries log a Error log and keep continuing
	var err error
	for cnt := uint16(0); cnt <= maxRetries; cnt++ {
		gplog.Debug("[%d]: Running the Command -\" %s\"", cnt, cmdStr)
		cmd, err := ExecuteCommandInBackground(cmdStr, outputBuffer)
		// Keep continue for any error till maxRetries.
		if err != nil {
			gplog.Error("retry attempt count %d , command %s failed with error %s", cnt+1, cmdStr, err)
		} else {
			return cmd, nil
		}
	}

	return nil, fmt.Errorf("failed to execute command %s with error %s ", cmdStr, err)
}

// Wait for the executed command to complete and after successful log the output
func waitForCommandToComplete(cmd *exec.Cmd, outputBuffer *bytes.Buffer) error {
	gplog.Debug("Waiting for command to complete")

	// Wait for the command to finish executing
	err := cmd.Wait()
	if err != nil {
		return fmt.Errorf("command %s failed with error:%s", cmd.Args[0], err)
	}

	// Retrieve output from buffers
	gplog.Debug("Command Successful : %s", outputBuffer.Bytes())

	return nil
}

func (s *Server) ExecutePreHook(taskArgs *TaskDescriptor, streamSender ...streamArgs) error {
	var outputBuffer bytes.Buffer

	if taskArgs.stopTaskCmdState == RUNNING {
		return fmt.Errorf("task stop is called hence skipping")
	}

	taskArgs.TaskCmdStatus.PreHookStatus = RUNNING
	cmd, err := s.ExecuteTaskCommand(taskArgs.taskConfigOptions.PreHook, taskArgs.taskConfigOptions.MaxRetriesOnError, &outputBuffer)
	if err != nil {
		taskArgs.TaskCmdStatus.PreHookStatus = FAILED
		return fmt.Errorf("PreHook Command \"%s\" of task \"%s\" failed %w", taskArgs.taskConfigOptions.PreHook, taskArgs.taskConfigOptions.Name, err)
	} else {
		taskArgs.taskCmdPID.PreHookPID = cmd.Process.Pid

		// Check if the optional argument is provided and send the in-progress stream buffer to client
		// This stream buffer output which contains the PID of the command is used to kill process manually
		// if the implicit command cleanup(task command cleanup) fails for some reason.
		if len(streamSender) > 0 {
			sendStreamData(&streamSender[0], taskArgs)
		}

		err = waitForCommandToComplete(cmd, &outputBuffer)
		if err != nil {
			return fmt.Errorf("wait for command failed:%s", err)
		}

		// Reset it to Zero
		taskArgs.taskCmdPID.PreHookPID = 0
		gplog.Debug("Successfully executed PreHook Command \"%s\" of task \"%s\"", taskArgs.taskConfigOptions.PreHook, taskArgs.taskConfigOptions.Name)
		taskArgs.TaskCmdStatus.PreHookStatus = SUCCEEDED

		// Check if the optional argument is provided and send the command successful stream buffer to client
		if len(streamSender) > 0 {
			sendStreamData(&streamSender[0], taskArgs)
		}
	}
	return nil
}

func (s *Server) ExecuteMainCmd(taskArgs *TaskDescriptor, streamSender ...streamArgs) error {
	var outputBuffer bytes.Buffer

	if taskArgs.stopTaskCmdState == RUNNING {
		return fmt.Errorf("task stop is called hence skipping")
	}

	taskArgs.TaskCmdStatus.CmdStatus = RUNNING
	cmd, err := s.ExecuteTaskCommand(taskArgs.taskConfigOptions.Cmd, taskArgs.taskConfigOptions.MaxRetriesOnError, &outputBuffer)
	if err != nil {
		taskArgs.TaskCmdStatus.CmdStatus = FAILED
		return fmt.Errorf("command \"%s\" of task \"%s\" failed with error %s", taskArgs.taskConfigOptions.Cmd, taskArgs.taskConfigOptions.Name, err)
	} else {
		taskArgs.taskCmdPID.CmdPID = cmd.Process.Pid

		// Check if the optional argument is provided and send the in-progress stream buffer to client
		if len(streamSender) > 0 {
			sendStreamData(&streamSender[0], taskArgs)
		}

		err = waitForCommandToComplete(cmd, &outputBuffer)
		if err != nil {
			return fmt.Errorf("wait for command failed:%s", err)
		}

		gplog.Info("Successfully executed the main command \"%s\" of task \"%s\"", taskArgs.taskConfigOptions.Cmd, taskArgs.taskConfigOptions.Name)
		taskArgs.TaskCmdStatus.CmdStatus = SUCCEEDED
		taskArgs.taskCmdPID.CmdPID = 0

		// Check if the optional argument is provided and send the successful command stream buffer to client
		if len(streamSender) > 0 {
			sendStreamData(&streamSender[0], taskArgs)
		}
	}

	return nil
}

func (s *Server) ExecutePostHook(taskArgs *TaskDescriptor, streamSender ...streamArgs) error {
	var outputBuffer bytes.Buffer

	if taskArgs.stopTaskCmdState == RUNNING {
		return fmt.Errorf("task stop is called hence skipping")
	}

	taskArgs.TaskCmdStatus.PostHookStatus = RUNNING
	cmd, err := s.ExecuteTaskCommand(taskArgs.taskConfigOptions.PostHook, taskArgs.taskConfigOptions.MaxRetriesOnError, &outputBuffer)
	if err != nil {
		taskArgs.TaskCmdStatus.PostHookStatus = FAILED
		return fmt.Errorf("posthook command \"%s\" failed for task \"%s\" with error %s", taskArgs.taskConfigOptions.PostHook, taskArgs.taskConfigOptions.Name, err)
	} else {
		taskArgs.taskCmdPID.PostHookPID = cmd.Process.Pid

		// Check if the optional argument is provided and send the in-progress stream buffer to client
		if len(streamSender) > 0 {
			sendStreamData(&streamSender[0], taskArgs)
		}

		err = waitForCommandToComplete(cmd, &outputBuffer)
		if err != nil {
			return fmt.Errorf("command failed:%s", err)
		}

		gplog.Debug("Successfully executed PostHook Command \"%s\" for the task \"%s\"", taskArgs.taskConfigOptions.PostHook, taskArgs.taskConfigOptions.Name)
		taskArgs.TaskCmdStatus.PostHookStatus = SUCCEEDED
		taskArgs.taskCmdPID.PostHookPID = 0

		// Check if the optional argument is provided and send the successful command stream buffer to client
		if len(streamSender) > 0 {
			sendStreamData(&streamSender[0], taskArgs)
		}
	}
	return nil
}

// Handle all the command execution of the task like Pre-Hook, Main task Command and Post-Hook.
// If any of the command fails then next command is not executed.
// There is a chaing between each commands "Pre-Hook -- Main task Command -- Post-Hook".
func (s *Server) ExecuteTask(taskArgs *TaskDescriptor, streamSender ...streamArgs) error {
	gplog.Debug("ExecuteTask called for task %+v", taskArgs)
	var err error

	//-----------------Pre-Hook Command---------------------------
	// Run the pre hook command if it is configured
	if taskArgs.taskConfigOptions.PreHook != "" {
		// Check if the optional argument is provided
		if len(streamSender) > 0 {
			err = s.ExecutePreHook(taskArgs, streamSender[0])
		} else {
			err = s.ExecutePreHook(taskArgs)
		}
		if err != nil {
			return err
		}
	}

	//-----------------Main Task Command---------------------------
	// Run the main command
	// Check if the optional argument is provided
	if len(streamSender) > 0 {
		err = s.ExecuteMainCmd(taskArgs, streamSender[0])
	} else {
		err = s.ExecuteMainCmd(taskArgs)
	}
	if err != nil {
		return err
	}

	//-----------------Post-Hook Command---------------------------
	// Run the post hook if it is configured
	if taskArgs.taskConfigOptions.PostHook != "" {
		// Check if the optional argument is provided
		if len(streamSender) > 0 {
			err = s.ExecutePostHook(taskArgs, streamSender[0])
		} else {
			err = s.ExecutePostHook(taskArgs)
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) GetNextScheduledTaskDuration(taskArgs *TaskDescriptor, NextScheduledTime string) (time.Duration, error) {
	// Fetch the NextSchedule time and configure the timer to run the task
	var duration time.Duration
	if NextScheduledTime == "" {
		gplog.Debug("fetching the next schedule:")
		NextSchedule, err := s.NextScheduledTime(taskArgs.taskConfigOptions.Name, taskArgs.taskConfigOptions.Schedule)
		if err != nil {
			return 0, fmt.Errorf("error getting next schedule time: %s", err)
		}

		// Set timer to next schedule
		duration = time.Until(NextSchedule)

		// set the next scheduled time
		taskArgs.TaskCmdStatus.NextScheduledTime = NextSchedule.Format("2006-01-02 15:04:05")
	} else { // For the NextSchedule fetched from history file during grpc service crash recovery
		gplog.Debug("Found existing schedule:")
		// Parse the string into a time.Time value
		// this scenarios is applicable when a task is reloaded during service crash
		parsedTime, err := time.Parse("2006-01-02 15:04:05", NextScheduledTime)
		if err != nil {
			return 0, fmt.Errorf("error parsing time: %s", err)
		}
		duration = time.Until(parsedTime)
	}

	return duration, nil
}

// Core functionality of the task is implemented here. For each task, a new goroutine is executed
// which handles the subsequent task execution in the background according to the configured schedule.
//
// Tasks are scheduled in blocking mode when it is initially added to the framework.
// There are two modes of creating an interval for the task:
//
// Method-1: Schedule between two execution entity
// Once the task is scheduled to run, the next interval is decided based on the current execution finished time.
//
// Method-2:(pg_cron) Interval as per its schedule
// Task interval is created as per its schedule.If the previous interval bypasses the next interval,
// the execution is queued, and then the task executed immediately after current execution ends.
// This aligns with pg_cron style of scheduling task.
//
// Here Method-1 is used to schedule task
func (s *Server) scheduleTask(taskName string, NextScheduledTime string) error {
	// Create a channel to receive errors from the goroutine if task pre-configuration fails
	errChan := make(chan error)
	doneChan := make(chan bool)

	// Run a Task Continuously at the scheduled time till the task is stopped
	go func() {
		s.taskMutexRWLock.RLock()
		taskArgs := s.activeTaskList[taskName]
		s.taskMutexRWLock.RUnlock()

		// Get the next duration of the task to execute
		nextDuration, err := s.GetNextScheduledTaskDuration(taskArgs, NextScheduledTime)
		if err != nil {
			errChan <- err
		}

		//Create a timer for the next scheduled task.
		timer := time.NewTimer(nextDuration)
		defer timer.Stop()

		// Initial setup is successful, now the caller can be unblocked
		doneChan <- true

		for {
			select {
			case timerTick := <-timer.C:
				taskArgs.currentTaskExecutionState = RUNNING
				startFormattedTime := timerTick.Format("2006-01-02 15:04:05")
				gplog.Verbose("Task execution started with timer ticked at %s", startFormattedTime)

				// For any error the retry will be done in next attempt
				err := s.ExecuteTask(taskArgs)
				if err != nil {
					taskArgs.currentTaskExecutionState = FAILED
					gplog.Error("Task %s execution failed : %s", taskName, err)
				}

				// Update the duration for the next scheduled time
				NextSchedule, err := s.NextScheduledTime(taskName, taskArgs.taskConfigOptions.Schedule)
				if err != nil {
					gplog.Error("error getting next schedule time for task %s: %s", taskArgs.taskConfigOptions.Name, err)
					return
				}

				// Reset the timer to the new interval
				durationUntilNext := time.Until(NextSchedule)
				timer.Reset(durationUntilNext)

				taskArgs.TaskCmdStatus.LastTriggeredTime = startFormattedTime
				taskArgs.TaskCmdStatus.NextScheduledTime = NextSchedule.Format("2006-01-02 15:04:05")

				taskArgs.currentTaskExecutionState = SUCCEEDED
				gplog.Verbose("Task %s scheduled to run in another %s at %s", taskName, durationUntilNext, taskArgs.TaskCmdStatus.NextScheduledTime)
			case task := <-taskArgs.updateTaskChan:
				gplog.Verbose("Updating the task %s", taskName)

				taskArgs.taskConfigOptions.PreHook = task.PreHook
				taskArgs.taskConfigOptions.Cmd = task.Cmd
				taskArgs.taskConfigOptions.PostHook = task.PostHook
				taskArgs.taskConfigOptions.MaxRetriesOnError = task.MaxRetriesOnError

				// Schedule needs to be changed explicitly  to reflect the current value.
				if taskArgs.taskConfigOptions.Schedule != task.Schedule {
					// Update the duration for the next scheduled time
					NextSchedule, err := s.NextScheduledTime(taskName, taskArgs.taskConfigOptions.Schedule)
					if err != nil {
						gplog.Error("error getting next schedule time for task %s: %s", taskArgs.taskConfigOptions.Name, err)
					}

					durationUntilNext := time.Until(NextSchedule)
					timer.Reset(durationUntilNext)
					taskArgs.TaskCmdStatus.NextScheduledTime = NextSchedule.Format("2006-01-02 15:04:05")
					gplog.Verbose("Updated Task %s schedule to run in another %s at %s", taskName, durationUntilNext, taskArgs.TaskCmdStatus.NextScheduledTime)
				}
			case <-taskArgs.stopTaskChan:
				timer.Stop()
				// Stop the task
				gplog.Info("Received Shutdown signal, Shutting down task %s", taskName)
				return
			}
		}
	}()

	// Handle initial errors of scheduling
	select {
	case err := <-errChan:
		if err != nil {
			return err
		}
	case <-doneChan:
	}

	return nil
}

// Most of the task parametrs are taken since this function can be reused during service crash where
// the previous data is loaded here as a paramter.
func (s *Server) InitializeAndStartTask(taskName string, LastTriggeredTime string, NextScheduledTime string, PreHookStatus TaskExecutionState, CmdStatus TaskExecutionState, PostHookStatus TaskExecutionState) error {
	gplog.Info("Initializing the task %s", taskName)

	s.taskMutexRWLock.RLock()
	// Check if any existing task is present, if yes then return
	if _, ok := s.activeTaskList[taskName]; ok {
		s.taskMutexRWLock.RUnlock()
		gplog.Info("task %s already running", taskName)
		return nil
	}
	s.taskMutexRWLock.RUnlock()

	// Load the config file and get the task data
	configTaskOptions, err := utils.LoadTaskConfigFileWithValidation(s.GpHome, constants.TaskConfigFileName)
	if err != nil {
		return fmt.Errorf("error loading task config file: %s", err)
	}

	// add the task to active task list and schedule the task to run
	for _, task := range configTaskOptions {
		gplog.Debug("Comparing Schedule task %s with task in configuration %s ", taskName, task.Name)
		if taskName == task.Name {
			gplog.Debug("Adding task %s to active task list", taskName)

			// Construct a task configuration paramters
			taskConfig := NewTaskConfig()
			taskConfig.taskConfigOptions = task
			taskConfig.TaskCmdStatus.PreHookStatus = PreHookStatus
			taskConfig.TaskCmdStatus.CmdStatus = CmdStatus
			taskConfig.TaskCmdStatus.PostHookStatus = PostHookStatus
			taskConfig.TaskCmdStatus.LastTriggeredTime = LastTriggeredTime
			taskConfig.TaskCmdStatus.NextScheduledTime = NextScheduledTime

			// For new task, add it to task list and schedule the task
			// Delete an entry from the map
			s.AddTaskToActiveTaskList(task.Name, taskConfig)

			gplog.Debug("Current Active Task in List %+v", s.activeTaskList[task.Name])
			break
		}
	}

	// Schedule the task to run as per it's configured interval
	err = s.scheduleTask(taskName, NextScheduledTime)
	if err != nil {
		// remove the task from the active task list
		s.DeleteTaskFromActiveTaskList(taskName)
		return err
	}

	return nil
}

func (s *Server) AddTaskToActiveTaskList(taskName string, taskArg *TaskDescriptor) {
	gplog.Debug("Adding task %s to active task list with value %+v", taskName, taskArg)
	s.taskMutexRWLock.Lock()
	s.activeTaskList[taskName] = taskArg
	s.taskMutexRWLock.Unlock()
}

func (s *Server) DeleteTaskFromActiveTaskList(taskName string) {
	gplog.Debug("Deleting task %s from active task list", taskName)
	s.taskMutexRWLock.Lock()
	delete(s.activeTaskList, taskName)
	s.taskMutexRWLock.Unlock()
}
