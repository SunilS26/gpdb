package hub

import (
	"bytes"
	"context"
	"fmt"

	"os"
	"os/exec"
	"time"

	"github.com/greenplum-db/gpdb/gp/idl"
	"github.com/greenplum-db/gpdb/gp/utils"
	"github.com/robfig/cron/v3"
	"gopkg.in/yaml.v2"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

// Schedule the task to be run on configured interval/time.
func (s *Server) ScheduleTasks(ctx context.Context, in *idl.TaskScheduleServiceRequest) (*idl.TaskScheduleServiceReply, error) {
	gplog.Info("Task StartTasks operation requested for tasks %s", in.TaskList)

	// Schedule all the task given in the task list
	taskExitStatus := make([]string, len(in.TaskList))
	for i, taskName := range in.TaskList {
		err := s.InitializeAndStartTask(taskName, "", "", false, false, false, false)
		if err != nil {
			gplog.Warn("failed to start the task %s: %s", taskName, err)
			taskExitStatus[i] = "FAILED"
		} else {
			taskExitStatus[i] = "SUCCESS"
		}
	}

	gplog.Info("Successfully started tasks %v", s.activeTaskList)
	return &idl.TaskScheduleServiceReply{Status: taskExitStatus}, nil
}

// Cron schedule parser
func (s *Server) NextScheduledTime(taskName string) time.Time {
	task := s.activeTaskList[taskName]
	schedule := task.taskConfigData.Schedule

	// Parse the cron schedule string
	gplog.Info("Received schedule value %s", schedule)
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	scheduleSpec, err := parser.Parse(schedule)
	if err != nil {
		gplog.Info("Error parsing cron schedule:%s", err)
		return time.Time{}
	}

	next := time.Now()
	next = scheduleSpec.Next(next)
	gplog.Info("Next Schedule %s", next.Format(time.RFC3339))

	return next
}
func ExecuteCommandInBackground(cmdStr string, stdout, stderr bytes.Buffer) (int, *exec.Cmd, error) {
	gplog.Info("Received command to run %s", cmdStr)

	cmd := exec.Command("bash", "-c", cmdStr)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Start()
	if err != nil {
		gplog.Info("command `%s` failed : %s with ", cmdStr, err)
		return -1, nil, err
	}

	// Get the process ID
	pid := cmd.Process.Pid
	gplog.Info("Process ID: %d", pid)

	return pid, cmd, nil
}

func ExecuteCommand(cmdStr string) (string, error) {
	gplog.Info("Received command to run %s", cmdStr)

	cmd := exec.Command("bash", "-c", cmdStr)
	output, err := cmd.CombinedOutput()
	if err != nil {
		gplog.Info("command `%s` failed : %s with output:%s", cmdStr, err, output)
		return string(output), err
	}

	return string(output), nil
}

func (s *Server) ExecuteTaskCommands(cmdStr string, maxRetries uint16) error {
	gplog.Info("Executing command \"%s\"", cmdStr)

	// Run the command
	for cnt := uint16(0); cnt <= maxRetries; cnt++ {
		gplog.Info("[%d]: Running the Command -\" %s\"", cnt, cmdStr)
		output, err := ExecuteCommand(cmdStr)
		if err != nil {
			gplog.Warn("command `%s` failed : %s", cmdStr, err)
			gplog.Debug("could not execute, retry attempt cnt %d ", cnt+1)
		}
		if err == nil {
			gplog.Info("Successfully executed task operation, \nCommand:\n%s \nOutput:\n %s", cmdStr, output)
			return nil
		}
	}

	return nil
}

// Default behaviour is to keep trying if onerror is mot specifies explicitly
// OnError for hook is default skip and for whole is also skip
func (s *Server) ExecuteTask(taskArgs utils.TaskConfigOptions, cmdExitStatus *TaskExitStatus) error {
	gplog.Info("ExecuteTask called for task %v", taskArgs)

	// Run the pre hook command
	if taskArgs.PreHook != "" {
		err := s.ExecuteTaskCommands(taskArgs.PreHook, taskArgs.MaxRetriesOnError)
		if err != nil {
			gplog.Info("PreHook Command \"%s\" of task \"%s\" failed %s", taskArgs.PreHook, taskArgs.Name, err)
			cmdExitStatus.PreHookStatus = false
		} else {
			gplog.Info("Successfully executed PreHook Command \"%s\" of task \"%s\"", taskArgs.PreHook, taskArgs.Name)
			cmdExitStatus.PreHookStatus = true
		}
	}

	// if Pre-Hook successful then run the command
	// Fixme: Fix environmental variable access to recognize the command
	cmdStrWithPath := fmt.Sprintf("source ~/.bashrc && %s", taskArgs.GpdrCmd)
	err := s.ExecuteTaskCommands(cmdStrWithPath, taskArgs.MaxRetriesOnError)
	if err != nil {
		gplog.Info("Gpdr Command \"%s\" of task \"%s\" failed %s", taskArgs.GpdrCmd, taskArgs.Name, err)
		cmdExitStatus.CmdStatus = false
	} else {
		gplog.Info("Successfully executed Gpdr Command \"%s\" of task \"%s\"", taskArgs.GpdrCmd, taskArgs.Name)
		cmdExitStatus.CmdStatus = true
	}

	if taskArgs.PostHook != "" {
		// if successful then run the post hook
		err = s.ExecuteTaskCommands(taskArgs.PostHook, taskArgs.MaxRetriesOnError)
		if err != nil {
			gplog.Info("PostHook Command \"%s\" of task \"%s\" failed %s", taskArgs.PostHook, taskArgs.Name, err)
			cmdExitStatus.PostHookStatus = false
		} else {
			gplog.Info("Successfully executed PostHook Command \"%s\" of task \"%s\"", taskArgs.PostHook, taskArgs.Name)
			cmdExitStatus.PostHookStatus = true
		}
	}

	return nil
}

func (s *Server) scheduleTask(taskName string, NextScheduledTime string) {
	gplog.Debug("Scheduling task %s", NextScheduledTime)

	// Fetch the NextSchedule time and configure the ticker to run the task
	var duration time.Duration
	if NextScheduledTime == "" {
		gplog.Debug(".......Didn't find any existing schedule:")
		duration = time.Until(s.NextScheduledTime(taskName))
	} else {
		gplog.Debug("Found existing schedule:")
		// Parse the string into a time.Time value
		//parsedTime, err := time.Parse("2006-01-02 15:04:05", NextScheduledTime)
		parsedTime, err := time.Parse(time.RFC3339, NextScheduledTime)
		if err != nil {
			gplog.Debug("Error parsing time: %s", err)
		}
		duration = time.Until(parsedTime)
	}

	ticker := time.NewTicker(duration)

	gplog.Info("Task %s scheduled to run in %s", taskName, duration)
	// Run a Task Continuously at the scheduled time till the task is stopped
	go func() {
		for {
			// Code to be executed at the desired time
			// Get the task Arguments
			// Sunil: Check lock requirement by analyzing the concurrency
			taskArgs := s.activeTaskList[taskName]

			select {
			case <-ticker.C:
				gplog.Info("Task is %v ", taskArgs)

				//formattedTime := time.Now().Format("2006-01-02 15:04:05")
				formattedTime := time.Now().Format(time.RFC3339)

				if taskArgs == nil {
					//return fmt.Errorf("task %s not found in the task list", taskName)
				}
				err := s.ExecuteTask(taskArgs.taskConfigData, &taskArgs.TaskStatus)
				if err != nil {
					gplog.Warn("Task %s execution failed : %s", taskName, err)
				}

				taskArgs.LastTriggeredTime = formattedTime
				gplog.Info("Task %s is running at %s", taskName, formattedTime)

				// Save the active task in a yaml file for any new task added
				// Because this is used if service is restarted during crash/ host reboot
				// Keep updating the LastTriggeredTime
				gplog.Info("Saving the active task list")
				err = s.SaveActiveTasks()
				if err != nil {
					gplog.Warn("error saving active task list: %s", err)
				}

				// Update the duration for the next scheduled time
				NextSchedule := s.NextScheduledTime(taskName)
				duration = time.Until(NextSchedule)
				ticker.Reset(duration)
				//taskArgs.NextScheduledTime = NextSchedule.Format("2006-01-02 15:04:05")
				taskArgs.NextScheduledTime = NextSchedule.Format(time.RFC3339)
				gplog.Info("Task %s scheduled to run in another %s", taskName, duration)

			case <-taskArgs.updateTask:
				gplog.Debug("Updating the task %s", taskName)

				// Update the duration for the next scheduled time
				NextSchedule := s.NextScheduledTime(taskName)
				duration = time.Until(NextSchedule)
				ticker.Reset(duration)
				//taskArgs.NextScheduledTime = NextSchedule.Format("2006-01-02 15:04:05")
				taskArgs.NextScheduledTime = NextSchedule.Format(time.RFC3339)
				gplog.Info("Task %s scheduled to run in another %s", taskName, duration)

			case <-taskArgs.stopChan:
				// Stop the task
				gplog.Info("Terminating the task %s", taskName)
				ticker.Stop()
				s.mutex.Lock()
				// Delete an entry from the map
				delete(s.activeTaskList, taskName)
				s.mutex.Unlock()

				// Save the active task in a yaml file for any new task added
				// Because this is used if service is restarted during crash/ host reboot
				// Keep updating the LastTriggeredTime
				gplog.Info("Saving the active task list")
				err := s.SaveActiveTasks()
				if err != nil {
					gplog.Warn("error saving active task list: %s", err)
				}

				return
			}
		}
	}()
}

func (s *Server) InitializeAndStartTask(taskName string, LastTriggeredTime string, NextScheduledTime string, PreHookStatus bool, CmdStatus bool, PostHookStatus bool, isDefaultTask bool) error {
	gplog.Info("Initializing the task %s", taskName)

	// Check if any existing task is present, if yes then return
	if _, ok := s.activeTaskList[taskName]; ok {
		return fmt.Errorf("task %s already running", taskName)
	}

	// Load the config file and get the task data
	var configTaskOptions []utils.TaskConfigOptions
	var err error
	if isDefaultTask {
		configTaskOptions, err = utils.LoadTaskConfigFileWithValidation(utils.GetServicePreloadTasksPath())
	} else {
		configTaskOptions, err = utils.LoadTaskConfigFileWithValidation(utils.GetServiceTaskConfigPath())
	}
	if err != nil {
		return fmt.Errorf("error loading task config file: %w", err)
	}

	// add the task to active task list and schedule the task to run
	for _, task := range configTaskOptions {
		gplog.Info("Matching task %s with %s ", taskName, task.Name)
		if taskName == task.Name {
			gplog.Info("Adding task %s", taskName)

			s.mutex.Lock()
			// For new task, add it to task list and schedule the task
			s.activeTaskList[task.Name] = &TaskDataInfo{
				taskConfigData: task,
				TaskStatus: TaskExitStatus{
					PreHookStatus:  PreHookStatus,
					CmdStatus:      CmdStatus,
					PostHookStatus: PostHookStatus,
				},
				LastTriggeredTime: LastTriggeredTime,
				NextScheduledTime: NextScheduledTime,
				stopChan:          make(chan bool),
				updateTask:        make(chan struct{}, 1),
			}
			s.mutex.Unlock()
			// If it is always task then just run a execute command in a go routine
			if task.Schedule == "NA" {
				gplog.Info("Running the task %s in background since schedule is %s", taskName, task.Schedule)
				go func() {
					var stdout, stderr bytes.Buffer
					pid, cmd, err := ExecuteCommandInBackground(task.GpdrCmd, stdout, stderr)
					if err != nil {
						gplog.Warn("Task %s execution failed :%d  %s", taskName, pid, err)
						return
					}
					s.activeTaskList[task.Name].pid = pid
					gplog.Info("Task %s is running with PID %d, waiting to complete...........", taskName, pid)
					err = cmd.Wait() // Wait for the command to finish executing
					if err != nil {
						gplog.Info("Command failed:%s", err)
						return
					}

					// Retrieve output from buffers
					outStr, errStr := string(stdout.Bytes()), string(stderr.Bytes())
					gplog.Info("Output:", outStr)
					gplog.Info("Error:", errStr)
				}()
				time.Sleep(2 * time.Second)
				return nil

			}

			gplog.Info("Task %+v", s.activeTaskList[task.Name])
			if NextScheduledTime == "" {
				gplog.Debug("Didn't find any existing schedule:")
				NextSchedule := s.NextScheduledTime(taskName)
				//NextScheduledTime = NextSchedule.Format("2006-01-02 15:04:05")
				NextScheduledTime = NextSchedule.Format(time.RFC3339)

			}
			s.activeTaskList[task.Name].NextScheduledTime = NextScheduledTime

			// Save the active task in a yaml file for any new task added
			// Because this is used if service is restarted during crash/ host reboot
			if LastTriggeredTime != "" {
				gplog.Info("Saving the active task list")
				err := s.SaveActiveTasks()
				if err != nil {
					gplog.Warn("error saving active task list: %s", err)
				}
			}

			break
		}
	}
	s.scheduleTask(taskName, NextScheduledTime)

	return nil
}

// Active tasks are stored to reload during service start.
// In case of host reboot if service is stopped then on restart
// all the active tasks are resumed.
func (s *Server) SaveActiveTasks() error {
	errMsg := "error saving active tasks"
	gplog.Info("Saving active tasks")

	s.mutex.Lock()
	defer s.mutex.Unlock()
	if len(s.activeTaskList) == 0 {
		gplog.Info("No active tasks to save")
		return nil
	}

	yamlData, err := yaml.Marshal(s.activeTaskList)
	if err != nil {
		return fmt.Errorf("%s : %s", errMsg, err)
	}

	gplog.Debug("Marshaled data is %s ", string(yamlData))
	err = os.WriteFile(utils.GetActiveServiceTaskConfigPath(), yamlData, 0644)
	if err != nil {
		return fmt.Errorf("%s : %s", errMsg, err)
	}

	return nil
}
