package hub

import (
	"fmt"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpdb/gp/constants"
	"github.com/greenplum-db/gpdb/gp/idl"
	"github.com/greenplum-db/gpdb/gp/utils"
)

type streamArgs struct {
	hubStream HubStream
	status    []string
	stream    idl.Hub_RunTaskServer
	response  []*idl.TaskExecutionStatus
	taskArg   *TaskDescriptor
	taskIndex int
}

func (s *Server) RunTask(in *idl.RunTaskRequest, stream idl.Hub_RunTaskServer) error {
	gplog.Info("Task RunTasks operation requested with arg %v", in.TaskList)

	err := s.RunTasks(in, stream)
	if err != nil {
		return err
	}

	return nil
}

func createTaskExecutionResponseBody(taskList []string) ([]*idl.TaskExecutionStatus, error) {
	// output of the task status
	var returnStatus []*idl.TaskExecutionStatus

	for _ = range taskList {
		// Construct a task configuration parameters
		taskArg := &TaskDescriptor{
			TaskCmdStatus: &TaskCommandStatus{
				PreHookStatus:     NA,
				CmdStatus:         NA,
				PostHookStatus:    NA,
				LastTriggeredTime: "",
				NextScheduledTime: "",
			}}

		//Construct the return status
		returnStatus = append(returnStatus, &idl.TaskExecutionStatus{
			PreHookStatus:  taskArg.TaskCmdStatus.PreHookStatus.String(),
			CmdStatus:      taskArg.TaskCmdStatus.CmdStatus.String(),
			PostHookStatus: taskArg.TaskCmdStatus.PostHookStatus.String(),
		})
	}

	return returnStatus, nil
}

func sendStreamData(streamArgs *streamArgs, taskArg *TaskDescriptor) {
	// fill the response with pid for taskArg
	streamArgs.response[streamArgs.taskIndex].PreHookStatus = taskArg.TaskCmdStatus.PreHookStatus.String()
	streamArgs.response[streamArgs.taskIndex].CmdStatus = taskArg.TaskCmdStatus.CmdStatus.String()
	streamArgs.response[streamArgs.taskIndex].PostHookStatus = taskArg.TaskCmdStatus.PostHookStatus.String()

	streamArgs.response[streamArgs.taskIndex].PreHookPid = uint32(taskArg.taskCmdPID.PreHookPID)
	streamArgs.response[streamArgs.taskIndex].CmdPid = uint32(taskArg.taskCmdPID.CmdPID)
	streamArgs.response[streamArgs.taskIndex].PostHookPid = uint32(taskArg.taskCmdPID.PostHookPID)

	// send the stream
	streamArgs.hubStream.StreamRunTaskResponse(streamArgs.status, streamArgs.response)
}

// Instantly Execute the given task
// Two cases where the execution is not possible,
//  1. When already same task in-progress by previous instant execution
//  2. When already same task in-progress by scheduled execution.
func (s *Server) RunTasks(in *idl.RunTaskRequest, stream idl.Hub_RunTaskServer) error {
	// Schedule all the task given in the task list
	runTasksStatus := make([]string, len(in.TaskList))

	// Load the task config file to get the task details
	configTaskOptions, err := utils.LoadTaskConfigFileWithValidation(s.GpHome, constants.TaskConfigFileName)
	if err != nil {
		return fmt.Errorf("error loading task config file: %w", err)
	}

	// output mof the task status
	response, err := createTaskExecutionResponseBody(in.TaskList)

	// Create a hub stream handler
	hubStream := NewHubStream(stream)

	// add the task to active task list and schedule the task to run
	for index, taskToRun := range in.TaskList {
		// Check if any existing task is present, if yes then return
		// This checks any previous instance execution is still running
		if _, ok := s.instantTaskRunList[taskToRun]; ok {
			gplog.Info("task %s already running", taskToRun)
			runTasksStatus[index] = "ALREADY_RUNNING"
			continue
		}

		// Check if any existing task is present, if yes then return for same scheduled tasl
		if _, ok := s.activeTaskList[taskToRun]; ok {
			var taskArgs *TaskDescriptor
			s.taskMutexRWLock.RLock()
			if _, ok := s.activeTaskList[taskToRun]; ok {
				s.taskMutexRWLock.RUnlock()
				taskArgs = s.activeTaskList[taskToRun]
			} else {
				s.taskMutexRWLock.RUnlock()
			}

			if taskArgs != nil && taskArgs.currentTaskExecutionState == RUNNING {
				gplog.Info("task %s already running", taskToRun)
				runTasksStatus[index] = "ALREADY_RUNNING"
				continue
			}
		}

		for _, task := range configTaskOptions {
			gplog.Debug("Comparing received task %s with configured task %s ", taskToRun, task.Name)
			if taskToRun == task.Name {

				// Construct a task configuration parameters
				taskArg := &TaskDescriptor{
					taskConfigOptions: task,
					TaskCmdStatus: &TaskCommandStatus{
						PreHookStatus:     NA,
						CmdStatus:         NA,
						PostHookStatus:    NA,
						LastTriggeredTime: "",
						NextScheduledTime: "",
					}}

				runTasksStatus[index] = "RUNNING"

				s.taskMutexRWLock.Lock()
				s.instantTaskRunList[task.Name] = taskArg
				s.taskMutexRWLock.Unlock()

				// Create a stream data
				streamArgInfo := streamArgs{
					hubStream: hubStream,
					stream:    stream,
					status:    runTasksStatus,
					response:  response,
					taskArg:   taskArg,
					taskIndex: index,
				}

				err := s.ExecuteTask(taskArg, streamArgInfo)
				if err != nil {
					gplog.Error("failed to run the task %s: %s", task.Name, err)
					runTasksStatus[index] = "FAILED"
				} else {
					runTasksStatus[index] = "SUCCEEDED"
				}

				s.taskMutexRWLock.Lock()
				delete(s.instantTaskRunList, task.Name)
				s.taskMutexRWLock.Unlock()
			}
		}
	}

	// Send the final status which can covers task failure scenarios
	hubStream.StreamRunTaskResponse(runTasksStatus, response)

	return nil
}
