package hub

import (
	"context"
	"fmt"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpdb/gp/constants"
	"github.com/greenplum-db/gpdb/gp/idl"
	"github.com/greenplum-db/gpdb/gp/utils"
)

// Task required information used for manging configured tasks
type TaskDescriptor struct {
	taskConfigOptions         utils.TaskConfigOptions
	TaskCmdStatus             *TaskCommandStatus
	taskCmdPID                TaskCommandPID
	stopTaskChan              chan bool
	updateTaskChan            chan utils.TaskConfigOptions
	currentTaskExecutionState TaskExecutionState
	stopTaskCmdState          TaskExecutionState
}

// PID is cached here so that when a force stop is requested ,
// the on-going process can be killed.
// PID in command context is an integer.
type TaskCommandPID struct {
	PreHookPID  int
	CmdPID      int
	PostHookPID int
}

// Set of Command Status with it's last and next scheduled time.
// These information needs to be stored for showing command execution history
// TODO: Needs to be modified accordingly to store last 10 task execution history
type TaskCommandStatus struct {
	PreHookStatus     TaskExecutionState
	CmdStatus         TaskExecutionState
	PostHookStatus    TaskExecutionState
	LastTriggeredTime string
	NextScheduledTime string
}

// Initially all the scheduled/instan task execution will have "NA".Based upon their execution state
// further state is determined.
type TaskExecutionState int

const (
	NA TaskExecutionState = iota
	FAILED
	RUNNING
	SUCCEEDED
	STOPPED
)

func (ts TaskExecutionState) String() string {
	return [...]string{"NA", "Failed", "Running", "Succeeded"}[ts]
}
func NewTaskConfig() *TaskDescriptor {
	return &TaskDescriptor{
		taskConfigOptions: utils.TaskConfigOptions{},
		TaskCmdStatus: &TaskCommandStatus{
			PreHookStatus:     FAILED,
			CmdStatus:         FAILED,
			PostHookStatus:    FAILED,
			LastTriggeredTime: "",
			NextScheduledTime: "",
		},
		stopTaskChan:   make(chan bool),
		updateTaskChan: make(chan utils.TaskConfigOptions, 1),
	}
}

func (s *Server) ApplyTasks(ctx context.Context, in *idl.ApplyTasksRequest) (*idl.ApplyTasksReply, error) {
	gplog.Info("ApplyTasks operation requested")

	// For update task action, update the active task data
	if in.TaskAction == idl.TaskAction_UPDATE {
		gplog.Info("Updating the active task with latest configuration")
		if err := s.UpdateActiveTaskConfiguration(); err != nil {
			return nil, fmt.Errorf("error updating the active task configuration %s", err)
		}
	}

	gplog.Info("ApplyTasks operation is Successfully")
	return &idl.ApplyTasksReply{}, nil
}

func (s *Server) UpdateActiveTaskConfiguration() error {
	s.taskMutexRWLock.RLock()
	// if no active task return
	if len(s.activeTaskList) == 0 {
		s.taskMutexRWLock.RUnlock()
		gplog.Debug("No active task running, skipping the active task configuration update")
		return nil
	}
	s.taskMutexRWLock.RUnlock()

	// Load the new configuration and Update Active Task in s.activeTaskList
	configTaskOptions, err := utils.LoadTaskConfigFileWithValidation(s.GpHome, constants.TaskConfigFileName)
	if err != nil {
		return fmt.Errorf("error loading config file %s: %s", constants.TaskConfigFileName, err)
	}

	for _, task := range configTaskOptions {
		// Check If task is present in active task list or not
		s.taskMutexRWLock.RLock()
		taskData, isTaskFound := s.activeTaskList[task.Name]
		s.taskMutexRWLock.RUnlock()

		// Check if the task is running or not
		if !isTaskFound {
			gplog.Debug("Task %s is not running", task.Name)
			continue
		}

		// Send the new task configuration update request
		gplog.Info("Updating Config found for the task %s : %s", taskData.taskConfigOptions.Name, task.Name)
		taskData.updateTaskChan <- task
	}

	return nil
}
