package hub

import (
	"context"

	"github.com/greenplum-db/gpdb/gp/utils"

	"github.com/greenplum-db/gpdb/gp/idl"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

// TaskDataInfo is the struct to hold the task related parameters
// used by gRPC server for task lifecycle management like start, stop.
type TaskDataInfo struct {
	taskConfigData    utils.TaskConfigOptions `yaml:"-"`
	TaskStatus        TaskExitStatus          `yaml:"taskStatus"`
	stopChan          chan bool               `yaml:"-"`
	updateTask        chan struct{}           `yaml:"-"`
	LastTriggeredTime string                  `yaml:"LastTriggeredTime"`
	NextScheduledTime string                  `yaml:"NextScheduledTime"`
}

type TaskExitStatus struct {
	PreHookStatus  bool `yaml:"preHookStatus"`
	CmdStatus      bool `yaml:"cmdStatus"`
	PostHookStatus bool `yaml:"postHookStatus"`
}

func (s *Server) ApplyTasks(ctx context.Context, in *idl.TaskApplyServiceRequest) (*idl.TaskApplyServiceReply, error) {
	gplog.Info("Task SetTasks operation requested ")

	// For update task action update the active task data
	if in.TaskAction == idl.TaskAction_UPDATE {
		gplog.Info("Updating active task data")
		if err := s.UpdateActiveTaskData(); err != nil {
			return nil, err
		}
	}

	gplog.Info("SetTasks operation is Successfully")
	return &idl.TaskApplyServiceReply{}, nil
}

func (s *Server) UpdateActiveTaskData() error {
	gplog.Info("Updating active task")

	// if no active task return
	if len(s.activeTaskList) == 0 {
		return nil
	}

	// Process the updated configuration
	// Update Active Task in s.activeTaskList
	configTaskOptions, err := utils.LoadTaskConfigFileWithValidation(utils.GetServiceTaskConfigPath())
	if err != nil {
		gplog.Info("Error loading config file: %s", err)
		return err
	}

	gplog.Debug("s.activeTaskList %v \n\nconfigOption %v", s.activeTaskList, configTaskOptions)
	for _, activeTask := range s.activeTaskList {
		for _, task := range configTaskOptions {
			gplog.Info("Searching task %s with %s", activeTask.taskConfigData.Name, task.Name)
			if activeTask.taskConfigData.Name == task.Name {
				gplog.Info("Updating Config found for the task %s", activeTask.taskConfigData.Name, task.Name)
				activeTask.taskConfigData.PreHook = task.PreHook
				activeTask.taskConfigData.GpdrCmd = task.GpdrCmd
				activeTask.taskConfigData.PostHook = task.PostHook
				activeTask.taskConfigData.MaxRetriesOnError = task.MaxRetriesOnError
				activeTask.taskConfigData.Schedule = task.Schedule
				activeTask.updateTask <- struct{}{}
			}
		}
	}

	return nil
}
