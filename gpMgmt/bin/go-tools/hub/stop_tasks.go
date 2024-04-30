package hub

import (
	"context"

	"os"

	"github.com/greenplum-db/gpdb/gp/utils"

	"github.com/greenplum-db/gpdb/gp/idl"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

func (s *Server) StopTasks(ctx context.Context, in *idl.TaskStopServiceRequest) (*idl.TaskStopServiceReply, error) {
	gplog.Info("Task StopTasks operation requested for %s", in.TaskList)

	// Stop all the task given in the task list
	taskExitStatus := make([]string, len(in.TaskList))
	for i, taskName := range in.TaskList {
		taskExitStatus[i] = "SUCCESS"

		// get the active task stop channel
		taskData, isTaskFound := s.activeTaskList[taskName]
		if !isTaskFound {
			gplog.Warn("Task %s is not running", taskName)
			continue
		}

		// Stop the task.
		taskData.stopChan <- true
	}

	// Walk through the active task list to see task is stopped or not
	// TODO: Any better way to have/configure this status.stop takes time  if task is running and
	// can't hold the call till it stop's. for Force stop checked and not in-progress task this works.
	for i, inputTaskName := range in.TaskList {
		for taskName, _ := range s.activeTaskList {
			if taskName == inputTaskName {
				taskExitStatus[i] = "FAILED"
			}
		}
	}

	// delete the active task file if it is last task.
	// Since this active task file is used when service got terminated unexpectedly.
	if len(s.activeTaskList) == 0 {
		if _, err := os.Stat(utils.GetActiveServiceTaskConfigPath()); err == nil {
			err = os.Remove(utils.GetActiveServiceTaskConfigPath())
			if err != nil {
				gplog.Warn("failed to delete the active task file %s: %w", utils.GetActiveServiceTaskConfigPath(), err)
			}
		}
	}

	gplog.Info("Successfully stopped tasks %s", in.TaskList)
	return &idl.TaskStopServiceReply{Status: taskExitStatus}, nil
}
