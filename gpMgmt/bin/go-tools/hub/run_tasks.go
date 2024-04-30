package hub

import (
	"context"

	"github.com/greenplum-db/gpdb/gp/idl"
	"github.com/greenplum-db/gpdb/gp/utils"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

func (s *Server) RunTasks(ctx context.Context, in *idl.TaskRunServiceRequest) (*idl.TaskRunServiceReply, error) {
	gplog.Info("Task RunTasks operation requested with arg %v", in.TaskList)

	// Load the task config file to get the task details
	// Load the config file and get the task data
	configTaskOptions, err := utils.LoadTaskConfigFileWithValidation(utils.GetServiceTaskConfigPath())
	if err != nil {
		//return fmt.Errorf("error loading task config file: %w", err)
	}

	// output mof the task status
	var returnStatus []*idl.TaskRunStatus

	// add the task to active task list and schedule the task to run
	for _, taskToRun := range in.TaskList {
		for _, task := range configTaskOptions {
			gplog.Info("Matching received task %s with configured task %s ", taskToRun, task.Name)
			if taskToRun == task.Name {
				cmdExitStatus := TaskExitStatus{}
				err := s.ExecuteTask(task, &cmdExitStatus)
				if err != nil {
					gplog.Warn("failed to run the task %s: %w", task.Name, err)
				}

				//Construct the return status
				returnStatus = append(returnStatus, &idl.TaskRunStatus{
					TaskName:       task.Name,
					PreHookStatus:  boolToString(cmdExitStatus.PreHookStatus),
					GpdrCmdStatus:  boolToString(cmdExitStatus.CmdStatus),
					PostHookStatus: boolToString(cmdExitStatus.PostHookStatus),
				})
			}
		}
	}

	return &idl.TaskRunServiceReply{TaskReturnStatus: returnStatus}, nil
}
