package hub

import (
	"context"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpdb/gp/idl"
)

func (s *Server) GetTaskStatus(ctx context.Context, in *idl.GetTaskStatusRequest) (*idl.GetTaskStatusReply, error) {
	gplog.Debug("Status gRPC method called")

	// fetch all the active task status
	taskStatus := make(map[string]*idl.TaskExecutionStatus)
	s.taskMutexRWLock.RLock()
	for _, task := range s.activeTaskList {
		status := &idl.TaskExecutionStatus{
			Status:            task.currentTaskExecutionState.String(),
			PreHookStatus:     task.TaskCmdStatus.PreHookStatus.String(),
			CmdStatus:         task.TaskCmdStatus.CmdStatus.String(),
			PostHookStatus:    task.TaskCmdStatus.PostHookStatus.String(),
			PreHookPid:        uint32(task.taskCmdPID.PreHookPID),
			CmdPid:            uint32(task.taskCmdPID.CmdPID),
			PostHookPid:       uint32(task.taskCmdPID.PostHookPID),
			LastTriggeredTime: task.TaskCmdStatus.LastTriggeredTime,
			NextScheduledTime: task.TaskCmdStatus.NextScheduledTime,
		}
		taskStatus[task.taskConfigOptions.Name] = status
	}
	s.taskMutexRWLock.RUnlock()

	gplog.Debug("Successfully retrieved the task status %v", taskStatus)
	return &idl.GetTaskStatusReply{TaskStatus: taskStatus}, nil
}
