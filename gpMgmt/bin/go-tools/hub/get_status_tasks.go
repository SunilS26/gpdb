package hub

import (
	"context"
	"net"
	"os"

	"github.com/greenplum-db/gpdb/gp/idl"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

// RPC method defined in service.proto to get the service status and invoked when `gpdr service status` is called
func (s *Server) GetTaskStatus(ctx context.Context, in *idl.TaskStatusServiceRequest) (*idl.TaskStatusServiceReply, error) {
	gplog.Debug("Status gRPC method called")

	// Extract port number
	addr := s.listener.Addr().String()
	_, port, _ := net.SplitHostPort(addr)

	// fetch all the active task status
	taskStatus := make(map[string]*idl.CmdStatus)
	for _, task := range s.activeTaskList {
		taskStatus[task.taskConfigData.Name] = &idl.CmdStatus{
			PreHookStatus:     boolToString(task.TaskStatus.PreHookStatus),
			GpdrCmdStatus:     boolToString(task.TaskStatus.CmdStatus),
			PostHookStatus:    boolToString(task.TaskStatus.PostHookStatus),
			LastTriggeredTime: task.LastTriggeredTime,
			NextScheduledTime: task.NextScheduledTime,
		}
		if task.LastTriggeredTime == "" {
			taskStatus[task.taskConfigData.Name].PreHookStatus = "NA"
			taskStatus[task.taskConfigData.Name].GpdrCmdStatus = "NA"
			taskStatus[task.taskConfigData.Name].PostHookStatus = "NA"
		}
	}

	statuses := idl.TaskStatusServiceReply{
		Pid:           int32(os.Getpid()),
		Port:          port,
		TaskCmdStatus: taskStatus,
	}

	gplog.Debug("gRPC server running with Pid:%d is using Port:%s Status:%v ", os.Getpid(), port, taskStatus)
	return &statuses, nil
}

func boolToString(value bool) string {
	if value {
		return "SUCCESS"
	}
	return "FAILED"
}
