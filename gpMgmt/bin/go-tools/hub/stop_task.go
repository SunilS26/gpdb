package hub

import (
	"context"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpdb/gp/idl"
	"syscall"
)

func (s *Server) StopTasks(ctx context.Context, in *idl.StopTasksRequest) (*idl.StopTasksReply, error) {
	gplog.Info("Task StopTasks operation requested for %s", in.TaskList)

	TaskCommandInfo := s.FinishTask(in.TaskList, in.ForceStop)

	gplog.Info("Successfully stopped tasks %s", in.TaskList)
	return &idl.StopTasksReply{Status: TaskCommandInfo}, nil
}

func (s *Server) ForceStopTask(taskData *TaskDescriptor) error {
	gplog.Info("Force Stop Requested")
	if taskData.taskCmdPID.PreHookPID > 0 {
		gplog.Debug("Killing pid %d", taskData.taskCmdPID.PreHookPID)

		// Stop the process using the PID
		err := syscall.Kill(taskData.taskCmdPID.PreHookPID, syscall.SIGKILL)
		if err != nil {
			gplog.Info("Error stopping process : %s", err)
		}
	}

	if taskData.taskCmdPID.CmdPID > 0 {
		gplog.Debug("Killing pid %d", taskData.taskCmdPID.CmdPID)
		// Stop the process using the PID
		err := syscall.Kill(taskData.taskCmdPID.CmdPID, syscall.SIGKILL)
		if err != nil {
			gplog.Info("Error stopping process : %s", err)
		}
	}

	if taskData.taskCmdPID.PostHookPID > 0 {
		gplog.Debug("Killing pid %d", taskData.taskCmdPID.PostHookPID)

		// Stop the process using the PID
		err := syscall.Kill(taskData.taskCmdPID.PostHookPID, syscall.SIGKILL)
		if err != nil {
			gplog.Info("Error stopping process : %s", err)
		}
	}

	s.taskMutexRWLock.RLock()
	gplog.Info("Waiting force for task to stop.....")
	// Stop the task.
	taskData, isTaskFound := s.activeTaskList[taskData.taskConfigOptions.Name]
	if isTaskFound {
		s.taskMutexRWLock.RUnlock()
		if taskData.stopTaskCmdState != RUNNING {
			taskData.stopTaskChan <- true
			// Delete an entry from the map
			s.DeleteTaskFromActiveTaskList(taskData.taskConfigOptions.Name)
		} else {
			gplog.Info("Skipping since already a stop in progress.")
		}
	} else {
		s.taskMutexRWLock.RUnlock()
	}

	gplog.Info("Task Stopped forcefully.....")

	return nil
}

func (s *Server) FinishTask(TaskList []string, forceStop bool) []string {
	// Stop all the task given in the task list
	TaskCommandInfo := make([]string, len(TaskList))
	for i, taskName := range TaskList {
		TaskCommandInfo[i] = "SUCCESS"

		// get the active task stop channel
		taskData, isTaskFound := s.activeTaskList[taskName]
		if !isTaskFound {
			gplog.Warn("Task %s is not running", taskName)
			TaskCommandInfo[i] = "Not Running"
			continue
		}

		if forceStop {
			err := s.ForceStopTask(taskData)
			if err != nil {
				TaskCommandInfo[i] = "FAILED"
			}
		} else {
			if taskData.stopTaskCmdState == RUNNING {
				continue
			}

			taskData.stopTaskCmdState = RUNNING
			gplog.Verbose("Waiting for task to stop")

			// Stop the task.
			taskData.stopTaskChan <- true

			taskData.stopTaskCmdState = STOPPED
			s.DeleteTaskFromActiveTaskList(taskName)
			gplog.Verbose("Task Stopped Successfully")
		}
	}

	return TaskCommandInfo
}
