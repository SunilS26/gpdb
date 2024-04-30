package cli

import (
	"context"
	"fmt"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpdb/gp/idl"
	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
)

const (
	StartMsgRunTasks   = "Executing the gpdr service tasks"
	SuccessMsgRunTasks = "Successfully executed the gpdr service tasks"
	ErrMsgRunTask      = "error occurred while executing gpdr service tasks"
)

func RunTaskService(tasksList []string) error {
	gplog.Info("Executing service task %s", tasksList)

	client, err := ConnectToHub(Conf)
	if err != nil {
		return fmt.Errorf("could not connect to hub; is the hub running? Error: %v", err)
	}

	var result *idl.TaskRunServiceReply

	gplog.Info("Starting service task %v", tasksList)
	result, err = client.RunTasks(context.Background(), &idl.TaskRunServiceRequest{TaskList: tasksList})
	if err != nil {
		errCode := grpcStatus.Code(err)
		errMsg := grpcStatus.Convert(err).Message()
		//TODO: Need to handle more known errors
		if errCode != codes.Unavailable {
			return fmt.Errorf("runTasks gRPC method failed with error :%w %s", err, errMsg)
		}
	}

	gplog.Info("result: %v", result)

	outputStr := formatRunTaskStats(result.TaskReturnStatus)
	if err != nil {
		return err
	}

	// Print the status information to the user.
	fmt.Print(outputStr)
	fmt.Println()

	return nil
}

func formatRunTaskStats(taskCmdStatus []*idl.TaskRunStatus) string {
	resultOutput := strings.Builder{}
	resultOutput.WriteString("----------------------------------------------------------------------------------------------------------------------------------------------------------------------\n")
	resultOutput.WriteString(fmt.Sprintf("%-46s | %-20s | %-20s | %-20s\n", "Task Name", "Pre-Hook Status", "Cmd-Status", "Post-Hook Status"))
	resultOutput.WriteString("----------------------------------------------------------------------------------------------------------------------------------------------------------------------\n")

	for _, task := range taskCmdStatus {
		resultOutput.WriteString(fmt.Sprintf("%-46s | %-20s | %-20s | %-20s\n", task.TaskName, task.PreHookStatus, task.GpdrCmdStatus, task.PostHookStatus))
	}
	return resultOutput.String()
}
