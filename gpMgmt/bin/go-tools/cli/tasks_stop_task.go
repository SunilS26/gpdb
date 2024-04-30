package cli

import (
	"context"
	"fmt"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpdb/gp/idl"
	"github.com/greenplum-db/gpdb/gp/utils"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
)

const (
	StartMsgStopTasks   = "Stopping gpdr service tasks"
	SuccessMsgStopTasks = "Successfully sent gpdr service task stop action"
	ErrMsgStopTask      = "error occurred while stopping gpdr service tasks"
)

func serviceStopTaskCmd() *cobra.Command {
	serviceStopTaskCmd := &cobra.Command{
		Use:     "stop",
		Short:   "Stop the running gpdr service tasks",
		PreRunE: InitializeCommand,
		RunE:    DoStopTasks,
	}

	// Required flags
	serviceStopTaskCmd.Flags().BoolVar(&allTasksFlag, "all", false,
		`Specifies that all tasks configured needs to be started.`)

	// Required flags
	serviceStopTaskCmd.Flags().StringSliceVar(&tasksNameListFlag, "task-name", []string{},
		`Specify the task that needs to be started.`)
	// serviceStopTaskCmd.MarkFlagsOneRequired("task-name", "all")
	// serviceStopTaskCmd.MarkFlagsMutuallyExclusive("task-name", "all")

	return serviceStopTaskCmd
}

func DoStopTasks(cmd *cobra.Command, args []string) error {
	gplog.Info(StartMsgStopTasks)

	configTaskOptions, err := utils.LoadTaskConfigFileWithValidation(utils.GetServiceTaskConfigPath())
	if err != nil {
		return err
	}

	//fetch the task list to start the service
	tasksList, err := FetchTasksNameListToStart(configTaskOptions, cmd.Flags())

	err = StopTaskService(tasksList)
	if err != nil {
		return err
	}

	return nil
}

func StopTaskService(tasksList []string) error {
	gplog.Info("Stopping task for %s", tasksList)

	client, err := ConnectToHub(Conf)
	if err != nil {
		return fmt.Errorf("could not connect to hub; is the hub running? Error: %v", err)
	}

	var result *idl.TaskStopServiceReply
	gplog.Info("Starting service task %v", tasksList)
	result, err = client.StopTasks(context.Background(), &idl.TaskStopServiceRequest{TaskList: tasksList})
	if err != nil {
		errCode := grpcStatus.Code(err)
		errMsg := grpcStatus.Convert(err).Message()
		//TODO: Need to handle more known errors
		if errCode != codes.Unavailable {
			return fmt.Errorf("stopTasks gRPC method failed with error :%w %s", err, errMsg)
		}
	}
	gplog.Info("result: %v", result)

	return nil
}
