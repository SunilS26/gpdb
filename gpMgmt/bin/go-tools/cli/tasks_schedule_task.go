package cli

import (
	"context"
	"fmt"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpdb/gp/idl"
	"github.com/greenplum-db/gpdb/gp/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
)

const (
	StartMsgStartTasks   = "Starting gpdr service tasks"
	ErrMsgStartTask      = "error occurred while starting gpdr service tasks"
	SuccessMsgStartTasks = "Service tasks started successfully"
)

var allTasksFlag bool
var runOnceFlag bool
var tasksNameListFlag []string

func serviceScheduleTaskCmd() *cobra.Command {
	serviceScheduleTaskCmd := &cobra.Command{
		Use:     "schedule",
		Short:   "Schedule the gpdr service tasks to run as per configured parameters",
		PreRunE: InitializeCommand,
		RunE:    DoStartTasks,
	}

	// Required flags
	serviceScheduleTaskCmd.Flags().BoolVar(&allTasksFlag, "all", false,
		`Specifies that all tasks configured needs to be started.`)

	// Required flags
	serviceScheduleTaskCmd.Flags().BoolVar(&runOnceFlag, "run-once", false,
		`Specifies that all tasks configured needs to be started.`)

	// Required flags
	serviceScheduleTaskCmd.Flags().StringSliceVar(&tasksNameListFlag, "task-name", []string{},
		`Specify the task that needs to be started.`)
	// serviceScheduleTaskCmd.MarkFlagsOneRequired("task-name", "all")
	// serviceScheduleTaskCmd.MarkFlagsMutuallyExclusive("task-name", "all")

	return serviceScheduleTaskCmd
}

func DoStartTasks(cmd *cobra.Command, args []string) error {
	gplog.Info(StartMsgStartTasks)

	// Load the task configuration file
	configTaskOptions, err := utils.LoadTaskConfigFileWithValidation(utils.GetServiceTaskConfigPath())
	if err != nil {
		return err
	}

	//parse the task list and identify the command type and do handling accordingly
	tasksList, err := FetchTasksNameListToStart(configTaskOptions, cmd.Flags())
	if err != nil {
		return err
	}

	// Call the gpdr service to start the service.
	err = StartTaskService(tasksList)
	if err != nil {
		return err
	}

	return nil
}

func FetchTasksNameListToStart(configTaskOptions []utils.TaskConfigOptions, cmdFlags *pflag.FlagSet) ([]string, error) {
	//Check if the task-name flag is used,return the explicitly specified task list
	isTaskNameFlagSet := cmdFlags.Lookup("task-name").Changed
	if isTaskNameFlagSet {
		return tasksNameListFlag, nil
	}

	//fetch the task list, if --all flag is passed fetch the list from task configuration file
	var allTasksList []string
	if allTasksFlag == true {
		for i, _ := range configTaskOptions {
			//walk through configTaskOptions to get the task list
			allTasksList = append(allTasksList, configTaskOptions[i].Name)
		}
	}

	return allTasksList, nil
}

func StartTaskService(tasksList []string) error {
	gplog.Info("Starting task service for %s", tasksList)

	client, err := ConnectToHub(Conf)
	if err != nil {
		return fmt.Errorf("could not connect to hub; is the hub running? Error: %v", err)
	}

	var result *idl.TaskScheduleServiceReply

	gplog.Info("Calling service task RPC to start task for %v", tasksList)
	result, err = client.ScheduleTasks(context.Background(), &idl.TaskScheduleServiceRequest{TaskList: tasksList})
	if err != nil {
		errCode := grpcStatus.Code(err)
		errMsg := grpcStatus.Convert(err).Message()
		//TODO: Need to handle more known errors
		if errCode != codes.Unavailable {
			return fmt.Errorf("startTasks gRPC method failed with error :%w %s", err, errMsg)
		}
	}
	gplog.Debug("result: %v", result)

	return nil
}
