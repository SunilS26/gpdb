package cli

import (
	"context"
	"fmt"
	"os"

	"strconv"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpdb/gp/idl"
	"github.com/greenplum-db/gpdb/gp/utils"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
)

const (
	STOPPED = "Stopped"
	RUNNING = "Running"
)

type TaskStats struct {
	name          string
	schedule      string
	status        string
	LastTriggered string
	NextSchedule  string
	prehook       string
	gpdrCmd       string
	posthook      string
}

func serviceStatusTaskCmd() *cobra.Command {
	serviceStatusTaskCmd := &cobra.Command{
		Use:     "status",
		Short:   "Configure set of tasks for the gpdr service",
		PreRunE: InitializeCommand,
		RunE:    DoStatusTasks,
	}

	serviceStatusTaskCmd.PersistentFlags().MarkHidden("config-file")
	return serviceStatusTaskCmd
}

func DoStatusTasks(cmd *cobra.Command, args []string) error {
	// Service is fetched from the gRPC server itself since we are displaying only PID for service status.
	// Later for displaying task related status we can use the same flow.
	statusOutput, err := GetFormattedServiceStatus()
	if err != nil {
		return err
	}

	// Print the status information to the user.
	fmt.Print(statusOutput)

	return nil
}

func GetTaskListWithStatus(serviceStatus string, result *idl.TaskStatusServiceReply) ([]TaskStats, error) {
	// Load the service configuration file for Validation purpose
	configTaskOptions, err := utils.LoadTaskConfigFileWithValidation(utils.GetServiceTaskConfigPath())
	if err != nil {
		return nil, err
	}

	var out []TaskStats
	//fetch the task list
	for _, task := range configTaskOptions {
		task1 := TaskStats{
			name:          task.Name,
			schedule:      task.Schedule,
			status:        "Stopped",
			LastTriggered: "NA",
			NextSchedule:  "NA",
			prehook:       "NA",
			gpdrCmd:       "NA",
			posthook:      "NA",
		}

		if serviceStatus == "Running" {
			var status string
			if _, ok := result.TaskCmdStatus[task.Name]; ok {
				status = "Running"
				stat := result.TaskCmdStatus[task.Name]
				task1.status = status
				task1.LastTriggered = stat.LastTriggeredTime
				task1.NextSchedule = stat.NextScheduledTime
				if task.PreHook != "" {
					task1.prehook = stat.PostHookStatus
				}
				task1.gpdrCmd = stat.GpdrCmdStatus
				if task.PreHook != "" {
					task1.posthook = stat.PreHookStatus
				}

			}
		}
		// Append the task to the slice
		out = append(out, task1)
	}

	return out, nil
}

func formatTaskStats(stat []TaskStats) string {
	resultOutput := strings.Builder{}
	resultOutput.WriteString("Current Task Status\n")
	resultOutput.WriteString("----------------------\n")
	resultOutput.WriteString(fmt.Sprintf("%-46s | %-15s | %-10s | %-25s | %-25s | %-20s | %-20s | %-20s\n", "Task Name", "Schedule", "Status", "Last Triggered Time", "Next Scheduled Time", "Pre-Hook Status", "Cmd-Status", "Post-Hook Status"))
	//resultOutput.WriteString(fmt.Sprintf("Task Name    | Schedule     |  Status        |Last Triggered Time    |Next Triggerred Time    | Pre-Hook Status|  Cmd-Status | Post-Hook Status\n"))
	resultOutput.WriteString("--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n")

	for _, task := range stat {
		resultOutput.WriteString(fmt.Sprintf("%-46s | %-15s | %-10s | %-25s | %-25s | %-20s | %-20s | %-20s\n", task.name, task.schedule, task.status, task.LastTriggered, task.NextSchedule, task.prehook, task.gpdrCmd, task.posthook))
	}
	return resultOutput.String()
}

func formatServiceStatus(port string, pid string, serviceStatus string, result *idl.TaskStatusServiceReply) (string, error) {
	resultOutput := strings.Builder{}
	resultOutput.WriteString("Service Status Information\n")
	resultOutput.WriteString("-----------------------------\n")
	resultOutput.WriteString(fmt.Sprintf("Service Port   : %-15s\n", port))
	resultOutput.WriteString(fmt.Sprintf("Service Status : %-15s\n", serviceStatus))
	resultOutput.WriteString(fmt.Sprintf("Service PID    : %-15s\n\n", pid))

	// If task config file exists then only display the task details
	if _, err := os.Stat(utils.GetServiceTaskConfigPath()); err == nil {
		taskStatus, err := GetTaskListWithStatus(serviceStatus, result)
		if err != nil {
			return "", fmt.Errorf("failed to get task list from service status response: %w", err)
		}
		taskStatusStr := formatTaskStats(taskStatus)
		// Add the task status
		resultOutput.WriteString(taskStatusStr)
	}

	return resultOutput.String(), nil
}

func FetchServiceStatus() (*idl.TaskStatusServiceReply, error) {
	var result *idl.TaskStatusServiceReply

	client, err := ConnectToHub(Conf)
	if err != nil {
		return result, fmt.Errorf("could not connect to hub; is the hub running? Error: %v", err)
	}

	result, err = client.GetTaskStatus(context.Background(), &idl.TaskStatusServiceRequest{})
	if err != nil {
		errCode := grpcStatus.Code(err)
		errMsg := grpcStatus.Convert(err).Message()
		gplog.Debug("%s:%s", errMsg, err)
		//TODO: Need to handle more known errors
		if errCode != codes.Unavailable {
			return result, fmt.Errorf("status gRPC method failed with error :%w %s", err, errMsg)
		}
	}

	gplog.Debug("Successfully fetched the status from gpdr service and result is %v", result)

	return result, nil
}

func GetFormattedServiceStatus() (string, error) {
	// fetch the gRPC service status
	result, err := FetchServiceStatus()
	if err != nil {
		return "", err
	}

	var outputStatusString string
	if result != nil {
		outputStatusString, err = formatServiceStatus(result.Port, strconv.Itoa(int(result.Pid)), RUNNING, result)
	} else {
		outputStatusString, err = formatServiceStatus("", "", STOPPED, result)
	}
	if err != nil {
		return "", err
	}

	gplog.Debug("Service status fetched successfully: %s", outputStatusString)
	return outputStatusString, nil
}
