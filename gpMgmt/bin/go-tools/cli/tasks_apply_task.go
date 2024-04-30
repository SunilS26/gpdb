package cli

import (
	"context"
	"fmt"

	"net"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpdb/gp/idl"
	"github.com/greenplum-db/gpdb/gp/utils"
	"github.com/sergi/go-diff/diffmatchpatch"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
	"gopkg.in/yaml.v2"
)

const (
	StartMsgSetTasks   = "Configuring tasks for GPDR service"
	SuccessMsgSetTasks = "Service tasks configured successfully"
	ErrMsgSetTasks     = "error occurred while configuring tasks for gpdr service"
)

// color code to print the diff of old task config file and new config file
var (
	red                    = color.New(color.FgRed).SprintFunc()
	green                  = color.New(color.FgGreen).SprintFunc()
	yellow                 = color.New(color.FgYellow).SprintFunc()
	ConfigTaskFileLocation = ""
)

func serviceApplyTaskCmd() *cobra.Command {
	serviceApplyTaskCmd := &cobra.Command{
		Use:     "apply",
		Short:   "Configure set of tasks for the gpdr service",
		PreRunE: InitializeCommand,
		RunE:    DoApplyTasks,
	}

	// Required flags
	serviceApplyTaskCmd.Flags().StringVar(&ConfigTaskFileLocation, "config-file", "",
		`Specifies the location of the service task configuration file. Template files can be found in "/usr/local/gpdr/templates".`)
	serviceApplyTaskCmd.MarkFlagRequired("config-file")

	return serviceApplyTaskCmd
}

func DoApplyTasks(cmd *cobra.Command, args []string) error {
	gplog.Info(StartMsgSetTasks)

	// Reconfigure the task if it is already configured
	if _, err := os.Stat(utils.GetServiceTaskConfigPath()); err == nil {
		return DoUpdateTaskConfig()
	}

	// If zero tasks then error out for initial configuration. For reconfiguration just remove the old configuration file.
	// which is handles as part of DoUpdateTaskConfig()
	// Load the service configuration file for Validation purpose
	taskList, err := utils.LoadTaskConfigFileWithValidation(ConfigTaskFileLocation)
	if err != nil {
		return err
	}

	// explcitly checking outside here since for initial config and reconfig handling is different for
	// an empty tasklist. for reconfigure with empty list is equivalent to configuration cleanup.
	if len(taskList) == 0 {
		return fmt.Errorf("no tasks found in the service task configuration file")
	}

	// Save the service configuration file
	err = utils.CopyToReadOnlyFile(utils.GetServiceTaskConfigPath(), ConfigTaskFileLocation)
	if err != nil {
		return err
	}
	gplog.Info("Successfully saved service configuration file")

	return nil
}

// Call update to update any active task running.
func UpdateServiceActiveTask() error {
	gplog.Debug("Updating the active task parameters")

	client, err := ConnectToHub(Conf)
	if err != nil {
		return fmt.Errorf("could not connect to hub; is the hub running? Error: %v", err)
	}

	var result *idl.TaskApplyServiceReply
	gplog.Info("Calling SetTasks RPC method to update the active tasks")
	result, err = client.ApplyTasks(context.Background(), &idl.TaskApplyServiceRequest{TaskAction: idl.TaskAction_UPDATE})
	if err != nil {
		errCode := grpcStatus.Code(err)
		errMsg := grpcStatus.Convert(err).Message()
		//TODO: Need to handle more known errors
		if errCode != codes.Unavailable {
			return fmt.Errorf("settasks gRPC method failed with error :%w %s", err, errMsg)
		}
	}
	gplog.Info("result: %v", result)

	return nil
}

func DoUpdateTaskConfig() error {
	gplog.Info("Task is already configured, updating the existing configuration with the new configuration")

	// check if there is any active task running and error out if it is deleted.
	anyActiveTaskRunning, err := DetectAnyActiveTaskRunning()
	if err != nil {
		return err
	}

	// Print the diff of the nwe task configuration file with existing configuration for confirmation with user
	gplog.Info("Identified below task configuration changes with the new configuration")
	err = printTaskConfigDiff()
	if err != nil {
		return err
	}

	// Take acknowledgement from the user to update the task configuration file with the new configuration
	err = utils.ForceConfigPrompt("set-tasks", "Do you want to continue (y/n)? ")
	if err != nil {
		return fmt.Errorf("error when taking acknowledgement from the user: %w", err)
	}

	// Load the new service configuration file for Validation purpose
	taskList, err := utils.LoadTaskConfigFileWithValidation(ConfigTaskFileLocation)
	if err != nil {
		return err
	}

	// If there is zero task in new task configuration then delete the old configuration,
	// since no point in having empty task configuration file.
	if len(taskList) == 0 {
		err := os.Remove(utils.GetServiceTaskConfigPath())
		if err != nil {
			return fmt.Errorf("failed to delete old task configuration file %s : %s", utils.GetServiceTaskConfigPath(), err.Error())
		}
		return nil
	} else {
		// copy new config
		err = utils.CopyToReadOnlyFile(utils.GetServiceTaskConfigPath(), ConfigTaskFileLocation)
		if err != nil {
			return err
		}
		gplog.Info("Successfully saved the new service configuration file")
	}

	// Call RPC method to update any running tasks parameters.
	if anyActiveTaskRunning {
		err = UpdateServiceActiveTask()
		if err != nil {
			return err
		}
	}

	return nil
}

func printTaskConfigDiff() error {
	// Read the old task configuration file
	oldTaskConfig, err := utils.LoadTaskConfigFileWithValidation(utils.GetServiceTaskConfigPath())
	if err != nil {
		return fmt.Errorf("failed to read old task configuration: %w", err)
	}

	// Read new task configuration file and create a map of task name as key and task itself as value
	newTaskConfig, err := utils.LoadTaskConfigFileWithValidation(ConfigTaskFileLocation)
	if err != nil {
		return fmt.Errorf("failed to read new task configuration: %w", err)
	}

	newTaskConfigList := make(map[string]utils.TaskConfigOptions)
	// check each task old with new and if diff exists
	for _, task := range newTaskConfig {
		newTaskConfigList[task.Name] = task
	}

	// Iterate over old task configurations and check if any task is updated/deleted in new task configurations.
	for _, task := range oldTaskConfig {
		// check if old config task exists in new configuration
		_, isTaskFound := newTaskConfigList[task.Name]
		if isTaskFound {
			// If change in task parameters log that task is updated
			if isIdenticalTask(newTaskConfigList[task.Name], task) {
				fmt.Println(yellow(fmt.Sprintf("No Change for Task \"%s\":", task.Name)))
			} else {
				fmt.Println(yellow(fmt.Sprintf("Task \"%s\" has changed:", task.Name)))
			}

			oldTaskYamlData, err := yaml.Marshal(task)
			if err != nil {
				return fmt.Errorf("failed to marshal task %s with error %w ", task, err)
			}

			newTaskYamlData, err := yaml.Marshal(newTaskConfigList[task.Name])
			if err != nil {
				return fmt.Errorf("failed to marshal task %s with error %w ", task, err)
			}

			printDiff(string(oldTaskYamlData), string(newTaskYamlData))

			// delete the task from new task config list, so that at the end we are left with new tasks only
			delete(newTaskConfigList, task.Name)
		} else {
			// If old config task is not found in new task config , then it can be deleted since before reaching
			// this point it is already verified that none of the active task is deleted.
			fmt.Println(yellow(fmt.Sprintf("Task \"%s\" is deleted:", task.Name)))

			taskYamlData, err := yaml.Marshal(task)
			if err != nil {
				return fmt.Errorf("failed to marshal task %s with error %w ", task, err)
			}

			printDiff(string(taskYamlData), "")
		}
	}

	// Any left over task in new task configuration list are new tasks
	for _, task := range newTaskConfigList {
		fmt.Println(yellow(fmt.Sprintf("New Task \"%s\" added:", task.Name)))

		taskYamlData, err := yaml.Marshal(task)
		if err != nil {
			return fmt.Errorf("failed to marshal task %s with error %w ", task, err)
		}

		printDiff("", string(taskYamlData))
	}

	return nil
}

func printDiff(oldDataStr, newDataStr string) {
	gplog.Debug("old data: %s, new data: %s", oldDataStr, newDataStr)

	// New task
	if oldDataStr == "" {
		lines := strings.Split(newDataStr, "\n")
		// Print each line with prefix "+"
		for _, line := range lines {
			if line != "" {
				fmt.Println(green(fmt.Sprintf("+ %s", line)))
			}
		}
		fmt.Println()
		return
	}

	// Deleted task
	if newDataStr == "" {
		lines := strings.Split(oldDataStr, "\n")
		// Print each line with prefix '-', since it is deleted.
		for _, line := range lines {
			if line != "" {
				fmt.Println(red(fmt.Sprintf("- %s", line)))
			}
		}
		fmt.Println()
		return
	}

	// For Update, Generate the diff by creating a new object
	diffObject := diffmatchpatch.New()
	oldStrLines, newStrLines, strlineArray := diffObject.DiffLinesToChars(oldDataStr, newDataStr)
	diffs := diffObject.DiffMain(oldStrLines, newStrLines, false)
	// Convert the differences back to text
	diffText := diffObject.DiffCharsToLines(diffs, strlineArray)

	// Print the diff text with original text
	for _, d := range diffText {
		switch d.Type {
		case diffmatchpatch.DiffEqual:
			fmt.Printf("%s", d.Text)
		case diffmatchpatch.DiffDelete:
			fmt.Printf(red(fmt.Sprintf("- %s", d.Text)))
		case diffmatchpatch.DiffInsert:
			fmt.Printf(green(fmt.Sprintf("+ %s", d.Text)))
		}
	}

	fmt.Println()
	return
}

func GetActiveTaskList() (*idl.TaskStatusServiceReply, error) {
	// If any old active task is deleted without stop it, error out here only because task needs to be explicitly stopped before deletion.
	// fetch the status to check for active tasks.
	// service status will give the active tasks. If service is not running then no active tasks exists.
	result, err := FetchServiceStatus()
	if err != nil {
		return result, err
	}

	return result, nil
}

func DetectAnyActiveTaskRunning() (bool, error) {
	// If any old active task is deleted without stop it, error out here only because task needs to be explicitly stopped before deletion.
	// fetch the service status to check for active tasks.
	result, err := GetActiveTaskList()
	if err != nil {
		return false, err
	}

	// if result is nil then service is not running
	if result == nil {
		gplog.Debug("service is not running")
		return false, nil
	}

	// If there are no active tasks then return simply
	if len(result.TaskCmdStatus) == 0 {
		gplog.Info("No active tasks found")
		return false, nil
	}

	err = DetectAnyActiveTaskDeletion(result)
	if err != nil {
		return true, err
	}

	return true, nil
}

func DetectAnyActiveTaskDeletion(result *idl.TaskStatusServiceReply) error {
	gplog.Info("Checking if any active tasks is being deleted")

	// Read the new task configuration file
	newTaskConfig, err := utils.LoadTaskConfigFileWithValidation(ConfigTaskFileLocation)
	if err != nil {
		return fmt.Errorf("error reading the new task config file %s: %w", ConfigTaskFileLocation, err)
	}

	// Iterate over status RPC result to know any running tasks being deleted in new task configuration file.
	var deletedTaskList []string
	for name, _ := range result.TaskCmdStatus {
		taskfound := false
		// check the acive task is present in new config file
		for _, task := range newTaskConfig {
			if name == task.Name {
				taskfound = true
				break
			}
		}
		// If task is not found, add it to delete task list
		if !taskfound {
			deletedTaskList = append(deletedTaskList, name)
		}
	}

	if len(deletedTaskList) != 0 {
		return fmt.Errorf("found active task are being deleted %s , please stop the task before deletion", deletedTaskList)
	}

	gplog.Info("No active running tasks is deleted")
	return nil
}

func isIdenticalTask(newTask, oldTask utils.TaskConfigOptions) bool {
	if newTask.PreHook != oldTask.PreHook ||
		newTask.GpdrCmd != oldTask.GpdrCmd ||
		newTask.PostHook != oldTask.PostHook ||
		newTask.MaxRetriesOnError != oldTask.MaxRetriesOnError ||
		newTask.Schedule != oldTask.Schedule {
		return false
	}

	return true
}

func IsServiceRunningInBackground() (bool, error) {
	// Check if the service is running in the background
	cmdStr := fmt.Sprintf(`ps ux | grep "[g]pdr service start --daemonize" | awk '{print $2}' `)
	cmd := exec.Command("bash", "-c", cmdStr)
	output, err := cmd.CombinedOutput()
	if err != nil {
		// If the command returns exit code 1, the process is not running
		if cmd.ProcessState.ExitCode() == 1 {
			return false, nil
		}
		// Otherwise, an error occurred while executing the ps command
		return false, fmt.Errorf("failed to execute ps command %s: %w", cmdStr, err)
	}

	// If output contains at least one process ID, the service is running
	return len(strings.TrimSpace(string(output))) > 0, nil
}

func IsGRPCServerResponding() (bool, error) {
	addr := fmt.Sprintf("localhost:%d", Conf.Port)
	conn, err := net.DialTimeout("tcp", addr, 60*time.Second)
	if err != nil {
		return false, fmt.Errorf("gpdr service is not responding on address %s : %w", addr, err) // Connection failed, service is not running
	}
	defer conn.Close()

	return true, nil // Connection successful, service is running
}

func isServiceRunning() (bool, error) {
	// Check if the service is running in the background
	isProcessRunning, err := IsServiceRunningInBackground()
	if err != nil {
		return false, fmt.Errorf("error while checking if service is running: %w", err)
	}

	// Check if the grpc server is responding
	isServerResponding, err := IsGRPCServerResponding()
	if err != nil {
		return false, fmt.Errorf("error while checking if service is running: %w", err)
	}

	return isProcessRunning && isServerResponding, nil
}
