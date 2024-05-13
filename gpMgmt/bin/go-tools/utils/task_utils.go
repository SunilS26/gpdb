package utils

import (
	"fmt"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/robfig/cron/v3"
	"regexp"
)

// Required task configuration parameters used by CLI and Hub
type TaskConfigOptions struct {
	Name              string `yaml:"name"`
	PreHook           string `yaml:"pre_hook"`
	Cmd               string `yaml:"cmd"`
	PostHook          string `yaml:"post_hook"`
	MaxRetriesOnError uint16 `yaml:"max_retries_on_error"`
	Schedule          string `yaml:"schedule"`
}

// Validate the service task configuration parameters
func (c *TaskConfigOptions) ValidateTaskConfigOptions() error {
	gplog.Debug("Validating service task configuration parameters")

	// check task name is not empty
	// Currently the task name is limited to 80 characters. Can be increased to more also.
	// TODO: Check if there should be any limitation on task name lenght
	if c.Name == "" || len(c.Name) > 80 {
		return fmt.Errorf("task name cannot be empty and should be within 80 characters")
	}

	// Check if the string contains only AlphaNumeric characters, underscore and space.
	// task name can contain only alpha-numeric characters, underscore and space
	pattern := "^[a-zA-Z0-9_ ]+$"
	re := regexp.MustCompile(pattern)
	if !re.MatchString(c.Name) {
		return fmt.Errorf("task name should contain only alphanumeric characters with spaces and underscores")
	}

	//check the main task command is provided or not
	if c.Cmd == "" {
		return fmt.Errorf("gpdr command cannot be empty")
	}

	// check max retry count
	// Max retry count is kept 20, which might be enuf for any command to retry.
	if c.MaxRetriesOnError < 0 && c.MaxRetriesOnError > 20 {
		return fmt.Errorf("max retry count should be betweeen 0 and 20")
	}

	// check schedule is not empty and validate schedule is correct or not
	if c.Schedule == "" {
		return fmt.Errorf("schedule cannot be empty")
	}

	// Parse and validate the cron expression
	_, err := cron.ParseStandard(c.Schedule)
	if err != nil {
		return fmt.Errorf("schedule should be a valid cron expression: %w", err)
	}

	return nil
}

func LoadTaskConfigFileWithValidation(gpHome string, serviceConfigFile string) ([]TaskConfigOptions, error) {
	gplog.Debug("Loading service task configuration file from %s/%s", gpHome, serviceConfigFile)

	serviceConfigFilePath := fmt.Sprintf("%s/%s", gpHome, serviceConfigFile)
	var taskList []TaskConfigOptions
	err := ReadYamlFile(&taskList, serviceConfigFilePath)
	if err != nil {
		return taskList, err
	}

	// Validate the config parameters for all the tasks
	isTasksValidationFailed := false
	for i, task := range taskList {
		gplog.Debug("TASK[%d]: %+v\n", i+1, task)

		// Validate the config parameters
		err = task.ValidateTaskConfigOptions()
		if err != nil {
			isTasksValidationFailed = true
			gplog.Warn("service task %s configuration validation error: %v", task.Name, err)
		}
	}

	if isTasksValidationFailed {
		return taskList, fmt.Errorf("service task configuration validation failed for one or more tasks")
	}

	// Config is valid
	gplog.Debug("Successfully validated the service task configuration parameters")

	return taskList, nil
}
