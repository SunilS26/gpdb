package cli

import (
	"github.com/spf13/cobra"
)

func taskCmd() *cobra.Command {
	taskCmd := &cobra.Command{
		Use:   "task",
		Short: "Start hub, agents services",
	}

	taskCmd.AddCommand(serviceApplyTaskCmd())
	taskCmd.AddCommand(serviceScheduleTaskCmd())
	taskCmd.AddCommand(serviceStatusTaskCmd())
	taskCmd.AddCommand(serviceStopTaskCmd())

	return taskCmd
}
