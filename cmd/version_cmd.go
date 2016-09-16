/**
# *　　　　 ┏┓　　 　┏┓+ +
# *　　　　┏┛┻━━━━━━━┛┻━━┓　 + +
# *　　　　┃　　　　　　 ┃
# *　　　　┃━　　━　　 　┃ ++ + + +
# *　　　 ████━████      ┃+
# *　　　　┃　　　　　　 ┃ +
# *　　　　┃　┻　　　    ┃
# *　　　　┃　　　　　　 ┃ + +
# *　　　　┗━━━┓　　 　┏━┛
# *　　　　　　┃　　 　┃
# *　　　　　　┃　　 　┃ + + + +
# *　　　　　　┃　　 　┃　　　Code is far away from bug
# *　　　　　　┃　　 　┃　　　with the animal protecting
# *　　　　　　┃　　 　┃
# *　　　　　　┃　　 　┃ + 　　　神兽保佑,代码无bug
# *　　　　　　┃　　 　┃
# *　　　　　　┃　　 　┃　　+
# *　　　　　　┃　 　　┗━━━━━━━┓ + +
# *　　　　　　┃ 　　　　　　　┣┓
# *　　　　　　┃ 　　　　　　　┏┛
# *　　　　　　┗━━┓┓┏━━━━━┳┓┏━━┛ + + + +
# *　　　　　　　 ┃┫┫　   ┃┫┫
# *　　　　　　　 ┗┻┛　   ┗┻┛+ + + +
# */

package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
)

// -------- versionCmd *cobra.Command --------
var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version number of kds",
	Long:  "All software has versions.",
	Run:   execute_versionCmd,
}

// -------- init() --------
func init() {
	RootCmd.AddCommand(versionCmd)
}

// -------- execute_versionCmd() --------
func execute_versionCmd(cmd *cobra.Command, args []string) {
	fmt.Println("Kleine Dateien Stack - 0.01")
}
