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
	"github.com/gorilla/mux"
	"github.com/spf13/cobra"
	"github.com/uukuguy/kds/server"
	"github.com/uukuguy/kds/utils"
	"net/http"
	"runtime"
	"strconv"
)

// -------- serverCmd *cobra.Command --------
var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "KDS server.",
	Long:  "Server node in kds clust.",
	Run:   execute_serverCmd,
}

var server_name = server.SERVER_DEFAULT_NAME
var server_ip = server.SERVER_DEFAULT_IP
var server_port = server.SERVER_DEFAULT_PORT
var store_dir = server.SERVER_DEFAULT_STOREDIR

// -------- init() --------
func init() {
	RootCmd.AddCommand(serverCmd)

	//Persistent Flags which will work for this command and all subcommands.
	serverCmd.PersistentFlags().StringVar(
		&server_ip, "ip", server.SERVER_DEFAULT_IP, "Server IP.")
	serverCmd.PersistentFlags().IntVar(
		&server_port, "port", server.SERVER_DEFAULT_PORT, "Server port.")
	serverCmd.PersistentFlags().StringVar(
		&store_dir, "dir", server.SERVER_DEFAULT_STOREDIR, "Store Dir.")
	serverCmd.PersistentFlags().StringVar(
		&server_name, "name", server.SERVER_DEFAULT_NAME, "Server Name.")

	// Local flags, which will only run when this action is called directly.
	serverCmd.Flags().IntP("vmodule", "v", 0, "glog vmodule. -v=1 for debug.")

}

// setRateLimitHandler limits the number of concurrent http requests based on MINIO_MAXCONN.
func setRateLimitHandler(handler http.Handler) http.Handler {
	return handler
	//if globalMaxConn == 0 {
	//return handler
	//} // else proceed to rate limiting.

	//// For max connection limit of > '0' we initialize rate limit handler.
	//return &rateLimit{
	//handler:   handler,
	//workQueue: make(chan struct{}, globalMaxConn),
	//waitQueue: make(chan struct{}, globalMaxConn*4),
	//}
}

// -------- execute_serverCmd() --------
func execute_serverCmd(cmd *cobra.Command, args []string) {
	runtime.GOMAXPROCS(runtime.NumCPU())

	utils.LogInfof("Kleine Dateien Stack - Server...")

	var ss *server.StackServer
	var err error
	if ss, err = server.NewStackServer(server_ip, server_port, store_dir); err != nil {
	}
	defer ss.Close()

	ss.ListenAndServe()
}

// -------- execute_serverCmd_Mux() --------
func execute_serverCmd_Mux(cmd *cobra.Command, args []string) {
	runtime.GOMAXPROCS(runtime.NumCPU())

	fmt.Println("Kleine Dateien Stack - Server...")

	router := mux.NewRouter()

	root_router := router.NewRoute().PathPrefix("/").Subrouter()
	bucket_router := root_router.PathPrefix("/{bucket}").Subrouter()

	object_handlers := server.ObjectHandlers{}
	bucket_router.Methods("GET").Path("/{object:.+}").HandlerFunc(object_handlers.Handle_GetObject)
	bucket_router.Methods("PUT").Path("/{object:.+}").HandlerFunc(object_handlers.Handle_PutObject)
	bucket_router.Methods("DELETE").Path("/{object:.+}").HandlerFunc(object_handlers.Handle_DeleteObject)

	var handlerFuncs = []server.HandlerFunc{
		setRateLimitHandler,
	}
	handler := server.RegisterHandlers(router, handlerFuncs...)

	addr := server_ip + ":" + strconv.Itoa(server_port)
	server := server.NewMuxServer("Main", addr, handler)

	var err error
	//tls := isSSL()
	tls := false
	if tls {
		//err = server.ListenAndServeTLS(mustGetCertFile(), mustGetKeyFile())
	} else {
		err = server.ListenAndServe()
	}

	utils.FatalIf(err, "Failed to start kds server.", "server_ip:", server_ip, "server_port", server_port)

}
