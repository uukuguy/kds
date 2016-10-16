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
	"net/http"
	"runtime"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/spf13/cobra"
	"github.com/uukuguy/kds/server"
	log "github.com/uukuguy/kds/utils/logger"
)

// -------- serverCmd *cobra.Command --------
var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "KDS server.",
	Long:  "Server node in kds clust.",
	Run:   executeServerCmd,
}

var serverName = server.SERVER_DEFAULT_NAME
var serverIP = server.SERVER_DEFAULT_IP
var serverPort = server.SERVER_DEFAULT_PORT
var storeDir = server.SERVER_DEFAULT_STOREDIR

// -------- init() --------
func init() {
	RootCmd.AddCommand(serverCmd)

	//Persistent Flags which will work for this command and all subcommands.
	serverCmd.PersistentFlags().StringVar(
		&serverIP, "ip", server.SERVER_DEFAULT_IP, "Server IP.")
	serverCmd.PersistentFlags().IntVar(
		&serverPort, "port", server.SERVER_DEFAULT_PORT, "Server port.")
	serverCmd.PersistentFlags().StringVar(
		&storeDir, "dir", server.SERVER_DEFAULT_STOREDIR, "Store Dir.")
	serverCmd.PersistentFlags().StringVar(
		&serverName, "name", server.SERVER_DEFAULT_NAME, "Server Name.")

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

// -------- executeServerCmd() --------
func executeServerCmd(cmd *cobra.Command, args []string) {
	runtime.GOMAXPROCS(runtime.NumCPU())

	log.Infof("Kleine Dateien Stack - Server...")

	var ss *server.StackServer
	var err error
	if ss, err = server.NewStackServer(serverIP, serverPort, storeDir); err != nil {
	}
	defer ss.Close()

	ss.ListenAndServe()
}

// -------- executeServerCmdMux() --------
func executeServerCmdMux(cmd *cobra.Command, args []string) {
	runtime.GOMAXPROCS(runtime.NumCPU())

	fmt.Println("Kleine Dateien Stack - Server...")

	router := mux.NewRouter()

	rootRouter := router.NewRoute().PathPrefix("/").Subrouter()
	bucketRouter := rootRouter.PathPrefix("/{bucket}").Subrouter()

	objectHandlers := server.ObjectHandlers{}
	bucketRouter.Methods("GET").Path("/{object:.+}").HandlerFunc(objectHandlers.Handle_GetObject)
	bucketRouter.Methods("PUT").Path("/{object:.+}").HandlerFunc(objectHandlers.Handle_PutObject)
	bucketRouter.Methods("DELETE").Path("/{object:.+}").HandlerFunc(objectHandlers.Handle_DeleteObject)

	var handlerFuncs = []server.HandlerFunc{
		setRateLimitHandler,
	}
	handler := server.RegisterHandlers(router, handlerFuncs...)

	addr := serverIP + ":" + strconv.Itoa(serverPort)
	server := server.NewMuxServer("Main", addr, handler)

	var err error
	//tls := isSSL()
	tls := false
	if tls {
		//err = server.ListenAndServeTLS(mustGetCertFile(), mustGetKeyFile())
	} else {
		err = server.ListenAndServe()
	}

	log.FatalIf(err, "Failed to start kds server.", "serverIP:", serverIP, "serverPort", serverPort)

}
