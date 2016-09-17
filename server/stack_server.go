package server

import (
    "github.com/kataras/iris"
    "strconv"
    "github.com/uukuguy/kds/utils"
	log "github.com/Sirupsen/logrus"
)

// **************** StackServer ****************
type StackServer struct {
    Name string
    ip string
    port int
    mux *iris.Framework
}

// ======== NewStackServer() ========
func NewStackServer(ip string, port int) *StackServer {
    var ss *StackServer = &StackServer{
        Name: "Default",
        ip: ip,
        port: port,
        mux: iris.New(),
    }

    ss.mux.Get("/", ss.Handle_GetObject) 
    ss.mux.Put("/", ss.Handle_PutObject)
    ss.mux.Delete("/", ss.Handle_DelObject)
    return ss
}

// ======== StackServer.ListenAndServe() ========
func (ss *StackServer) ListenAndServe() {
    addr := ss.ip + ":" + strconv.Itoa(ss.port)
    ss.mux.Listen(addr)
}

// -------- StackServer.Handle_GetObject() --------
func (ss *StackServer) Handle_GetObject(ctx *iris.Context){
    ctx.Data(iris.StatusOK, [] byte("Some binary data here."))
}

// -------- StackServer.Handle_PutObject() --------
func (ss *StackServer) Handle_PutObject(ctx *iris.Context){
    utils.LogDebug("StackServer.Handle_PutObject()", log.Fields{
        "Path": string(ctx.Path()),
        "RemoteIP": ctx.RemoteIP(),
        "ContentLength": ctx.Request.Header.ContentLength(),
        "IsBodyStream": ctx.IsBodyStream(),
        "Body": string(ctx.Request.Body()),
        //"Request": ctx.Request,
    })
    ctx.Data(iris.StatusOK, [] byte("Handle_PutObject() return OK."))

    //ctx.Write("Hi %s PutObject", "Iris")
}


// -------- StackServer.Handle_DelObject() --------
func (ss *StackServer) Handle_DelObject(ctx *iris.Context){
    ctx.Write("Hi %s DelObject", "Iris")
}
