package server

import (
    "os"
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

    ss.mux.Get("/:bucket/:object", ss.Handle_GetObject) 

    ss.mux.Put("/:bucket/:object", ss.Handle_PutObject)

    ss.mux.Delete("/:bucket/:object", ss.Handle_DelObject)
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

// -------- get_upload_file_content() --------
func get_upload_file_content(ctx *iris.Context) ([] byte, error){
    var file_bytes [] byte

    req := ctx.Request

    /*
    type Form struct {
        Value map[string][]string
        File map[string][]*FileHeader
    }
    form multipart.Form
    */
    form, err := req.MultipartForm()
    if err != nil {
        log.Fatalf("unexpected error: %s", err)
    }
	defer req.RemoveMultipartFormFiles()

	if len(form.File) >= 1 {
        // (name, fileHeaders)
        for _, fileHeaders := range form.File {
            /*
            type FileHeader struct {
                Filename string
                Header textproto.MIMEHeader
            }
            func (fh *FileHeader) Open() (File, error)
            */
            fh := fileHeaders[0]
            utils.LogDebug("File attributes", log.Fields{
                "Filename": fh.Filename,
                "Content-Type": fh.Header.Get("Content-Type"),
            })

            file, _ := fh.Open()
            defer file.Close()

            // Get file length.
            file_len, _ := file.Seek(0, os.SEEK_END)
            file.Seek(0, os.SEEK_SET) 
            //utils.LogDebug("File Info", log.Fields{
                //"len": file_len,
            //})

            // Read all content into file_bytes.
            buf := make([]byte, file_len)
            for n, err := file.Read(buf); err == nil; n, err = file.Read(buf) {
                file_bytes = append(file_bytes, buf[:n]...)
            }
            //utils.LogDebug("File Readed.", log.Fields{
                //"file size": len(file_bytes),
                ////"content": string(file_bytes),
            //})

            // TODO Only process one part.
            break
        }
    }

    return file_bytes, err
}

// ======== StackServer.Handle_PutObject() ========
func (ss *StackServer) Handle_PutObject(ctx *iris.Context){

    bucket := ctx.Param("bucket")
    object := ctx.Param("object")
    utils.LogDebug("Handle_PutObject", log.Fields{
        "bucket": bucket,
        "object": object,
    })

    //req := ctx.Request
    //contentLength := req.Header.ContentLength()
    //utils.LogDebug("StackServer.Handle_PutObject()", log.Fields{
        //"Path": string(ctx.Path()),
        //"RemoteIP": ctx.RemoteIP(),
        //"ContentLength": contentLength,
        //"IsBodyStream": ctx.IsBodyStream(),
        //"URI": string(req.RequestURI()),
        ////"Body": string(ctx.Request.Body()),
        ////"Request": ctx.Request,
    //})

    // Uploaded file content
    file_bytes, _ := get_upload_file_content(ctx)
    utils.LogDebug("Upload file content:", log.Fields{
        "file length": len(file_bytes),
        "file content": string(file_bytes),
    })

    ctx.Data(iris.StatusOK, [] byte("Handle_PutObject() return OK."))

}


// -------- StackServer.Handle_DelObject() --------
func (ss *StackServer) Handle_DelObject(ctx *iris.Context){
    ctx.Write("Hi %s DelObject", "Iris")
}
