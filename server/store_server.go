package server

import (
	//"github.com/AsynkronIT/gam/actor"
	"fmt"
	"github.com/labstack/echo"
	"github.com/labstack/echo/engine/standard"
	"github.com/uukuguy/kds/haystack"
	"github.com/uukuguy/kds/store/errors"
	"github.com/uukuguy/kds/utils"
	"mime/multipart"
	"net/http"
	"os"
	"strconv"
)

const (
	UPLOADFILE_MAXSIZE = 1024 * 1024 * 16
)

// **************** StackServer ****************
type StackServer struct {
	Name   string
	ip     string
	port   int
	mux    *echo.Echo
	store  *haystack.Store
	closed bool
}

// ======== NewStackServer() ========
func NewStackServer(ip string, port int, store_dir string) (ss *StackServer, err error) {
	ss = &StackServer{
		Name:   "Default",
		ip:     ip,
		port:   port,
		mux:    echo.New(),
		store:  haystack.NewStore(store_dir),
		closed: false,
	}

	if err = ss.store.Init(); err != nil {
		return
	}

	ss.mux.Get("/:bucket/*object", ss.DownloadHandler)
	ss.mux.Put("/:bucket/*object", ss.UploadHandler)
	ss.mux.Delete("/:bucket/*object", ss.DeleteHandler)

	return
}

// ======== Close() ========
func (this *StackServer) Close() {
	this.closed = true

	if this.store != nil {
		this.store.Close()
		this.store = nil
	}
}

// ======== StackServer.ListenAndServe() ========
func (ss *StackServer) ListenAndServe() {
	addr := ss.ip + ":" + strconv.Itoa(ss.port)
	ss.mux.Run(standard.New(addr))
}

// ======== DownloadHandler() ========
func (this *StackServer) DownloadHandler(ctx echo.Context) (err error) {
	var (
		vid    int64
		key    int64
		cookie int64
	)
	if vid, err = strconv.ParseInt(ctx.QueryParam("vid"), 10, 32); err != nil {
		return
	}
	if key, err = strconv.ParseInt(ctx.QueryParam("key"), 10, 64); err != nil {
		return
	}
	if cookie, err = strconv.ParseInt(ctx.QueryParam("cookie"), 10, 64); err != nil {
		return
	}

	utils.LogDebugf("DownloadHandler() vid=%d key=%d cookie=%d", vid, key, cookie)

	var volume *haystack.Volume
	var ok bool
	if volume, ok = this.store.GetVolume(int32(vid)); !ok {
		err = fmt.Errorf("Volume not exist. vid=%d", vid)
		utils.LogErrorf(err, "DownloadHandler()")
		return
	}

	var needle *haystack.Needle
	if needle, err = volume.ReadNeedle(key); err != nil {
		if err == errors.ErrNeedleNotExist {
			return ctx.HTML(http.StatusNotFound, "Needle not exist.\n")
		}
		return
	}

	return ctx.HTML(http.StatusOK, fmt.Sprintf("%s\n", string(needle.Data)))
}

type sizer interface {
	Size() int64
}

// checkFileSize get multipart.File size
func checkFileSize(file multipart.File, maxSize int) (size int64, err error) {

	//file_len, _ := file.Seek(0, os.SEEK_END)
	//file.Seek(0, os.SEEK_SET)
	//return file_len, nil
	var (
		ok bool
		sr sizer
		fr *os.File
		fi os.FileInfo
	)
	if sr, ok = file.(sizer); ok {
		size = sr.Size()
	} else if fr, ok = file.(*os.File); ok {
		if fi, err = fr.Stat(); err != nil {
			utils.LogErrorf(err, "checkFileSize()")
			return
		}
		size = fi.Size()
	}
	if maxSize > 0 && size > int64(maxSize) {
		err = fmt.Errorf("Upload file size > maxSize(%d)", maxSize)
		return
	}
	return
}

//type UploadMessage struct {
	//Context echo.Context
	//Stack_server *StackServer
//}

//type UploadActor struct {}
//func (this *UploadActor) Receive(actor_context actor.Context){
	//switch msg := actor_context.Message().(type){
	//case *actor.Started:
		//utils.LogDebugf("Started, initialize actor here.")
	//case *actor.Stopping:
		//utils.LogDebugf("Stopping, actor is about shut down.")
	//case *actor.Stopped:
		//utils.LogDebugf("Stopped, actor and it's children are stopped.")
	//case *actor.Restarting:
		//utils.LogDebugf("Restarting, actor is about restart.")
	//case UploadMessage:
		//handle_Upload(msg.Stack_server, msg.Context)	
	//}
//}

// ======== UploadHandler() ========
func (this *StackServer) UploadHandler(ctx echo.Context) (err error) {
	//props := actor.FromInstance(&UploadActor{})
	//pid := actor.Spawn(props)
	//pid.Tell(UploadMessage{Context: ctx, Stack_server: this,})
	//return 
//}

//// -------- handle_Upload() --------
//func handle_Upload(this *StackServer, ctx echo.Context) (err error){
	bucket := ctx.Param("bucket")
	object := ctx.Param("object")
	utils.LogDebugf("UploadHandler() bucket:%s object:%s", bucket, object)

	var (
		vid    int64
		key    int64
		cookie int64
	)
	if vid, err = strconv.ParseInt(ctx.FormValue("vid"), 10, 32); err != nil {
		utils.LogErrorf(err, "ParseInt from vid")
		return
	}
	if key, err = strconv.ParseInt(ctx.FormValue("key"), 10, 64); err != nil {
		utils.LogErrorf(err, "ParseInt from Key")
		return
	}
	if cookie, err = strconv.ParseInt(ctx.FormValue("cookie"), 10, 64); err != nil {
		utils.LogErrorf(err, "ParseInt from cookie")
		return
	}
	var fh *multipart.FileHeader
	if fh, err = ctx.FormFile("file"); err != nil {
		return
	}

	utils.LogDebugf("Form multipart/form-data. %s", fmt.Sprintf(`
-----------------------------
vid:                  %d
key:                  %d
cookie:               %d
Filename:             %s
Content-Type:         %s
-----------------------------
	`,
		vid, key, cookie,
		fh.Filename,
		fh.Header.Get("Content-Type"),
	))

	// Get or create volume in store.
	var volume *haystack.Volume
	var ok bool
	if volume, ok = this.store.GetVolume(int32(vid)); !ok {
		if volume, err = this.store.CreateVolume(int32(vid)); err != nil {
			return
		}
	}

	file, err := fh.Open()
	defer file.Close()
	if err != nil {
		return
	}

	var file_len int64
	if file_len, err = checkFileSize(file, UPLOADFILE_MAXSIZE); err != nil {
		return
	}
	utils.LogDebugf("checkFileSize(). file_len=%d", file_len)

	// Create new needle and fill the data and metadata.
	needle := haystack.NewNeedle(key, int32(cookie), uint32(file_len))

	if err = needle.ReadFrom(file); err != nil {
		return
	}
	//needle.Data = make([]byte, file_len)
	//if _, err := file.Read(needle.Data); err != nil {
	//}

	// Save to local store.
	volume.WriteNeedle(needle)

	utils.LogInfof("Upload a needle to a volume. \n%s\n%s\n", needle.String(), volume.String())

	//ctx.Data(iris.StatusOK, []byte("Handle_Upload() return OK."))

	return ctx.HTML(http.StatusOK, "Handle_Upload() return OK.\n")
}

// ======== DeleteHandler() ========
func (this *StackServer) DeleteHandler(ctx echo.Context) (err error) {
	return
}
