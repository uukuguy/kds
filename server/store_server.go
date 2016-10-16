package server

import (
	//"github.com/AsynkronIT/gam/actor"
	"fmt"
	"mime/multipart"
	"net/http"
	"os"
	"strconv"
	"sync"

	"github.com/labstack/echo"
	"github.com/labstack/echo/engine/standard"
	"github.com/uukuguy/kds/haystack"
	"github.com/uukuguy/kds/store"
	"github.com/uukuguy/kds/store/errors"
	log "github.com/uukuguy/kds/utils/logger"
)

const (
	_UploadFileMaxSize = 1024 * 1024 * 16
)

var (
	storeCluster = new(store.Cluster)
)

// StackServer -
// **************** StackServer ****************
type StackServer struct {
	Name   string
	ip     string
	port   int
	mux    *echo.Echo
	store  *haystack.Store
	closed bool

	// stopC signals the run goroutine should shutdown
	stopC chan struct{}
	// stoppingC is closed by run goroutine on shutdown
	stoppingC chan struct{}
	// doneC is closed when all goroutines complete.
	doneC chan struct{}

	stoppingLock sync.RWMutex
	wg           sync.WaitGroup
}

// goAttach - creates a goroutine on a given function and tracks it using the waitgroup.
func (_this *StackServer) goAttach(f func()) {
	_this.stoppingLock.RLock()
	defer _this.stoppingLock.RUnlock()
	select {
	case <-_this.stoppingC:
		log.Warnf("Server has stopped (skipping goAttach)")
		return
	default:
	}
	_this.wg.Add(1)
	go func() {
		defer _this.wg.Done()
		f()
	}()
}

// NewStackServer -
// ======== NewStackServer() ========
func NewStackServer(ip string, port int, storeDir string) (ss *StackServer, err error) {
	ss = &StackServer{
		Name:      "Default",
		ip:        ip,
		port:      port,
		mux:       echo.New(),
		store:     haystack.NewStore(storeDir),
		closed:    false,
		stopC:     make(chan struct{}),
		stoppingC: make(chan struct{}),
		doneC:     make(chan struct{}),
	}

	if err = ss.store.Init(); err != nil {
		return
	}

	ss.mux.Get("/:bucket/*object", ss.downloadHandler)
	ss.mux.Put("/:bucket/*object", ss.uploadHandler)
	ss.mux.Delete("/:bucket/*object", ss.deleteHandler)

	return
}

// Close -
// ======== Close() ========
func (_this *StackServer) Close() {
	_this.closed = true

	if _this.store != nil {
		_this.store.Close()
		_this.store = nil
	}
}

// ListenAndServe -
// ======== StackServer.ListenAndServe() ========
func (_this *StackServer) ListenAndServe() {
	addr := _this.ip + ":" + strconv.Itoa(_this.port)
	_this.mux.Run(standard.New(addr))
}

// ======== DownloadHandler() ========
func (_this *StackServer) downloadHandler(ctx echo.Context) (err error) {
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

	log.Debugf("DownloadHandler() vid=%d key=%d cookie=%d", vid, key, cookie)

	var volume *haystack.Volume
	var ok bool
	if volume, ok = _this.store.GetVolume(int32(vid)); !ok {
		err = fmt.Errorf("Volume not exist. vid=%d", vid)
		log.Errorf("StackServer.store.GetVolume() failed. %v", err)
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
			log.Errorf("fr.Stat() failed. %v", err)
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

// ======== uploadHandler() ========
func (_this *StackServer) uploadHandler(ctx echo.Context) (err error) {
	//props := actor.FromInstance(&UploadActor{})
	//pid := actor.Spawn(props)
	//pid.Tell(UploadMessage{Context: ctx, Stack_server: _this,})
	//return
	//}

	//// -------- handle_Upload() --------
	//func handle_Upload(_this *StackServer, ctx echo.Context) (err error){
	bucket := ctx.Param("bucket")
	object := ctx.Param("object")
	log.Debugf("uploadHandler() bucket:%s object:%s", bucket, object)

	var (
		vid    int64
		key    int64
		cookie int64
	)
	if vid, err = strconv.ParseInt(ctx.FormValue("vid"), 10, 32); err != nil {
		log.Errorf("ParseInt from vid failed. %v", err)
		return
	}
	if key, err = strconv.ParseInt(ctx.FormValue("key"), 10, 64); err != nil {
		log.Errorf("ParseInt from Key failed. %v", err)
		return
	}
	if cookie, err = strconv.ParseInt(ctx.FormValue("cookie"), 10, 64); err != nil {
		log.Errorf("ParseInt from cookie failed. %v", err)
		return
	}
	var fh *multipart.FileHeader
	if fh, err = ctx.FormFile("file"); err != nil {
		return
	}

	log.Debugf("Form multipart/form-data. %s", fmt.Sprintf(`
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
	if volume, ok = _this.store.GetVolume(int32(vid)); !ok {
		if volume, err = _this.store.CreateVolume(int32(vid)); err != nil {
			return
		}
	}

	file, err := fh.Open()
	defer file.Close()
	if err != nil {
		return
	}

	var fileLen int64
	if fileLen, err = checkFileSize(file, _UploadFileMaxSize); err != nil {
		return
	}
	log.Debugf("checkFileSize(). fileLen=%d", fileLen)

	// Create new needle and fill the data and metadata.
	needle := haystack.NewNeedle(key, int32(cookie), uint32(fileLen))

	if err = needle.ReadFrom(file); err != nil {
		return
	}
	//needle.Data = make([]byte, fileLen)
	//if _, err := file.Read(needle.Data); err != nil {
	//}

	// Save to local store.
	volume.WriteNeedle(needle)

	log.Infof("Upload a needle to a volume. \n%s\n%s\n", needle.String(), volume.String())

	//ctx.Data(iris.StatusOK, []byte("Handle_Upload() return OK."))

	return ctx.HTML(http.StatusOK, "Handle_Upload() return OK.\n")
}

// ======== deleteHandler() ========
func (_this *StackServer) deleteHandler(ctx echo.Context) (err error) {
	return
}
