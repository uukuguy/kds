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

package server

import (
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
)

// **************** ObjectHandlers ****************
type ObjectHandlers struct {
}

// ======== Handle_DeleteObject ========
func (oh ObjectHandlers) Handle_GetObject(w http.ResponseWriter, r *http.Request) {
	var object, bucket string
	vars := mux.Vars(r)
	bucket = vars["bucket"]
	object = vars["object"]
	fmt.Printf("bucket: %x object: %x", bucket, object)
}

// ======== Handle_PutObject ========
func (oh ObjectHandlers) Handle_PutObject(w http.ResponseWriter, r *http.Request) {

	//size := r.ContentLength
	//if size == -1 && !contains(r.TransferEncoding, "chunked") {
	//writeErrorResponse(w, r, ErrMissingContentLength, r.URL.Path)
	//return
	//}
	///// maximum Upload size for objects in a single operation
	//if isMaxObjectSize(size) {
	//writeErrorResponse(w, r, ErrEntityTooLarge, r.URL.Path)
	//return
	//}
}

// ======== Handle_DeleteObject ========
func (oh ObjectHandlers) Handle_DeleteObject(w http.ResponseWriter, r *http.Request) {

}
