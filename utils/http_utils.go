package utils

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

var (
	client *http.Client
	// Transport -
	Transport *http.Transport
)

// -------- init() --------
func init() {
	Transport = &http.Transport{
		MaxIdleConnsPerHost: 1024,
	}
	client = &http.Client{
		Transport: Transport,
	}
}

// -------- waitHTTPResponse() --------
func waitHTTPResponse(url string, rsp *http.Response, err error) ([]byte, error) {
	if err != nil {
		return nil, fmt.Errorf("Wait http response %s: %v", url, err)
	}
	if rsp.StatusCode >= 400 {
		return nil, fmt.Errorf("%s: %s", url, rsp.Status)
	}

	rspBody, err := ioutil.ReadAll(rsp.Body)
	if err != nil {
		return nil, fmt.Errorf("Read response body: %v", err)
	}

	return rspBody, nil
}

// HTTPPostBytes ()
// ======== HttpPostBytes() ========
func HTTPPostBytes(url string, body []byte) ([]byte, error) {
	rsp, err := client.Post(url, "application/octet-stream", bytes.NewReader(body))
	defer rsp.Body.Close()

	return waitHTTPResponse(url, rsp, err)
}

// HTTPPost ()
// ======== HTTPPost() ========
func HTTPPost(url string, values url.Values) ([]byte, error) {
	rsp, err := client.PostForm(url, values)
	defer rsp.Body.Close()

	return waitHTTPResponse(url, rsp, err)
}

// HTTPGet ()
// ======== HTTPGet() ========
func HTTPGet(url string) ([]byte, error) {
	rsp, err := client.Get(url)
	defer rsp.Body.Close()

	return waitHTTPResponse(url, rsp, err)
}

// HTTPDelete ()
// ======== HTTPDelete() ========
func HTTPDelete(url string) error {
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return err
	}

	rsp, err := client.Do(req)
	defer rsp.Body.Close()
	if err != nil {
		return err
	}
	body, err := ioutil.ReadAll(rsp.Body)
	if err != nil {
		return err
	}
	switch rsp.StatusCode {
	case http.StatusNotFound, http.StatusAccepted, http.StatusOK:
		return nil
	}

	m := make(map[string]interface{})
	if err := json.Unmarshal(body, m); err == nil {
		if s, ok := m["error"].(string); ok {
			return errors.New(s)
		}
	}

	return errors.New(string(body))
}

// HTTPDoRequest ()
// ======== HTTPDoRequest() ========
func HTTPDoRequest(req *http.Request) (*http.Response, error) {
	return client.Do(req)
}

// HTTPDownloadFile ()
// ======== HTTPDownloadFile() ========
func HTTPDownloadFile(url string) (filename string, rc io.ReadCloser, err error) {
	rsp, err := client.Get(url)
	if err != nil {
		return "", nil, err
	}
	contentDisposition := rsp.Header["Content-Disposition"]
	if len(contentDisposition) > 0 {
		if strings.HasPrefix(contentDisposition[0], "filename=") {
			filename = contentDisposition[0][len("filename="):]
			filename = strings.Trim(filename, "\"")
		}
	}
	rc = rsp.Body
	return
}

// HTTPPostByFunc ()
// ======== HTTPPostByFunc ========
func HTTPPostByFunc(url string, values url.Values, allocatedBuf []byte, eachBufferFn func([]byte, int)) error {
	rsp, err := client.PostForm(url, values)
	defer rsp.Body.Close()
	if err != nil {
		return err
	}
	if rsp.StatusCode != 200 {
		return fmt.Errorf("%s: %s", url, rsp.Status)
	}
	for {
		n, err := rsp.Body.Read(allocatedBuf)
		if n > 0 {
			eachBufferFn(allocatedBuf, n)
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
	return nil
}

// HTTPPostByReader ()
// ======== HTTPPostByReader() ========
func HTTPPostByReader(url string, values url.Values, readFn func(io.Reader) error) error {
	rsp, err := client.PostForm(url, values)
	defer rsp.Body.Close()
	if err != nil {
		return err
	}
	if rsp.StatusCode != 200 {
		return fmt.Errorf("%s: %s", url, rsp.Status)
	}
	return readFn(rsp.Body)
}
