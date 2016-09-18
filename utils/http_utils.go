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
	client    *http.Client
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

// -------- wait_http_response() --------
func wait_http_response(url string, rsp *http.Response, err error) ([]byte, error) {
	if err != nil {
		return nil, fmt.Errorf("Wait http response %s: %v", url, err)
	}
	if rsp.StatusCode >= 400 {
		return nil, fmt.Errorf("%s: %s", url, rsp.Status)
	}

	rsp_body, err := ioutil.ReadAll(rsp.Body)
	if err != nil {
		return nil, fmt.Errorf("Read response body: %v", err)
	}

	return rsp_body, nil
}

// ======== HttpPostBytes() ========
func HttpPostBytes(url string, body []byte) ([]byte, error) {
	rsp, err := client.Post(url, "application/octet-stream", bytes.NewReader(body))
	defer rsp.Body.Close()

	return wait_http_response(url, rsp, err)
}

// ======== HttpPost() ========
func HttpPost(url string, values url.Values) ([]byte, error) {
	rsp, err := client.PostForm(url, values)
	defer rsp.Body.Close()

	return wait_http_response(url, rsp, err)
}

// ======== HttpGet() ========
func HttpGet(url string) ([]byte, error) {
	rsp, err := client.Get(url)
	defer rsp.Body.Close()

	return wait_http_response(url, rsp, err)
}

// ======== HttpDelete() ========
func HttpDelete(url string) error {
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

// ======== HttpDoRequest() ========
func HttpDoRequest(req *http.Request) (*http.Response, error) {
	return client.Do(req)
}

// ======== HttpDownloadFile() ========
func HttpDownloadFile(url string) (filename string, rc io.ReadCloser, err error) {
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

// ======== HttpPost_by_Func ========
func HttpPost_by_Func(url string, values url.Values, allocatedBuf []byte, eachBufferFn func([]byte, int)) error {
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

// ======== HttpPost_by_Reader() ========
func HttpPost_by_reader(url string, values url.Values, readFn func(io.Reader) error) error {
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
