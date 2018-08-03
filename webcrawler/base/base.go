package base

import (
	"net/http"
	"bytes"
	"fmt"
	"Webcrawler-Framework/logging"
)

func NewLogger() logging.Logger {
	return logging.NewSimpleLogger()
}

type Request struct {
	httpReq *http.Request
	depth uint32
}

//请求
func NewRequest(httpReq *http.Request, depth uint32) *Request {
	return &Request{httpReq , depth}
}

func (req *Request) HttpReq() *http.Request {
	return req.httpReq
}

func (req *Request) Depth() uint32 {
	return req.depth
}

//响应
type Response struct {
	httpResp *http.Response
	depth uint32
}

func NewResponse(httpResp *http.Response, depth uint32) *Response {
	return &Response{httpResp, depth}
}

func (resp *Response) HttpResp() *http.Response {
	return resp.httpResp
}

func(resp *Response) Depth() uint32 {
	return resp.depth
}


//条目
type Item map[string]interface{}



//数据的接口
type Data interface{
	Valid() bool
}

func (req *Request) Valid() bool {
	return req.httpReq != nil && req.httpReq.URL != nil
}

func (resp *Response) Valid() bool {
	return resp.httpResp != nil && resp.httpResp.Body != nil
}

func (item Item) Valid() bool {
	return item != nil
}

//错误类型-哪一个处理模块抛出的
type ErrorType string
//爬虫错误的接口
type CrawlerError interface {
	Type()  ErrorType
	Error() string
}


type myCrawlerError struct {
	errType ErrorType
	errMsg string//错误信息
	fullErrMsg string//完整的错误提示信息
}


const (
	DOWNLOADER_ERROR ErrorType = "Downloader Error"
	ANALYZER_ERROR ErrorType = "Analyzer Error"
	ITEM_PROCESSOR_ERROR ErrorType = "Item Processor Error"
)

//创建一个新的爬虫错误
func NewCrawlerError(errType ErrorType, errMsg string) CrawlerError {
	return &myCrawlerError{errType: errType, errMsg: errMsg}
}

func (ce *myCrawlerError) Type() ErrorType {
	return ce.errType
}

func (ce *myCrawlerError) Error() string {
	if ce.fullErrMsg == "" {
		ce.genFullMsg()
	}
	return ce.fullErrMsg
}

func (ce *myCrawlerError) genFullMsg() {
	var buffer bytes.Buffer
	buffer.WriteString("Crawler Error: ")
	if ce.errType != "" {
		buffer.WriteString(string(ce.errType))
		buffer.WriteString(": ")
	}
	buffer.WriteString(ce.errMsg)
	ce.fullErrMsg = fmt.Sprintf("%s\n", buffer.String())
	return
}






































