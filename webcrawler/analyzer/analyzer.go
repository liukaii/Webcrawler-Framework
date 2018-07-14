package analyzer

import (
	"webcrawler/base"
	"net/http"
	"webcrawler/middleware"
	"errors"
	"net/url"
	"github.com/op/go-logging"
	"fmt"
)

var logger logging.Logger = base.NewLogger()


type Analyzer interface {
	Id() uint32
	Analyze(respParsers []ParseResponse, resp base.Response) ([]base.Data, []error)//根据规则分析响应并返回请求和条目
}

//被用于解析HTTP响应的函数类型
type ParseResponse func(httpResp *http.Response, respDepth uint32) ([]base.Data, []error)

//分析器池的接口类型
type AnalyzerPool interface {
	Take() (Analyzer, error)
	Return(analyzer Analyzer) error
	Total() uint32
	Used() uint32
}

var analyzerDownloader middleware.IdGenertor = middleware.NewIdGenertor()

func genAnalyzerId() uint32 {
	return analyzerDownloader.GetUint32()
}

type myAnalyzer struct {
	id uint32
}

func NewAnalyzer() Analyzer {
	return &myAnalyzer{id: genAnalyzerId()}
}

func (analyzer *myAnalyzer) Id() uint32 {
	return analyzer.id
}

func (analyzer *myAnalyzer) Analyze(respParses []ParseResponse, resp base.Response) (dataList []base.Data, errorList []error) {
	if respParses == nil {
		err := errors.New("The response parser list is invalid!")
		return nil, []error{err}
	}
	httpResp := resp.HttpResp()
	if httpResp == nil {
		err := errors.New("The http response is invalid!")
		return nil, []error{err}
	}
	var reqUrl *url.URL = httpResp.Request.URL
	logger.Infof("Parse the response (reqUrl=%s)... \n",reqUrl)
	respDepth := resp.Depth()
	//解析HTTP响应
	dataList = make([]base.Data, 0)
	errorList = make([]error, 0)
	for i, respParser := range respParses {
		if respParser == nil {
			err := errors.New(fmt.Sprintf("The document parser [%d] is invalid!", i))
			errorList = append(errorList, err)
			continue
		}
		pDataList, pErrorList := respParser(httpResp, respDepth)
		if pDataList != nil {
			for _, pData := range pDataList {
				dataList = appendDataList(dataList, pData, respDepth)
			}
		}
		if pErrorList != nil {
			for _, pError := range pErrorList {
				errorList = appendErrorList(errorList, pError)
			}
		}
	}
	return dataList, errorList
}


func appendDataList(dataList []base.Data, data base.Data, respDepth uint32) []base.Data {
	if data == nil {
		return dataList
	}
	req, ok := data.(*base.Request)
	if !ok{
		return append(dataList, data)
	}
	newDepth := respDepth + 1
	if req.Depth() != newDepth {
		req = base.NewRequest(req.HttpReq(), newDepth)
	}
	return append(dataList, req)
}

func appendErrorList(errorList []error, err error) []error {
	if err == nil {
		return errorList
	}
	return append(errorList, err)
}
































