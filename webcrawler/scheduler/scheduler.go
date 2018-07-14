package scheduler

import (
	"webcrawler/analyzer"
	"webcrawler/itempipeline"
	"net/http"
)

type Scheduler interface {
	Start(channelLen uint, poolSize uint32, crawlDepth uint32, httpClientGenerator GenHttpClient, respParsers []analyzer.ParseResponse,
	itemProcessors []itempipeline.ProcessItem, firstHttpReq *http.Request) (err error)

	Stop() bool

	Running() bool

	ErrorChan() <-chan error

	Idle() bool

	Summary(prefix string) SchedSummary
}


//被用来生成客户端的函数类型,需要提供一个默认实现
type GenHttpClient func() *http.Client

type SchedSummary interface {
	String() string
	Detail() string
	Same(other SchedSummary) bool
}







