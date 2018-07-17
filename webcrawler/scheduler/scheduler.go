package scheduler

import (
	anlz "Webcrawler-Framework/webcrawler/analyzer"
	ipl  "Webcrawler-Framework/webcrawler/itempipeline"
	"net/http"
	"Webcrawler-Framework/webcrawler/middleware"
	"Webcrawler-Framework/webcrawler/downloader"
)


type Scheduler interface {
	Start(channelLen uint, poolSize uint32, crawlDepth uint32, httpClientGenerator GenHttpClient, respParsers []anlz.ParseResponse,
	itemProcessors []ipl.ProcessItem, firstHttpReq *http.Request) (err error)

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


type myScheduler struct {
	poolSize uint32
	channelLen uint
	crawlDepth uint32
	primaryDomain string	//主域名


	chanman middleware.ChannelManager
	stopSign middleware.StopSign
	dlpool downloader.PageDownloaderPool
	analyzerPool anlz.AnalyzerPool
	itemPipeline ipl.ItemPipeline

	running uint32    //运行标记。0表示未运行，1表示已运行，2表示已停止

	reqCache requestCache   //请求缓存
}



































































