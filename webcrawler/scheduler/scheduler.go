package scheduler

import (
	anlz "Webcrawler-Framework/webcrawler/analyzer"
	ipl  "Webcrawler-Framework/webcrawler/itempipeline"
	"net/http"
	"Webcrawler-Framework/webcrawler/middleware"
	dwl "Webcrawler-Framework/webcrawler/downloader"
	"fmt"
	"errors"
	"Webcrawler-Framework/webcrawler/base"
	"Webcrawler-Framework/logging"
	"sync/atomic"
	"time"
	"strings"
	"sync"
	"bytes"
	"regexp"
)


//组件的统一代号
const (
	DOWNLOADER_CODE = "downloader"
	ANALYZER_CODE = "analyzer"
	ITEMPIPELINE_CODE = "item_pipeline"
	SCHEDULER_CODE = "scheduler"
)


var logger logging.Logger = base.NewLogger()


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
	dlpool dwl.PageDownloaderPool
	analyzerPool anlz.AnalyzerPool
	itemPipeline ipl.ItemPipeline

	running uint32    //运行标记。0表示未运行，1表示已运行，2表示已停止

	reqCache requestCache   //请求缓存

	urlMap  map[string]bool  //已请求的URL字典
}


func NewScheduler() Scheduler {
	return &myScheduler{}
}


func (sched *myScheduler) Start(channelLen uint, poolSize uint32, crawlDepth uint32, httpClientGenerator GenHttpClient, respParsers []anlz.ParseResponse,
	itemProcessors []ipl.ProcessItem, firstHttpReq *http.Request) (err error) {

	defer func() {
		if p := recover(); p != nil {
			errMsg := fmt.Sprintf("Fatal Scheduler Error: %s\n", p)
			logger.Fatal(errMsg)
			err = errors.New(errMsg)
		}
	}()

	if atomic.LoadUint32(&sched.running) == 1 {
		return errors.New("The scheduler has been started!\n")
	}
	atomic.StoreUint32(&sched.running, 1)

	if channelLen == 0 {
		return errors.New("The channel max length(capacity) can not be 0!\n")
	}
	sched.channelLen = channelLen

	if poolSize == 0 {
		return errors.New("The pool size can not be 0!\n")
	}
	sched.poolSize = poolSize
	sched.crawlDepth = crawlDepth

	sched.chanman = generateChannelManager(sched.channelLen)

	if httpClientGenerator == nil {
		return errors.New("The HTTP client generator list is invalid!")
	}

	dlpool, err := generatePageDownloaderPool(sched.poolSize, httpClientGenerator)
	if err != nil {
		errMsg := fmt.Sprintf("Occur error when get page downloader pool: %s\n", err)
		return errors.New(errMsg)
	}
	sched.dlpool = dlpool

	analyzerPool, err := generateAnalyzerPool(sched.poolSize)
	if err != nil {
		errMsg := fmt.Sprintf("Occur error when get analyzer pool: %s\n", err)
		return errors.New(errMsg)
	}
	sched.analyzerPool = analyzerPool

	if itemProcessors == nil {
		return errors.New("The item processor list is invalid!")
	}
	for i, ip := range itemProcessors {
		if ip == nil {
			return errors.New(fmt.Sprintf("The %dth item processor is invalid!"))
		}
	}
	sched.itemPipeline = generateItemPipeline(itemProcessors)

	if sched.stopSign == nil {
		sched.stopSign = middleware.NewStopSign()
	} else {
		sched.stopSign.Reset()
	}

	sched.urlMap = make(map[string]bool)

	sched.startDownloading()
	sched.activateAnalyzers(respParsers)
	sched.openItemPipeline()
	sched.schedule(10 * time.Millisecond)

	if firstHttpReq == nil{
		return errors.New("The first HTTP request is invalid!")
	}
	pd, err := getPrimaryDomain(firstHttpReq.Host)
	if err != nil {
		return err
	}
	sched.primaryDomain = pd

	firstReq := base.NewRequest(firstHttpReq, 0)
	sched.reqCache.put(firstReq)

	return nil
}


func generateChannelManager(channelLen uint) middleware.ChannelManager {
	return middleware.NewChannelManager(channelLen)
}

func generateAnalyzerPool(poolSize uint32) (anlz.AnalyzerPool, error) {
	analyzerPool, err := anlz.NewAnalyzerPool(
		poolSize,
		func() anlz.Analyzer {
			return anlz.NewAnalyzer()
		},
	)
	if err != nil {
		return nil, err
	}
	return analyzerPool, nil
}

func generatePageDownloaderPool(poolSize uint32, client GenHttpClient) (dwl.PageDownloaderPool, error){
	dlPool, err := dwl.NewPageDownloaderPool(
		poolSize,
		func() dwl.PageDownloader {
			return dwl.NewPageDownloader(client())
		},
	)
	if err != nil {
		return nil, err
	}
	return dlPool, nil
}

func generateItemPipeline(itemprocessor []ipl.ProcessItem) ipl.ItemPipeline {
	return ipl.NewPipeline(itemprocessor)
}



var regexpForIp = regexp.MustCompile(`((?:(?:25[0-5]|2[0-4]\d|[01]?\d?\d)\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d?\d))`)

var regexpForDomains = []*regexp.Regexp{
	// *.xx or *.xxx.xx
	regexp.MustCompile(`\.(com|com\.\w{2})$`),
	regexp.MustCompile(`\.(gov|gov\.\w{2})$`),
	regexp.MustCompile(`\.(net|net\.\w{2})$`),
	regexp.MustCompile(`\.(org|org\.\w{2})$`),
	// *.xx
	regexp.MustCompile(`\.me$`),
	regexp.MustCompile(`\.biz$`),
	regexp.MustCompile(`\.info$`),
	regexp.MustCompile(`\.name$`),
	regexp.MustCompile(`\.mobi$`),
	regexp.MustCompile(`\.so$`),
	regexp.MustCompile(`\.asia$`),
	regexp.MustCompile(`\.tel$`),
	regexp.MustCompile(`\.tv$`),
	regexp.MustCompile(`\.cc$`),
	regexp.MustCompile(`\.co$`),
	regexp.MustCompile(`\.\w{2}$`),
}


func getPrimaryDomain(host string) (string, error) {
	host = strings.TrimSpace(host)
	if host == "" {
		return "", errors.New("The host is empty!")
	}
	if regexpForIp.MatchString(host) {
		return host, nil
	}
	var suffixIndex int
	for _, re := range regexpForDomains {
		pos := re.FindStringIndex(host)
		if pos != nil {
			suffixIndex = pos[0]
			break
		}
	}
	if suffixIndex > 0 {
		var pdIndex int
		firstPart := host[:suffixIndex]
		index := strings.LastIndex(firstPart, ".")
		if index < 0 {
			pdIndex = 0
		} else {
			pdIndex = index + 1
		}
		return host[pdIndex:], nil
	} else {
		return "", errors.New("Unrecognized host!")
	}
}

func (sched *myScheduler) startDownloading() {
	go func() {
		for {
			req, ok := <- sched.getReqChan()
			if !ok {
				break
			}
			go sched.download(req)
		}
	}()
}


func (sched *myScheduler) download(req base.Request) {
	defer func() {
		if p := recover(); p !=nil {
			errMsg := fmt.Sprintf("Fatal Download Error: %s\n", p)
			logger.Fatal(errMsg)
		}
	}()

	downloader, err := sched.dlpool.Take()
	if err != nil {
		errMsg := fmt.Sprint("Downloader pool error: %s", err)
		sched.sendError(errors.New(errMsg), SCHEDULER_CODE)
		return
	}
	defer func() {
		err := sched.dlpool.Return(downloader)
		if err != nil {
			errMsg := fmt.Sprint("Downloader pool error:%s", err)
			sched.sendError(errors.New(errMsg), SCHEDULER_CODE)
		}
	}()

	code := generateCode(DOWNLOADER_CODE, downloader.Id())
	respp, err := downloader.Download(req)
	if respp != nil {
		sched.sendResp(*respp, code)
	}
	if err != nil {
		sched.sendError(err, code)
	}


}

//发送响应
func (sched *myScheduler) sendResp(resp base.Response, code string) bool {
	if sched.stopSign.Signed() {
		sched.stopSign.Deal(code)
		return false
	}

	sched.getRespChan() <- resp
	return true

}


//获取通道管理器持有的请求通道
func (sched *myScheduler) getReqChan() chan base.Request {
	reqChan, err := sched.chanman.ReqChan()
	if err != nil {
		panic(err)
	}
	return reqChan
}


//获取通道管理器持有的响应通道
func (sched *myScheduler) getRespChan() chan base.Response {
	respChan, err := sched.chanman.RespChan()
	if err != nil {
		panic(err)
	}
	return respChan
}

func (sched *myScheduler) sendError(err error, code string) bool {
	if err == nil {
		return false
	}
	codePrefix := parseCode(code)[0]
	var errorType base.ErrorType
	switch codePrefix {
	case DOWNLOADER_CODE:
		errorType = base.DOWNLOADER_ERROR
	case ANALYZER_CODE:
		errorType = base.ANALYZER_ERROR
	case ITEMPIPELINE_CODE:
		errorType = base.ITEM_PROCESSOR_ERROR
	}
	cError := base.NewCrawlerError(errorType, err.Error())

	if sched.stopSign.Signed(){
		sched.stopSign.Deal(code)
		return false
	}
	go func() {
		sched.getErrorChan() <- cError
	}()
	return true
}

// 解析组件实例代号。
func parseCode(code string) []string {
	result := make([]string, 2)
	var codePrefix string
	var id string
	index := strings.Index(code, "-")
	if index > 0 {
		codePrefix = code[:index]
		id = code[index+1:]
	} else {
		codePrefix = code
	}
	result[0] = codePrefix
	result[1] = id
	return result
}

//获取通道管理器持有的请求通道
func (sched *myScheduler) getErrorChan() chan error {
	errorChan, err := sched.chanman.ErrorChan()
	if err != nil {
		panic(err)
	}
	return errorChan
}


func (sched *myScheduler) activateAnalyzers(respParsers []anlz.ParseResponse) {
	go func() {
		for {
			resp, ok := <-sched.getRespChan()
			if !ok {
				break
			}
			go sched.analyze(respParsers, resp)
		}
	}()
}


func (sched *myScheduler) analyze(respParsers []anlz.ParseResponse, resp base.Response) {
	defer func() {
		if p := recover(); p != nil {
			errMsg := fmt.Sprintf("Fatal Analysis Error: %s\n", p)
			logger.Fatal(errMsg)
		}
	}()
	analyzer, err := sched.analyzerPool.Take()
	if err != nil {
		errMsg := fmt.Sprintf("Analyzer pool error: %s", err)
		sched.sendError(errors.New(errMsg), SCHEDULER_CODE)
		return
	}
	defer func() {
		err := sched.analyzerPool.Return(analyzer)
		if err != nil {
			errMsg := fmt.Sprintf("Analyzer pool error: %s", err)
			sched.sendError(errors.New(errMsg), SCHEDULER_CODE)
		}
	}()

	code := generateCode(ANALYZER_CODE, analyzer.Id())
	dataList, errs := analyzer.Analyze(respParsers, resp)
	if dataList != nil {
		for _, data := range dataList {
			if data == nil {
				continue
			}
			switch d := data.(type) {
			case *base.Request:
				sched.saveReqToCache(*d, code)
			case *base.Item:
				sched.sendItem(*d, code)
			default:
				errMsg := fmt.Sprintf("Unsupported data type '%T'! (value=%v)\n",d ,d)
				sched.sendError(errors.New(errMsg), code)
			}
		}
	}
	if errs != nil {
		for _, err := range errs {
			if err == nil {
				continue
			}
			sched.sendError(err, code)
		}
	}

}

// 生成组件实例代号。
func generateCode(prefix string, id uint32) string {
	return fmt.Sprintf("%s-%d", prefix, id)
}


func (sched *myScheduler) sendItem(item base.Item, code string) bool {
	if sched.stopSign.Signed() {
		sched.stopSign.Deal(code)
		return false
	}

	sched.getItemChan() <- item
	return true
}


//把请求存放到请求缓存
func (sched *myScheduler) saveReqToCache(req base.Request, code string) bool {
	httpReq := req.HttpReq()
	if httpReq == nil {
		logger.Warnln("Ignore the request! It's HTTP request is invalid!")
		return false
	}

	reqUrl := httpReq.URL
	if reqUrl == nil {
		logger.Warnln("Ignore the request! It's url is invalid!")
		return false
	}

	if strings.ToLower(reqUrl.Scheme) != "http"{
		logger.Warnln("Ignore the request! It's url scheme '%s' , but should be 'http'!\n", reqUrl.Scheme)
		return false
	}

	if _, ok := sched.urlMap[reqUrl.String()]; ok {
		logger.Warnf("Ignore the request! It's url is repeated. (requestUrl=%s)\n", reqUrl)
		return false
	}

	if pd, _ := getPrimaryDomain(httpReq.Host); pd != sched.primaryDomain {
		logger.Warnf("Ignore the request! It's host '%s' not in primary domain '%s'. (requestUrl=%s)\n", httpReq.Host, sched.primaryDomain, reqUrl)
		return false
	}

	if req.Depth() > sched.crawlDepth {
		logger.Warnf("Ignore the request! It's depth %d greater than %d. (requestUrl=%s)\n", req.Depth(), sched.crawlDepth, reqUrl)
		return false
	}

	if sched.stopSign.Signed() {
		sched.stopSign.Deal(code)
		return false
	}
	sched.reqCache.put(&req)
	sched.urlMap[reqUrl.String()] = true
	return true
}


//打开条目处理管道
func (sched *myScheduler) openItemPipeline() {
	go func() {
		sched.itemPipeline.SetFailFast(true)
		code := ITEMPIPELINE_CODE
		for item := range sched.getItemChan() {
			go func(item base.Item){
				defer func() {
					if p := recover(); p != nil {
						errMsg := fmt.Sprintf("Fatal Item Processing Error: %s\n", p)
						logger.Fatal(errMsg)
					}
				}()
				errs := sched.itemPipeline.Send(item)
				if errs !=nil {
					for _, err := range errs {
						sched.sendError(err, code)
					}
				}
			}(item)
		}
	}()
}


// 获取通道管理器持有的条目通道。
func (sched *myScheduler) getItemChan() chan base.Item {
	itemChan, err := sched.chanman.ItemChan()
	if err != nil {
		panic(err)
	}
	return itemChan
}

//调度。适当的搬运请求缓存中的请求到请求通道
func (sched *myScheduler) schedule(interval time.Duration) {
	go func() {
		for {
			if sched.stopSign.Signed() {
				sched.stopSign.Deal(SCHEDULER_CODE)
				return
			}
			remainder := cap(sched.getReqChan()) - len(sched.getReqChan())
			var temp *base.Request
			for remainder > 0 {
				temp = sched.reqCache.get()
				if temp == nil {
					break
				}
				if sched.stopSign.Signed() {
					sched.stopSign.Deal(SCHEDULER_CODE)
					return
				}
				sched.getReqChan() <- *temp
				remainder--
			}
			time.Sleep(interval)
		}
	}()
}


func (sched *myScheduler) Stop() bool {
	if atomic.LoadUint32(&sched.running) != 1 {
		return false
	}
	sched.stopSign.Sign()
	sched.chanman.Close()
	sched.reqCache.close()
	atomic.StoreUint32(&sched.running, 2)
	return true
}

func (sched *myScheduler) Running() bool {
	return atomic.LoadUint32(&sched.running) == 1
}

func (sched *myScheduler) ErrorChan() <- chan error {
	if sched.chanman.Status() != middleware.CHANNEL_MANAGER_STATUS_INITIALIZED {
		return nil
	}
	return sched.getErrorChan()
}

func (sched *myScheduler) Idle() bool {
	idleDlPool := sched.dlpool.Used() == 0
	idleAnalyzerPool := sched.analyzerPool.Used() == 0
	idleItemPipeline := sched.itemPipeline.ProcessingNumber() == 0

	if idleDlPool && idleAnalyzerPool && idleItemPipeline {
		return true
	}
	return false
}


func NewSchedDummary(sched *myScheduler, prefix string) SchedSummary {

}

func (sched *myScheduler) Summary(prefix string) SchedSummary {
	return NewSchedDummary(sched, prefix)
}


//请求缓存的接口类型
type requestCache interface {
	put(req *base.Request) bool
	get() *base.Request
	capacity() int
	length() int
	close()
	summary() string
}

type reqCacheBySlice struct {
	cache []*base.Request
	mutex sync.Mutex
	//值为1代表请求缓存已被关闭
	status byte
}

func newRequestCache() requestCache {
	rc := &reqCacheBySlice {
		cache: make([]*base.Request, 0),
	}
	return rc
}

func (rcCache *reqCacheBySlice) put(req *base.Request) bool {
	if req == nil {
		return false
	}
	if rcCache.status == 1 {
		return false
	}
	rcCache.mutex.Lock()
	defer rcCache.mutex.Unlock()
	rcCache.cache = append(rcCache.cache, req)
	return true
}

func (rcCache *reqCacheBySlice) get() *base.Request {
	if rcCache.length() == 0 {
		return nil
	}
	if rcCache.status == 1 {
		return nil
	}
	rcCache.mutex.Lock()
	defer rcCache.mutex.Unlock()
	req := rcCache.cache[0]
	rcCache.cache = rcCache.cache[1:]
	return req
}


func (rcCache *reqCacheBySlice) capacity() int {
	return cap(rcCache.cache)
}

func (rcCache *reqCacheBySlice) length() int {
	return len(rcCache.cache)
}

func (rcCache *reqCacheBySlice) close() {
	if rcCache.status == 1 {
		return
	}
	rcCache.status = 1
}

//摘要信息模板
var summaryTemplate = "status: %s," + "length: %d," + "capacity: %d"

func (rcCache *reqCacheBySlice) summary() string {
	summary := fmt.Sprintf(summaryTemplate,
		statusMap[rcCache.status],
		rcCache.length(),
		rcCache.capacity())
		return summary
}

//状态字典
var statusMap = map[byte]string{
	0: "running",
	1:"closed",
}

type mySchedSummary struct {
	prefix string
	running uint32//运行标记
	poolSize uint32//池大小
	channelLen uint//通道总长度
	crawlDepth uint32//爬取的最大深度

	chanmanSummary string
	reqCacheSummary string
	itemPipelineSummary string
	stopSignSummary string

	dlPoolLen uint32
	dlPoolCap uint32
	analyzerPoolLen uint32
	analyzerPoolCap uint32

	urlCount int //已请求的URL的计数
	urlDetail string//已请求的URL的详细信息
}

//创建调度器摘要信息
func NewSchedSummary(sched *myScheduler, prefix string) SchedSummary {
	if sched == nil {
		return nil
	}

	urlCount := len(sched.urlMap)
	var urlDetail string
	if urlCount >0 {
		var buffer bytes.Buffer
		buffer.WriteByte('\n')
		for k, _ := range sched.urlMap {
			buffer.WriteString(prefix)
			buffer.WriteString(prefix)
			buffer.WriteString(k)
			buffer.WriteByte('\n')
		}
		urlDetail = buffer.String()
	}else {
		urlDetail = "\n"
	}

	return &mySchedSummary{
		prefix: prefix,
		running: sched.running,
		poolSize: sched.poolSize,
		channelLen: sched.channelLen,
		crawlDepth: sched.crawlDepth,
		chanmanSummary: sched.chanman.Summary(),

		reqCacheSummary: sched.reqCache.summary(),
		dlPoolLen: sched.dlpool.Used(),
		dlPoolCap: sched.dlpool.Total(),
		analyzerPoolLen: sched.analyzerPool.Used(),
		analyzerPoolCap: sched.analyzerPool.Total(),
		itemPipelineSummary: sched.itemPipeline.Summary(),
		urlCount: urlCount,
		urlDetail: urlDetail,
		stopSignSummary: sched.stopSign.Summary(),
	}
}



//获取摘要信息
func (ss *mySchedSummary) getSummary(detail bool) string {
	template := ss.prefix + "Running: %v \n" +
		ss.prefix + "Pool Size: %d \n" +
		ss.prefix + "Channel length: %d \n" +
		ss.prefix + "Crawl depth: %d \n" +
		ss.prefix + "Channels manager: %s\n" +
		ss.prefix + "Request cache: %s\n" +
		ss.prefix + "Downloader pool: %d/%d\n" +
		ss.prefix + "Analyzer pool: %d/%d\n" +
		ss.prefix + "Item pipeline: %s\n" +
		ss.prefix + "Urls(%d): %s" +
		ss.prefix + "Stop Sign: %s\n"

	return fmt.Sprintf(template,
		func() bool {
			return ss.running == 1
		}(),
		ss.poolSize,
		ss.channelLen,
		ss.crawlDepth,
		ss.chanmanSummary,
		ss.reqCacheSummary,
		ss.dlPoolLen,ss.dlPoolCap,
		ss.analyzerPoolLen,ss.analyzerPoolCap,
		ss.itemPipelineSummary,
		ss.urlCount,
		func() string {
			if detail {
				return ss.urlDetail
			} else {
				return "<concealed>\n"
			}
		}(),
		ss.stopSignSummary)
}

func (ss *mySchedSummary) String() string {
	return ss.getSummary(false)
}

func (ss *mySchedSummary) Detail() string {
	return ss.getSummary(true)
}

func (ss *mySchedSummary) Same(other SchedSummary) bool {
	if other == nil {
		return false
	}
	otherSs, ok := interface{}(other).(*mySchedSummary)
	if !ok {
		return false
	}

	if ss.running != otherSs.running ||
		ss.poolSize != otherSs.poolSize ||
		ss.channelLen != otherSs.channelLen ||
		ss.crawlDepth != otherSs.crawlDepth ||
		ss.dlPoolLen != otherSs.dlPoolLen ||
		ss.dlPoolCap != otherSs.dlPoolCap ||
		ss.analyzerPoolLen != otherSs.analyzerPoolLen ||
		ss.analyzerPoolCap != otherSs.analyzerPoolCap ||
		ss.urlCount != otherSs.urlCount ||
		ss.stopSignSummary != otherSs.stopSignSummary ||
		ss.reqCacheSummary != otherSs.reqCacheSummary ||
		ss.itemPipelineSummary != otherSs.itemPipelineSummary ||
		ss.chanmanSummary != otherSs.chanmanSummary {
			return false
	} else {
		return true
	}


}
















































