package downloader

import (
	"Webcrawler-Framework/webcrawler/base"
	"net/http"
	"Webcrawler-Framework/webcrawler/middleware"
	"reflect"
	"fmt"
	"errors"
)

var downloaderIdGenerator middleware.IdGenertor = middleware.NewIdGenertor()


//网络下载器的接口类型
type PageDownloader interface {
	Id()  uint32//获得ID
	Download(req base.Request) (*base.Response, error) //根据请求下载网页并返回响应
}


type PageDownloaderPool interface {
	Take() (PageDownloader, error)//从池中取出一个网页下载器
	Return(dl PageDownloader) error//把一个下载器返回给池
	Total() uint32//获得池的总容量
	Used() uint32//获得正在被使用的网页下载器的数量
}

type myPageDownloader struct {
	httpClient http.Client

	id uint32
}

//将网页下载器与ID生成器完全解耦
func genDownloaderId() uint32 {
	return downloaderIdGenerator.GetUint32()
}

func NewPageDownloader(client *http.Client) PageDownloader {
	id := genDownloaderId()
	if client == nil {
		client = &http.Client{}
	}
	return &myPageDownloader{
		id: id,
		httpClient: *client,
	}
}

func (dl *myPageDownloader) Id() uint32 {
	return dl.id
}

func (dl *myPageDownloader) Download(req base.Request) (*base.Response, error) {
	httpReq := req.HttpReq()
	httpResp, err := dl.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	return base.NewResponse(httpResp, req.Depth()), nil
}


type myDownloaderPool struct {
	pool middleware.Pool
	etype reflect.Type
}

type GenPageDownloader func() PageDownloader

func NewPageDownloaderPool(total uint32, gen GenPageDownloader) (PageDownloaderPool, error) {
	etype := reflect.TypeOf(gen())
	genEntity := func() middleware.Entity{ return gen()}
	pool, err := middleware.NewPool(total, etype, genEntity)
	if err != nil {
		return nil, err
	}
	dlpool := &myDownloaderPool{pool, etype}
	return dlpool,nil
}


func (dlpool *myDownloaderPool) Take() (PageDownloader, error) {
	entity, err := dlpool.pool.Take()
	if err != nil {
		return nil , err
	}
	dl, ok := entity.(PageDownloader)
	if !ok {
		errMsg := fmt.Sprintf("The type of entity is NOT %s!\n", dlpool.etype)
		panic(errors.New(errMsg))
	}
	return dl, nil
}

func (dlpool *myDownloaderPool) Return(dl PageDownloader) error {
	return dlpool.pool.Return(dl)
}

func (dlpool *myDownloaderPool) Total() uint32 {
	return dlpool.pool.Total()
}

func (dlpool *myDownloaderPool) Used() uint32 {
	return dlpool.pool.Used()
}


