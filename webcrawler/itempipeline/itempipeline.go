package itempipeline

import (
	"Webcrawler-Framework/webcrawler/base"
	"errors"
	"fmt"
	"sync/atomic"
)

//条目处理管道的接口类型
type ItemPipeline interface {
	Send(item base.Item) []error
	//快速失败，条目处理在某一个步骤上出错则忽略后续所有步骤并报告错误
	FailFast() bool
	//设置是否快速失败
	SetFailFast(failFast bool)
	//获得已发送、已接受和已处理的条目计数值
	Count() []uint64
	//正在被处理的条目数量
	ProcessingNumber() uint64
	//获取摘要信息
	Summary() string
}

//用来处理条目的函数类型
type ProcessItem func(item base.Item) (result base.Item, err error)

type myItemPipeline struct {
	itemProcessors []ProcessItem
	failFast bool
	sent uint64
	accepted uint64
	processed uint64
	processingNumber uint64
}

func NewPipeline(itemProcessors []ProcessItem) ItemPipeline {
	if itemProcessors == nil {
		panic(errors.New(fmt.Sprintln("Invalid item processor list!")))
	}
	innerItemProcessors := make([]ProcessItem, 0)
	for i, ip := range itemProcessors {
		if ip == nil {
			panic(errors.New(fmt.Sprintf("Invalid item processor![%d]!\n", i)))
		}
		innerItemProcessors = append(innerItemProcessors, ip)
	}
	return &myItemPipeline{itemProcessors: innerItemProcessors}
}

func (ip *myItemPipeline) Send(item base.Item) []error {
	atomic.AddUint64(&ip.processingNumber, 1)

	defer atomic.AddUint64(&ip.processingNumber, ^uint64(0))

	atomic.AddUint64(&ip.sent, 1)

	errs := make([]error, 0)
	if item == nil {
		errs = append(errs, errors.New("The item is invalid!"))
		return errs
	}

	atomic.AddUint64(&ip.accepted, 1)

	var currentItem base.Item = item
	for _, itemProcessor := range ip.itemProcessors {
		processedItem, err := itemProcessor(currentItem)
		if err != nil {
			errs= append(errs, err)
			if ip.failFast {
				break
			}
		}
		if processedItem != nil {
			currentItem = processedItem
		}
	}

	atomic.AddUint64(&ip.processed, 1)
	return errs
}

func (ip *myItemPipeline) FailFast() bool {
	if ip.failFast {
		return true
	}
	return false
}

func (ip *myItemPipeline) SetFailFast(failfast bool) {
	ip.failFast = failfast
}

func (ip *myItemPipeline) Count() []uint64 {
	counts := make([]uint64, 3)
	counts[0] = atomic.LoadUint64(&ip.sent)
	counts[1] = atomic.LoadUint64(&ip.accepted)
	counts[2] = atomic.LoadUint64(&ip.processed)
	return counts
}

func (ip *myItemPipeline) ProcessingNumber() uint64 {
	return ip.processingNumber
}

var summaryTemplate = "failFast: %v, procesorNumber: %d," +
	" send: %d, accepted: %d, processed: %d, processingNumber: %d"

func (ip *myItemPipeline) Summary() string {
	counts := ip.Count()
	summary := fmt.Sprintf(summaryTemplate, ip.failFast, len(ip.itemProcessors), counts[0], counts[1], counts[2], ip.ProcessingNumber())
	return summary
}



































































































































