package itempipeline

import "webcrawler/base"

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


