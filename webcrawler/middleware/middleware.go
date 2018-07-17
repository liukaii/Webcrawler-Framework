package middleware

import (
	"Webcrawler-Framework/webcrawler/base"
	"errors"
	"sync"
	"fmt"
	"reflect"
	"math"
)



type ChannelManagerStatus uint8

const (
	CHANNEL_MANAGER_STATUS_UNINITIALIZED ChannelManagerStatus = 0  //未初始化状态
	CHANNEL_MANAGER_STATUS_INITIALIZED ChannelManagerStatus = 1  //已初始化状态
	CHANNEL_MANAGER_STATUS_CLOSED ChannelManagerStatus = 2  //已关闭状态
	defaultChanLen uint = 15
)

var statusNameMap = map[ChannelManagerStatus]string {
	CHANNEL_MANAGER_STATUS_UNINITIALIZED: "uninitialized",
	CHANNEL_MANAGER_STATUS_INITIALIZED: "initialized",
	CHANNEL_MANAGER_STATUS_CLOSED: "closed",
}

var chanmanSummaryTemplate = "status: %s, " +
	"requestChannel: %d/%d, " +
	"responseChannel: %d/%d, " +
	"itemChannel: %d/%d, " +
	"errorChannel: %d/%d "

//通道管理器的接口类型
type ChannelManager interface {
	Init(channelLen uint, reset bool) bool

	Close() bool

	ReqChan() (chan base.Request, error)
	RespChan() (chan base.Response, error)

	ItemChan() (chan base.Item, error)

	ErrorChan() (chan error, error)

	ChannelLen() uint

	Status() ChannelManagerStatus

	Summary() string

}

//实体池的接口类型
type Pool interface {
	Take() (Entity, error)
	Return(entity Entity) error
	Total() uint32
	Used() uint32
}

type Entity interface {
	Id() uint32
}

//ID生成器的接口类型， 其实现类型还需要完成
type IdGenertor interface {
	GetUint32() uint32
}


type  StopSign interface{
	Sign() bool

	Signed() bool

	Reset()

	Deal(code string)

	DealCount(code string) uint32

	DealTotal() uint32

	Summary() string
}



type myChannelManager struct {
	channelLen uint
	reqCh chan base.Request
	respCh chan base.Response
	itemCh chan base.Item
	errorCh chan error
	status ChannelManagerStatus
	rwmutex sync.RWMutex
}

//创建通道管理器
func NewChannelManager(channelLen uint) ChannelManager {
	if channelLen == 0 {
		channelLen = defaultChanLen
	}
	chanman := &myChannelManager{}
	chanman.Init(channelLen, true)
	return chanman
}


func (chanman *myChannelManager) Init(channelLen uint, reset bool) bool {
	if channelLen == 0 {
		panic(errors.New("The channel length is invalid!"))
	}
	chanman.rwmutex.Lock()
	defer chanman.rwmutex.Unlock()
	if chanman.status == CHANNEL_MANAGER_STATUS_INITIALIZED && !reset {
		return false
	}
	chanman.channelLen = channelLen
	chanman.reqCh = make(chan base.Request, channelLen)
	chanman.respCh = make(chan base.Response, channelLen)
	chanman.itemCh = make(chan base.Item, channelLen)
	chanman.errorCh = make(chan error, channelLen)
	chanman.status = CHANNEL_MANAGER_STATUS_INITIALIZED
	return true
}

func (chanman *myChannelManager) Close() bool {
	chanman.rwmutex.Lock()
	defer chanman.rwmutex.Unlock()
	if chanman.status != CHANNEL_MANAGER_STATUS_INITIALIZED {
		return false
	}
	close(chanman.reqCh)
	close(chanman.respCh)
	close(chanman.itemCh)
	close(chanman.errorCh)
	chanman.status = CHANNEL_MANAGER_STATUS_CLOSED
	return true
}

func (chanman *myChannelManager) checkStatus() error {
	if chanman.status == CHANNEL_MANAGER_STATUS_INITIALIZED {
		return nil
	}
	statusName, ok := statusNameMap[chanman.status]
	if !ok {
		statusName = fmt.Sprintf("%d", chanman.status)
	}
	errMsg := fmt.Sprintf("The undesirable status of channel manager: %s!\n", statusName)
	return errors.New(errMsg)
}


func (chanman *myChannelManager) ReqChan() (chan base.Request, error) {
	chanman.rwmutex.RLock()
	defer chanman.rwmutex.RUnlock()
	if err := chanman.checkStatus(); err != nil {
		return nil, err
	}
	return chanman.reqCh, nil
}

func (chanman *myChannelManager) RespChan() (chan base.Response, error) {
	chanman.rwmutex.RLock()
	defer chanman.rwmutex.RUnlock()
	if err := chanman.checkStatus(); err != nil {
		return nil, err
	}
	return chanman.respCh, nil
}

func (chanman *myChannelManager) ItemChan() (chan base.Item, error) {
	chanman.rwmutex.RLock()
	defer chanman.rwmutex.RUnlock()
	if err := chanman.checkStatus(); err != nil {
		return nil, err
	}
	return chanman.itemCh, nil
}

func (chanman *myChannelManager) ErrorChan() (chan error, error) {
	chanman.rwmutex.RLock()
	defer chanman.rwmutex.RUnlock()
	if err := chanman.checkStatus(); err != nil {
		return nil, err
	}
	return chanman.errorCh,nil
}

func (chanman *myChannelManager) ChannelLen() uint {
	return chanman.channelLen
}

func (chanman *myChannelManager) Status() ChannelManagerStatus {
	return chanman.status
}

func (chanman *myChannelManager) Summary() string {
	summary := fmt.Sprintf(chanmanSummaryTemplate, statusNameMap[chanman.status],
		len(chanman.reqCh), cap(chanman.reqCh),
		len(chanman.respCh), cap(chanman.respCh),
		len(chanman.itemCh), cap(chanman.itemCh),
		len(chanman.errorCh), cap(chanman.errorCh))
	return summary
}

//实体池的实现类型
type myPool struct {
	total uint32
	etype reflect.Type
	genEntity func() Entity
	container chan Entity
	idContainer map[uint32]bool
	mutex sync.Mutex
}

func NewPool(total uint32, entityType reflect.Type, genEntity func() Entity) (Pool, error) {
	if total == 0 {
		errMsg := fmt.Sprintf("The pool can not be initialized! (total=%d)\n", total)
		return nil, errors.New(errMsg)
	}
	size := int(total)
	container := make(chan Entity, size)
	idContainer := make(map[uint32]bool)
	for i := 0; i < size; i ++ {
		newEntity := genEntity()
		if entityType != reflect.TypeOf(newEntity) {
			errMsg := fmt.Sprintf("The type of result of function genEntity() is NOT %s!\n", entityType)
			return nil, errors.New(errMsg)
		}
		container <- newEntity
		idContainer[newEntity.Id()] = true
	}
	pool := &myPool{
		total: total,
		etype: entityType,
		genEntity: genEntity,
		container: container,
		idContainer:idContainer,
	}
	return pool, nil
}

func (pool *myPool) Take() (Entity, error) {
	entity, ok := <- pool.container
	if !ok {
		return nil, errors.New("The inner contianer is invalid!")
	}
	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	pool.idContainer[entity.Id()] = false
	return entity, nil
}

func (pool *myPool) Return(entity Entity) error {
	if entity == nil {
		return errors.New("The returning entity is invalid!")
	}
	if pool.etype != reflect.TypeOf(entity) {
		errMsg := fmt.Sprintf("The type of returning entity is NOT %s!\n", pool.etype)
		return errors.New(errMsg)
	}
	entityId := entity.Id()
	casResult := pool.compareAndSetForIdContainer(entityId, false, true)
	if casResult == 1 {
		pool.container <- entity
		return nil
	}else if casResult == -1 {
		errMsg := fmt.Sprintf("The entity (id=%d) is illegal!\n", entity.Id())
		return errors.New(errMsg)
	}else {
		errMsg := fmt.Sprintf("The entity (id=%d) is already in the pool!\n", entity.Id())
		return errors.New(errMsg)
	}
}

//比较并设置实体ID容器中与给定实体ID对应的键值对的元素值
//结果值
//-1：键值对不存在
//0：操作失败
//1：操作成功
func (pool *myPool) compareAndSetForIdContainer(entityId uint32, oldValue bool, newValue bool) int8 {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	v, ok := pool.idContainer[entityId]
	if !ok {
		return -1
	}
	if v != oldValue {
		return 0
	}
	pool.idContainer[entityId] = newValue
	return 1
}

func(pool *myPool) Total() uint32 {
	return pool.total
}

func(pool *myPool) Used() uint32 {
	return pool.total - uint32(len(pool.container))
}


type myStopSign struct {
	rwmutex sync.RWMutex

	signed bool
	dealCountMap map[string]uint32
}

func NewStopSign() StopSign {
	ss := &myStopSign{
		dealCountMap: make(map[string]uint32),
	}
	return ss
}

func (ss *myStopSign) Sign() bool {
	ss.rwmutex.Lock()
	defer ss.rwmutex.Unlock()
	if ss.signed {
		return false
	}
	ss.signed = true
	return true
}

func (ss *myStopSign) Signed() bool {
	return ss.signed
}

func (ss *myStopSign) Deal(code string) {
	ss.rwmutex.Lock()
	defer ss.rwmutex.Unlock()
	if !ss.signed {
		return
	}
	if _, ok := ss.dealCountMap[code]; !ok {
		ss.dealCountMap[code] = 1
	}else {
		ss.dealCountMap[code] += 1
	}
}

func (ss *myStopSign) Reset() {
	ss.rwmutex.Lock()
	defer ss.rwmutex.Unlock()
	ss.signed = false
	ss.dealCountMap = make(map[string]uint32)
}


func (ss *myStopSign) DealCount(code string) uint32 {
	ss.rwmutex.RLock()
	defer ss.rwmutex.RUnlock()
	return ss.dealCountMap[code]
}

func (ss *myStopSign) DealTotal() uint32 {
	ss.rwmutex.RLock()
	defer ss.rwmutex.RUnlock()
	var total uint32 = 0
	for _, v := range ss.dealCountMap {
		total = total + v
	}
	return total
}

func (ss *myStopSign) Summary() string {
	summary := fmt.Sprintf("The StopSign has been sent: %s, the total of dealing StopSign is: %d.\n", ss.signed, ss.DealTotal())
	return summary
}


type cyclicIdGenertor struct {
	sn uint32
	ended bool
	mutex sync.Mutex
}


func NewIdGenertor() IdGenertor {
	return &cyclicIdGenertor{}
}

func (gen *cyclicIdGenertor) GetUint32() uint32 {
	gen.mutex.Lock()
	defer gen.mutex.Unlock()
	if gen.ended {
		defer func() { gen.ended = false }()
		gen.sn = 0
		return gen.sn
	}
	id := gen.sn
	if id < math.MaxUint32 {
		gen.sn ++
	} else {
		gen.ended = true
	}
	return id
}














































































































































