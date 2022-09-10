package masterslavepool

import (
	"sort"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	log "github.com/sirupsen/logrus"
)

type PoolNodeMeta struct {
	IsAlive    bool
	Reports    []time.Time
	LastReport time.Time
	BringAlive time.Time
}

type PoolNode[I any] struct {
	Item I
	Meta PoolNodeMeta
}

type MasterSlavePool[I any] struct {
	rwlock sync.RWMutex
	Master *PoolNode[I]
	Slaves []*PoolNode[I]
}

type DurationTuple[I any] struct {
	Duration time.Duration
	Item     I
}

type DurationTupleList[I any] []DurationTuple[I]

func (a DurationTupleList[I]) Len() int           { return len(a) }
func (a DurationTupleList[I]) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a DurationTupleList[I]) Less(i, j int) bool { return a[i].Duration < a[j].Duration }

func NewNode[I any](item I) *PoolNode[I] {
	return &PoolNode[I]{
		Item: item,
		Meta: PoolNodeMeta{
			IsAlive:    true,
			Reports:    []time.Time{},
			LastReport: time.Time{},
			BringAlive: time.Time{},
		},
	}
}

func NewEthClientMasterSlavePool(masterURL string,
	slaveURLs []string) (*MasterSlavePool[*ethclient.Client], error) {
	// Setup master
	ms, err := ethclient.Dial(masterURL)
	if err != nil {
		return nil, err
	}

	// Setup slaves
	slaves := []*PoolNode[*ethclient.Client]{}
	for _, url := range slaveURLs {
		cl, err := ethclient.Dial(url)
		if err != nil {
			return nil, err
		}
		slaves = append(slaves, NewNode(cl))
	}

	return &MasterSlavePool[*ethclient.Client]{
		rwlock: sync.RWMutex{},
		Master: NewNode(ms),
		Slaves: slaves,
	}, nil
}

func (m *MasterSlavePool[I]) Report(item I, timedOut bool) error {
	if !timedOut {
		return nil
	}
	// TODO
	return nil
}

func (m *MasterSlavePool[I]) GetItem() I {
	// Lock global RW lock for reads
	m.rwlock.RLock()

	// Check if master is alive, if so return master
	if m.Master.Meta.IsAlive {
		m.rwlock.RUnlock()
		return m.Master.Item
	}

	// If master is not alive, check if time has come to
	// recheck on master
	now := time.Now()
	if m.Master.Meta.BringAlive.Sub(now) == time.Duration(0) {
		m.rwlock.RUnlock()
		m.rwlock.Lock()
		m.Master.Meta = NewReadyPoolNodeMeta()
		m.rwlock.Unlock()
		return m.Master.Item
	}

	// If master is not alive, nor is the time to bring it
	// back online, check if any of the slaves is ready.
	for _, slave := range m.Slaves {
		sm := slave.Meta
		if sm.IsAlive {
			m.rwlock.RUnlock()
			return slave.Item
		}
		if sm.BringAlive.Sub(now) == time.Duration(0) {
			m.rwlock.RUnlock()
			m.rwlock.Lock()
			slave.Meta = NewReadyPoolNodeMeta()
			m.rwlock.Unlock()
			return slave.Item
		}
	}

	// If none of the others were successfully, we may have to
	// wait till first rpc comes back online and send it
	m.rwlock.RUnlock()
	return m.allFailureRecovery()
}

func (m *MasterSlavePool[I]) allFailureRecovery() I {
	log.Warn("critical rpc failure. All upstreams in cooldown state. Blocking application")
	currentTime := time.Now()

	list := DurationTupleList[*PoolNode[I]]{}

	m.rwlock.RLock()
	list = append(list, DurationTuple[*PoolNode[I]]{
		Duration: m.Master.Meta.BringAlive.Sub(currentTime),
		Item:     m.Master,
	})

	for _, slave := range m.Slaves {
		list = append(list, DurationTuple[*PoolNode[I]]{
			Duration: slave.Meta.BringAlive.Sub(currentTime),
			Item:     slave,
		})
	}
	m.rwlock.RUnlock()

	sort.Sort(list)

	m.rwlock.Lock()
	minDuration := list[0].Duration
	time.Sleep(minDuration)

	// Cleanup
	for _, tuple := range list {
		if tuple.Duration == minDuration {
			tuple.Item.Meta = NewReadyPoolNodeMeta()
		}
	}

	m.rwlock.Unlock()

	return list[0].Item.Item
}

func NewReadyPoolNodeMeta() PoolNodeMeta {
	return PoolNodeMeta{
		IsAlive:    true,
		Reports:    []time.Time{},
		LastReport: time.Time{},
		BringAlive: time.Time{},
	}
}
