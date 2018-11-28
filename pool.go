package datastoreConnectionPool

import (
	"cloud.google.com/go/datastore"
	"context"
	"errors"
	"sync"
)

type Pool struct {
	mux             sync.Mutex
	pool            []*datastore.Client
	open            []int
	ProjectId       string
	MinimumOpen     int
	MaximumExisting int
}

func (pool *Pool) growToMimimum() error {
	for i := 0; len(pool.open) < pool.MinimumOpen && len(pool.pool) < pool.MaximumExisting; i++ {
		if _, err := pool.addDS(); err != nil {
			return err
		}
		if i > pool.MaximumExisting {
			return errors.New("alloc stuck in loop")
		}
	}
	return nil
}

func (pool *Pool) addDS() (*PoolItem, error) {
	pool.mux.Lock()
	defer pool.mux.Unlock()
	i := len(pool.pool)
	dsClient, err := datastore.NewClient(context.Background(), pool.ProjectId)
	if err != nil {
		return nil, err
	}
	pool.pool = append(pool.pool, dsClient)
	pool.open = append(pool.open, i)
	pi := newPoolItem(pool, i)
	return pi, nil
}

func newPoolItem(pool *Pool, index int) *PoolItem {
	pi := &PoolItem{
		pool:     pool,
		DSClient: pool.pool[index],
		InUse:    false,
		poolItem: index,
	}
	return pi
}

func (pool *Pool) DSClient() (*PoolItem, error) {
	if len(pool.open) == 0 {
		if err := pool.growToMimimum(); err != nil {
			if len(pool.open) == 0 {
				return nil, err
			}
		}
	}
	if len(pool.open) == 0 {
		return nil, errors.New("no connections available")
	}
	pool.mux.Lock()
	defer pool.mux.Unlock()
	i := pool.open[0]
	pool.open = pool.open[1:]
	pi := newPoolItem(pool, i)
	return pi, nil
}

var (
	globalPool *Pool
)

func InitGlobalWithProjectId(projectId string) error {
	globalPool = New(projectId)
	if err := globalPool.growToMimimum(); err != nil {
		return err
	}
	return nil
}

func DSClient() (*PoolItem, error) {
	return globalPool.DSClient()
}

func New(projectId string) *Pool {
	return &Pool{
		pool:            []*datastore.Client{},
		ProjectId:       projectId,
		open:            []int{},
		MaximumExisting: 1000,
		MinimumOpen:     10,
	}
}

type PoolItem struct {
	DSClient *datastore.Client
	poolItem int
	InUse    bool
	pool     *Pool
}

func (pi *PoolItem) Release() error {
	pi.pool.mux.Lock()
	defer pi.pool.mux.Unlock()
	pi.pool.open = append(pi.pool.open, pi.poolItem)
	pi.InUse = false
	return nil
}
