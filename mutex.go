package mapmutex

import (
	"math/rand"
	"sync"
	"time"
)

// Mutex is the mutex with synchronized map
// it's for reducing unnecessary locks among different keys
type Mutex struct {
	locks     sync.Map
	maxRetry  int
	maxDelay  float64 // in nanosend
	baseDelay float64 // in nanosecond
	factor    float64
	jitter    float64
}

// TryLock tries to aquire the lock.
func (m *Mutex) TryLock(key interface{}) (gotLock bool) {
	for i := 0; i < m.maxRetry; i++ {
		if _, ok := m.locks.Load(key); ok { // if locked
			time.Sleep(m.backoff(i))
		} else { // if unlock, lockit
			m.locks.Store(key, struct{}{})
			return true
		}
	}

	return false
}

// Unlock unlocks for the key
// please call Unlock only after having aquired the lock
func (m *Mutex) Unlock(key interface{}) {
	m.locks.Delete(key)
}

// borrowed from grpc
func (m *Mutex) backoff(retries int) time.Duration {
	if retries == 0 {
		return time.Duration(m.baseDelay) * time.Nanosecond
	}
	backoff, max := m.baseDelay, m.maxDelay
	for backoff < max && retries > 0 {
		backoff *= m.factor
		retries--
	}
	if backoff > max {
		backoff = max
	}
	backoff *= 1 + m.jitter*(rand.Float64()*2-1)
	if backoff < 0 {
		return 0
	}
	return time.Duration(backoff) * time.Nanosecond
}

// NewMapMutex returns a mapmutex with default configs
func NewMapMutex() *Mutex {
	var l sync.Map
	return &Mutex{
		locks:     l,
		maxRetry:  100,
		maxDelay:  100000000, // 0.1 second
		baseDelay: 10,        // 10 nanosecond
		factor:    1.2,
		jitter:    100,
	}
}

// NewCustomizedMapMutex returns a customized mapmutex
func NewCustomizedMapMutex(mRetry int, mDelay, bDelay, factor, jitter float64) *Mutex {
	var l sync.Map
	return &Mutex{
		locks:     l,
		maxRetry:  mRetry,
		maxDelay:  mDelay,
		baseDelay: bDelay,
		factor:    factor,
		jitter:    jitter,
	}
}
