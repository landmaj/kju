package kju

import (
	"sync"
	"sync/atomic"
	"time"
)

var _ WaitGroup = &cwg{}

type WaitGroup interface {
	// Add increments the WaitGroup counter by delta.
	Add(delta int)
	// AddTask increments the WaitGroup task counter by 1.
	AddTask()
	// Done decrements the WaitGroup counter by 1.
	Done()
	// TaskDone decrements the WaitGroup task counter by 1.
	TaskDone()
	// Wait returns a channel that's closed  when WaitGroup
	// counters are zero.
	Wait() <-chan struct{}
	// WaitTimeout block until the WaitGroup counters are zero
	// or the specified max timeout is reached. Returns zero if it exits
	// normally or current value of task counter in case of timeout.
	WaitTimeout(timeout time.Duration) int
	// TaskCount returns the current value of WaitGroup task counter.
	TaskCount() int
}

func NewWaitGroup() WaitGroup {
	var c int32
	return &cwg{
		wg:      &sync.WaitGroup{},
		tasks:   &sync.WaitGroup{},
		counter: &c,
	}
}

type cwg struct {
	wg      *sync.WaitGroup
	tasks   *sync.WaitGroup
	counter *int32
}

func (c *cwg) Add(delta int) {
	c.wg.Add(delta)
}

func (c *cwg) AddTask() {
	atomic.AddInt32(c.counter, int32(1))
	c.tasks.Add(1)
}

func (c *cwg) Done() {
	c.wg.Done()
}

func (c *cwg) TaskDone() {
	atomic.AddInt32(c.counter, -1)
	c.tasks.Done()
}

func (c *cwg) Wait() <-chan struct{} {
	cc := make(chan struct{})
	go func() {
		defer close(cc)
		c.tasks.Wait()
		c.wg.Wait()
	}()
	return cc
}

func (c *cwg) WaitTimeout(timeout time.Duration) int {
	cc := make(chan struct{})
	go func() {
		defer close(cc)
		<-c.Wait()
	}()
	select {
	case <-cc:
		return 0
	case <-time.After(timeout):
		return c.TaskCount()
	}
}

func (c *cwg) TaskCount() int {
	return int(*c.counter)
}
