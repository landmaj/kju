package kju

import (
	"sync"
	"sync/atomic"
)

func newCustomWaitGroup() *customWaitGroup {
	var counter int32
	return &customWaitGroup{
		wg:      &sync.WaitGroup{},
		tasks:   &sync.WaitGroup{},
		counter: &counter,
	}
}

type customWaitGroup struct {
	wg      *sync.WaitGroup
	tasks   *sync.WaitGroup
	counter *int32
}

// Add increments the customWaitGroup counter by delta.
func (c *customWaitGroup) Add(delta int) {
	c.wg.Add(delta)
}

// AddTask increments the customWaitGroup task counter by 1.
func (c *customWaitGroup) AddTask() {
	atomic.AddInt32(c.counter, int32(1))
	c.tasks.Add(1)
}

// Done decrements the customWaitGroup counter by 1.
func (c *customWaitGroup) Done() {
	c.wg.Done()
}

// TaskDone decrements the customWaitGroup task counter by 1.
func (c *customWaitGroup) TaskDone() {
	atomic.AddInt32(c.counter, -1)
	c.tasks.Done()
}

// Wait blocks until customWaitGroup main counter is zero.
func (c *customWaitGroup) Wait() {
	c.wg.Wait()
}

// WaitTasks blocks until customWaitGroup task counter is zero.
func (c *customWaitGroup) WaitTasks() {
	c.tasks.Wait()
}

// TaskCount returns the current value of customWaitGroup task counter.
func (c *customWaitGroup) TaskCount() int {
	return int(*c.counter)
}
