package kju

import (
	"sync"
	"sync/atomic"
	"time"
)

var _ CountableWaitGroup = &cwg{}

type CountableWaitGroup interface {
	// Add increments the CountableWaitGroup counter by 1.
	Add()
	// Done decrements the CountableWaitGroup counter by 1.
	Done()
	// Wait returns a channel that's closed  when CountableWaitGroup
	// counter is zero.
	Wait() <-chan struct{}
	// WaitTimeout block until the CountableWaitGroup counter is zero
	// or the specified max timeout is reached. Returns zero if it exits
	// normally or current value of counter in case of timeout.
	WaitTimeout(timeout time.Duration) int
	// Count returns the current value of CountableWaitGroup counter.
	Count() int
}

func NewCountableWaitGroup() CountableWaitGroup {
	var c int32
	return &cwg{
		wg:      &sync.WaitGroup{},
		counter: &c,
	}
}

type cwg struct {
	wg      *sync.WaitGroup
	counter *int32
}

func (c *cwg) Add() {
	atomic.AddInt32(c.counter, int32(1))
	c.wg.Add(1)
}

func (c *cwg) Done() {
	atomic.AddInt32(c.counter, -1)
	c.wg.Done()
}

func (c *cwg) Wait() <-chan struct{} {
	cc := make(chan struct{})
	go func() {
		defer close(cc)
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
		return c.Count()
	}
}

func (c *cwg) Count() int {
	return int(*c.counter)
}
