// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package engine

import (
	"github.com/xdblab/xdb/common/clock"
	"github.com/xdblab/xdb/common/log"
	"time"
)

type (
	// TimerGate interface
	TimerGate interface {
		// FireChan return the signals channel of firing timers
		// after receiving an empty signal, caller should call Update to set up next one
		FireChan() <-chan struct{}
		// FireAfter checks whether the current timer will fire after the provided time
		FireAfter(checkTime time.Time) bool
		// Update updates the TimerGate, return true if update is successful
		// success means TimerGate is idle, or TimerGate is set with a sooner time to fire the timer
		Update(nextTime time.Time) bool
		// Close shutdown the TimerGate
		Close()
	}

	// LocalTimerGateImpl is a local timer implementation,
	// which basically is a wrapper of golang's timer
	LocalTimerGateImpl struct {
		// the channel which will be used to proxy the fired timer
		fireChan  chan struct{}
		closeChan chan struct{}

		timeSource clock.TimeSource

		// the actual timer which will fire
		timer *time.Timer
		// variable indicating when the above timer will fire
		nextWakeupTime time.Time
		logger         log.Logger
	}
)

// NewLocalTimerGate create a new timer gate instance
func NewLocalTimerGate(logger log.Logger) TimerGate {
	timer := &LocalTimerGateImpl{
		timer:          time.NewTimer(0),
		nextWakeupTime: time.Time{},
		fireChan:       make(chan struct{}, 1),
		closeChan:      make(chan struct{}),
		timeSource:     clock.NewRealTimeSource(),
		logger:         logger,
	}

	if !timer.timer.Stop() {
		// the timer should be stopped when initialized
		// but drain it just in case it's not
		<-timer.timer.C
	}

	go func() {
		defer close(timer.fireChan)
		defer timer.timer.Stop()
	loop:
		for {
			select {
			case <-timer.timer.C:
				select {
				// when timer fires, send a signal to channel
				case timer.fireChan <- struct{}{}:
				default:
					// ignore if caller is not able to consume the previous signal
					logger.Warn("timer.fireChan is full when sending signal")
				}

			case <-timer.closeChan:
				// closed; cleanup and quit
				break loop
			}
		}
	}()

	return timer
}

func (tg *LocalTimerGateImpl) FireChan() <-chan struct{} {
	return tg.fireChan
}

func (tg *LocalTimerGateImpl) FireAfter(checkTime time.Time) bool {
	return tg.nextWakeupTime.After(checkTime)
}

func (tg *LocalTimerGateImpl) Update(nextTime time.Time) bool {
	// NOTE: negative duration will make the timer fire immediately
	now := tg.timeSource.Now()

	if tg.timer.Stop() && tg.nextWakeupTime.Before(nextTime) {
		// this means the timer, before stopped, is active && next nextWakeupTime do not need to be updated
		// So reset it back to the previous wakeup time
		tg.timer.Reset(tg.nextWakeupTime.Sub(now))
		return false
	}

	// this means the timer, before stopped, is active && nextWakeupTime needs to be updated
	// or this means the timer, before stopped, is already fired / never active (when the tg.timer.Stop() returns false)
	tg.nextWakeupTime = nextTime
	tg.timer.Reset(nextTime.Sub(now))
	// Notifies caller that next notification is reset to fire at passed in 'next' visibility time
	return true
}

// Close shutdown the timer
func (tg *LocalTimerGateImpl) Close() {
	close(tg.closeChan)
}
