package gotimerwheel

import (
	"testing"
	"time"
)

func TestCreate(t *testing.T) {
	then := time.Unix(0, 0)
	tw := NewTimerWheel(then, 1)
	assertNowLength(t, tw, then, 0)
}

func TestAdvance(t *testing.T) {
	start := time.Unix(0, 0)
	tw := NewTimerWheel(start, 1)
	// no advance should not error
	count := tw.AdvanceTo(start, 0)
	assertNowLength(t, tw, start, 0)
	ten := time.Unix(0, 10)
	count += tw.AdvanceTo(ten, 0)
	assertNowLength(t, tw, ten, 0)
	// reverse time should not error or change time
	count += tw.AdvanceTo(start, 0)
	assertNowLength(t, tw, ten, 0)
	if count != 0 {
		t.Errorf("Expected zero events scheduled, but found %v", count)
	}
}

func TestSchedule(t *testing.T) {
	start := time.Unix(0, 10)
	tw := NewTimerWheel(start, 5)
	assertNowLength(t, tw, start, 0)
	// schedule at the current time should add
	tw.ScheduleEventAt(start, nil)
	assertNowLength(t, tw, start, 1)
	// schedule multiple at the current time
	tw.ScheduleEventAt(start, nil)
	tw.ScheduleEventAt(start, nil)
	assertNowLength(t, tw, start, 3)
	// schedule in future
	for idx := 0; idx < 1000; idx++ {
		future := time.Unix(0, 10+int64(idx))
		tw.ScheduleEventAt(future, nil)
		assertNowLength(t, tw, start, 4+idx)
	}
}

type e struct {
	time.Time
	Event
}

type callbackRun struct {
	*testing.T
	*TimerWheel
	start           time.Time
	end             time.Time
	events          []e
	execCount       int
	targetExecCount int
}

func newCallbackRun(t *testing.T, start, end time.Time, bucketSize time.Duration, events ...e) *callbackRun {
	return &callbackRun{
		T:               t,
		TimerWheel:      NewTimerWheel(start, bucketSize),
		start:           start,
		end:             end,
		execCount:       0,
		events:          events,
		targetExecCount: len(events),
	}
}

func (cbr *callbackRun) scheduleEvents() {
	for _, event := range cbr.events {
		cbr.ScheduleEventAt(event.Time, event.Event)
	}
	cbr.Log(cbr.TimerWheel)
}

func (cbr *callbackRun) assertExecCount(validValues ...int) {
	for _, value := range validValues {
		if value == cbr.execCount {
			cbr.execCount++
			return
		}
	}
	cbr.Errorf("Wrong exec count. Found %v. Acceptable: %v", cbr.execCount, validValues)
}

func createBasicRun(t *testing.T) (run *callbackRun) {
	run = newCallbackRun(t, time.Unix(0, 10), time.Unix(0, 400), 5,
		// things scheduled at the start do get invoked
		e{time.Unix(0, 10), func(*time.Time) { run.assertExecCount(0) }},
		// multiple events at the same time, we don't know what order they'll be invoked in
		e{time.Unix(0, 20), func(*time.Time) { run.assertExecCount(6, 7) }},
		e{time.Unix(0, 20), func(*time.Time) { run.assertExecCount(6, 7) }},
		// perfectly safe to schedule things out of order
		e{time.Unix(0, 15), func(*time.Time) { run.assertExecCount(4) }},
		e{time.Unix(0, 14), func(*time.Time) { run.assertExecCount(2, 3) }},
		// schedule some into a next timerWheel
		e{time.Unix(0, 170), func(*time.Time) { run.assertExecCount(8, 9) }},
		e{time.Unix(0, 171), func(*time.Time) { run.assertExecCount(10) }},
		e{time.Unix(0, 330), func(*time.Time) { run.assertExecCount(11) }},
		e{time.Unix(0, 331), func(*time.Time) { run.assertExecCount(12, 13) }},
		e{time.Unix(0, 170), func(*time.Time) { run.assertExecCount(8, 9) }},
		e{time.Unix(0, 331), func(*time.Time) { run.assertExecCount(12, 13) }},
		// still perfectly safe to schedule things out of order
		e{time.Unix(0, 13), func(*time.Time) { run.assertExecCount(1) }},
		e{time.Unix(0, 16), func(*time.Time) { run.assertExecCount(5) }},
		e{time.Unix(0, 14), func(*time.Time) { run.assertExecCount(2, 3) }},
		// things schedule in the past should never get invoked
		e{time.Unix(0, 9), func(*time.Time) { t.Error("Event should never have been invoked") }},
	)
	run.targetExecCount-- // last one shouldn't be invoked
	run.scheduleEvents()
	return
}

func TestExecNoLimit(t *testing.T) {
	run := createBasicRun(t)
	if count := run.AdvanceTo(run.end, 0); count != run.targetExecCount {
		t.Errorf("Expected %v callbacks to be invoked, but got %v", run.targetExecCount, count)
	}
	assertNowLength(t, run.TimerWheel, run.end, 0)
	run.assertExecCount(run.targetExecCount)
}

func TestExecLimited(t *testing.T) {
	batchSize := 2
	run := createBasicRun(t)
	totalCount := 0
	for count := run.AdvanceTo(run.end, batchSize); count > 0; count = run.AdvanceTo(run.end, batchSize) {
		if count > batchSize {
			t.Errorf("Expected batch no greater than %v, but got %v", batchSize, count)
		}
		totalCount += count
	}
	if totalCount != run.targetExecCount {
		t.Errorf("Expected %v callbacks to be invoked, but got %v", run.targetExecCount, totalCount)
	}
	assertNowLength(t, run.TimerWheel, run.end, 0)
	run.assertExecCount(run.targetExecCount)
}

func assertNowLength(t *testing.T, tw *TimerWheel, then time.Time, length int) {
	if now := tw.Now(); !now.Equal(then) {
		t.Errorf("Not equal now: %v vs %v", then, now)
	}
	if l := tw.Length(); l != length {
		t.Errorf("Not equal length: %v vs %v", l, length)
	}
	if length == 0 && !tw.IsEmpty() {
		t.Error("Expected empty")
	} else if length > 0 && tw.IsEmpty() {
		t.Error("Expected not empty")
	}
}
