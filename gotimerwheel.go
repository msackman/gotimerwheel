package gotimerwheel

import (
	"errors"
	"fmt"
	"time"
)

const (
	ringLength = 32
)

var (
	ScheduledInPast = errors.New("Requested event to be scheduled in the past")
)

type Event func(*time.Time)

type TimerWheel struct {
	ring       []eventNodeContainer
	ringIdx    int
	next       *TimerWheel
	now        time.Time
	start      time.Time
	bucketSize time.Duration
}

type eventNodeContainer struct{ *eventNode }

type eventNode struct {
	at   *time.Time
	fun  Event
	next eventNodeContainer
}

func NewTimerWheel(startAt time.Time, bucketSize time.Duration) *TimerWheel {
	if bucketSize <= 0 {
		panic("TimerWheel bucket size must be greater than 0")
	}
	return &TimerWheel{
		ring:       make([]eventNodeContainer, ringLength),
		bucketSize: bucketSize,
		now:        startAt,
		start:      startAt,
	}
}

func (tw *TimerWheel) ScheduleEventAt(at time.Time, e Event) error {
	if !tw.now.Before(at) {
		return ScheduledInPast
	}
	idx := int((at.Sub(tw.start)) / tw.bucketSize)
	if idx >= ringLength {
		tw.ensureNext()
		return tw.next.ScheduleEventAt(at, e)
	}
	event := &eventNode{at: &at, fun: e}
	enContainer := &(tw.ring[idx])
	enContainer.addEvent(event)
	return nil
}

func (tw *TimerWheel) ScheduleEventIn(in time.Duration, e Event) error {
	return tw.ScheduleEventAt(tw.now.Add(in), e)
}

func (tw *TimerWheel) addEvent(event *eventNode) {
	event.next.eventNode = nil
	idx := int(event.at.Sub(tw.start) / tw.bucketSize)
	enContainer := &(tw.ring[idx])
	enContainer.addEvent(event)
}

func (tw *TimerWheel) AdvanceTo(now time.Time, limit int) int {
	execCount := 0
	limited := limit > 0
	bucketStart := tw.start.Add(time.Duration(tw.ringIdx) * tw.bucketSize)
	for !now.Before(tw.now) {
		enContainer := &(tw.ring[tw.ringIdx])
		event := enContainer.eventNode
		for event != nil && !now.Before(*event.at) {
			if limited && limit == execCount {
				break
			}
			event.fun(&now)
			event = event.next.eventNode
			execCount++
		}
		enContainer.eventNode = event
		if event == nil {
			bucketStart = bucketStart.Add(tw.bucketSize)
			if !now.Before(bucketStart) {
				tw.ringIdx++
				tw.now = bucketStart
				if tw.ringIdx == ringLength {
					tw.fetchFromNext()
				}
				continue
			}
		} else if limited && limit == execCount {
			tw.now = *event.at
		}
		break
	}
	tw.now = now
	return execCount
}

func (tw *TimerWheel) AdvanceBy(interval time.Duration, limit int) {
	tw.AdvanceTo(tw.now.Add(interval), limit)
}

func (tw *TimerWheel) Now() time.Time {
	return tw.now
}

func (tw *TimerWheel) Length() int {
	if tw == nil {
		return 0
	}
	count := 0
	for _, enContainer := range tw.ring[tw.ringIdx:] {
		count += enContainer.length()
	}
	count += tw.next.Length()
	return count
}

func (tw *TimerWheel) ensureNext() {
	if tw.next == nil {
		ringWidth := time.Duration(tw.bucketSize * ringLength)
		tw.next = NewTimerWheel(tw.start.Add(ringWidth), ringWidth)
	}
}

func (tw *TimerWheel) fetchFromNext() {
	tw.ringIdx = 0
	tw.start = tw.start.Add(time.Duration(tw.bucketSize * ringLength))
	if next := tw.next; next != nil {
		enContainer := &(next.ring[next.ringIdx])
		event := enContainer.eventNode
		enContainer.eventNode = nil
		for event != nil {
			// We have to capture the next early because addEvent will
			// rewire event.next.
			next := event.next.eventNode
			tw.addEvent(event)
			event = next
		}
		next.ringIdx++
		next.now = next.now.Add(next.bucketSize)
		if next.Length() == 0 {
			tw.next = nil
		} else if next.ringIdx == ringLength {
			next.fetchFromNext()
		}
	}
}

func (tw *TimerWheel) String() string {
	return fmt.Sprintf("{TimerWheel start: %v, now: %v, bucketSize: %v, remainingEvents: %v, next: %v}",
		tw.start, tw.now, tw.bucketSize, tw.ring[tw.ringIdx:], tw.next)
}

func (enContainer *eventNodeContainer) addEvent(event *eventNode) {
	switch {
	case enContainer.eventNode == nil:
		enContainer.eventNode = event
	case event.at.Before(*enContainer.at) || event.at.Equal(*enContainer.at):
		enContainer.eventNode, event.next = event, *enContainer
	default:
		enContainer.next.addEvent(event)
	}
}

func (enContainer eventNodeContainer) length() int {
	if enContainer.eventNode == nil {
		return 0
	}
	return 1 + enContainer.eventNode.next.length()
}

func (enContainer eventNodeContainer) String() string {
	str := ""
	for event := enContainer.eventNode; event != nil; event = event.next.eventNode {
		str += fmt.Sprintf(", %s", event.String())
	}
	if len(str) == 0 {
		return "[]"
	} else {
		return fmt.Sprintf("[%s]", str[2:])
	}
}

func (e eventNode) String() string {
	return fmt.Sprintf("{at: %v, event: %v}", e.at, e.fun)
}
