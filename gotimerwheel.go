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

// Events that you wish to be invoked when their time comes. The
// argument they are provided with is the time argument passed to
// AdvanceTo (or AdvanceBy plus the current Timer Wheel time).
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

// Create a new Timer Wheel. The Timer Wheel considers the current
// time to be the value of startAt. BucketSize should be chosen so
// that you normally have no more than around 100 events within a
// bucketSize-duration.
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

// Returns the Timer Wheel's current time.
func (tw *TimerWheel) Now() time.Time {
	return tw.now
}

// Returns the number of scheduled events in the Timer Wheel.
func (tw *TimerWheel) Length() int {
	if tw == nil {
		return 0
	}
	count := 0
	for _, enContainer := range tw.ring[tw.ringIdx:] {
		count += enContainer.length()
	}
	return count + tw.next.Length()
}

// O(1) test on Timer Wheel having scheduled events
func (tw *TimerWheel) IsEmpty() bool {
	if tw.next != nil {
		return false
	}
	for _, enContainer := range tw.ring[tw.ringIdx:] {
		if enContainer.eventNode != nil {
			return false
		}
	}
	return true
}

// Schedules an event to be invoked at the indicated time. If that
// time is in the past of the Timer Wheel's current time then the
// ScheduledInPast error is returned. The event is never invoked at
// this point, even if the event is scheduled for the exact same time
// as the Timer Wheel's current time (though it is enqueued).
func (tw *TimerWheel) ScheduleEventAt(at time.Time, e Event) error {
	if at.Before(tw.now) {
		return ScheduledInPast
	}
	idx := int((at.Sub(tw.start)) / tw.bucketSize)
	if idx >= ringLength {
		tw.ensureNext()
		tw.next.scheduleEventAt(at, e)
	} else {
		event := &eventNode{at: &at, fun: e}
		enContainer := &(tw.ring[idx])
		enContainer.addEvent(event)
	}
	return nil
}

// Schedules an event to be invoked at the current Timer Wheel's time
// plus the supplied duration.
func (tw *TimerWheel) ScheduleEventIn(in time.Duration, e Event) error {
	return tw.ScheduleEventAt(tw.now.Add(in), e)
}

func (tw *TimerWheel) scheduleEventAt(at time.Time, e Event) {
	idx := int((at.Sub(tw.start)) / tw.bucketSize)
	if idx >= ringLength {
		tw.ensureNext()
		tw.next.scheduleEventAt(at, e)
	} else {
		// We don't care about sorting for non-root timer wheels, so
		// this gets inserted right at the head, to keep it O(1).
		enContainer := &(tw.ring[idx])
		enContainer.eventNode = &eventNode{
			at:   &at,
			fun:  e,
			next: eventNodeContainer{eventNode: enContainer.eventNode},
		}
	}
}

// Advances the Timer Wheel's current time to the indicated time. Any
// event scheduled before (or including) the indicated time is
// invoked. Note that events scheduled at the current time are
// invoked. E.g. if you schedule an event at 12pm, and then call
// AdvanceTo with 12pm then the event will be invoked. Limit allows
// you to control how many events are invoked: set to 0 to allow all
// necessary events to be invoked. If a positive limit is set then a
// maximum of limit events are invoked, at which point the Timer
// Wheel's current time is set to the time of the most recently
// invoked event. Returns the number of events invoked.
func (tw *TimerWheel) AdvanceTo(now time.Time, limit int) int {
	if now.Before(tw.now) {
		return 0
	}
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

// Advances the Timer Wheel's current time by the indicated
// amount. See AdvanceTo for the semantics of the limit parameter and
// returned value.
func (tw *TimerWheel) AdvanceBy(interval time.Duration, limit int) {
	tw.AdvanceTo(tw.now.Add(interval), limit)
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
		if next.IsEmpty() {
			tw.next = nil
		} else if next.ringIdx == ringLength {
			next.fetchFromNext()
		}
	}
}

func (tw *TimerWheel) addEvent(event *eventNode) {
	event.next.eventNode = nil
	idx := int(event.at.Sub(tw.start) / tw.bucketSize)
	enContainer := &(tw.ring[idx])
	enContainer.addEvent(event)
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
