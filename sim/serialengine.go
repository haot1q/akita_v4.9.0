package sim

import (
	"fmt"
	"log"
	"math"
	"reflect"
	"sync"
)

// A SerialEngine is an Engine that always run events one after another.
type SerialEngine struct {
	HookableBase

	timeLock       sync.RWMutex
	time           VTimeInSec
	queue          EventQueue
	secondaryQueue EventQueue

	isPaused     bool
	isPausedLock sync.Mutex
	pauseLock    sync.Mutex

	singleRunLock sync.Mutex
}

// NewSerialEngine creates a SerialEngine
func NewSerialEngine() *SerialEngine {
	e := new(SerialEngine)

	e.queue = NewEventQueue()
	e.secondaryQueue = NewEventQueue()
	//e.queue = NewInsertionQueue()

	return e
}

// Schedule register an event to be happen in the future
func (e *SerialEngine) Schedule(evt Event) {
	now := e.readNow()
	if evt.Time() < now {
		log.Panic("scheduling an event earlier than current time")
	}

	if evt.IsSecondary() {
		e.secondaryQueue.Push(evt)

		return
	}

	e.queue.Push(evt)
}

func (e *SerialEngine) readNow() VTimeInSec {
	e.timeLock.RLock()
	t := e.time
	e.timeLock.RUnlock()

	return t
}

func (e *SerialEngine) writeNow(t VTimeInSec) {
	e.timeLock.Lock()
	e.time = t
	e.timeLock.Unlock()
}

// Run processes all the events scheduled in the SerialEngine
func (e *SerialEngine) Run() error {
	e.singleRunLock.Lock()
	defer e.singleRunLock.Unlock()

	for {
		if e.noMoreEvent() {
			return nil
		}

		e.pauseLock.Lock()

		_ = e.handleEvent(e.nextEvent())

		e.pauseLock.Unlock()
	}
}

// PeekNextTime returns the earliest native event time without advancing time,
// removing an event, or invoking hooks. An empty engine returns present=false.
// It fails without blocking while Run, another step operation, or Pause owns
// control. External Schedule calls must occur between these operations; arbitrary
// concurrent injection is not supported. Only these new methods are reentrant
// from a handler or hook (they return an error); Run and Pause are not.
func (e *SerialEngine) PeekNextTime() (next VTimeInSec, present bool, err error) {
	if err = e.acquireStepControl(); err != nil {
		return 0, false, err
	}
	defer e.singleRunLock.Unlock()
	defer e.pauseLock.Unlock()

	return e.inspectNextTime()
}

// StepAt handles exactly one earliest event if its native time equals expected.
// Use the unrounded value returned by PeekNextTime. The safe-point requirements
// and busy behavior are the same as PeekNextTime. Invalid or mismatched times
// fail before dequeueing; an empty engine returns processed=false without moving
// time. Existing primary/secondary and within-queue ordering is preserved.
//
// A handler error is returned with processed=true after the after-event hooks.
// Panics propagate, with the step-control locks released. Either failure may
// leave component side effects and does not imply that the model can recover.
// Run retains its existing behavior of ignoring handler errors.
func (e *SerialEngine) StepAt(expected VTimeInSec) (processed bool, err error) {
	if err = e.acquireStepControl(); err != nil {
		return false, err
	}
	defer e.singleRunLock.Unlock()
	defer e.pauseLock.Unlock()

	if err = validateStepTime(expected, e.readNow()); err != nil {
		return false, err
	}
	next, present, err := e.inspectNextTime()
	if err != nil || !present {
		return false, err
	}
	if next != expected {
		return false, fmt.Errorf("next event time %v does not match expected %v", next, expected)
	}
	return true, e.handleEvent(e.nextEvent())
}

func (e *SerialEngine) acquireStepControl() error {
	if !e.singleRunLock.TryLock() {
		return fmt.Errorf("engine is busy running or stepping")
	}
	if !e.pauseLock.TryLock() {
		e.singleRunLock.Unlock()
		return fmt.Errorf("engine is paused")
	}
	return nil
}

func validateStepTime(t, now VTimeInSec) error {
	if math.IsNaN(float64(t)) || math.IsInf(float64(t), 0) || t < 0 || t < now {
		return fmt.Errorf("invalid step time %v at current time %v", t, now)
	}
	return nil
}

func (e *SerialEngine) inspectNextTime() (VTimeInSec, bool, error) {
	now := e.readNow()
	if err := validateStepTime(now, 0); err != nil {
		return 0, false, err
	}
	// Validate both visible candidates before comparing them: NaN in either
	// queue must not silently select an apparently valid event in the other.
	for _, q := range []EventQueue{e.queue, e.secondaryQueue} {
		if q.Len() > 0 {
			if err := validateStepTime(q.Peek().Time(), now); err != nil {
				return 0, false, err
			}
		}
	}
	q := e.nextQueue()
	if q == nil {
		return 0, false, nil
	}
	return q.Peek().Time(), true, nil
}

func (e *SerialEngine) handleEvent(evt Event) error {
	now := e.readNow()
	if evt.Time() < now {
		log.Panicf(
			"cannot run event in the past, evt %s @ %.10f, now %.10f",
			reflect.TypeOf(evt), evt.Time(), now,
		)
	}
	e.writeNow(evt.Time())
	hookCtx := HookCtx{Domain: e, Pos: HookPosBeforeEvent, Item: evt}
	e.InvokeHook(hookCtx)
	err := evt.Handler().Handle(evt)
	hookCtx.Pos = HookPosAfterEvent
	e.InvokeHook(hookCtx)
	return err
}

func (e *SerialEngine) noMoreEvent() bool {
	return e.queue.Len() == 0 && e.secondaryQueue.Len() == 0
}

func (e *SerialEngine) nextEvent() Event {
	return e.nextQueue().Pop()
}

func (e *SerialEngine) nextQueue() EventQueue {
	if e.noMoreEvent() {
		return nil
	}
	if e.queue.Len() == 0 {
		return e.secondaryQueue
	}

	if e.secondaryQueue.Len() == 0 {
		return e.queue
	}

	primaryEvt := e.queue.Peek()
	secondaryEvt := e.secondaryQueue.Peek()

	if primaryEvt.Time() <= secondaryEvt.Time() {
		return e.queue
	}

	return e.secondaryQueue
}

// Pause prevents the SerialEngine to trigger more events.
func (e *SerialEngine) Pause() {
	e.isPausedLock.Lock()
	defer e.isPausedLock.Unlock()

	if e.isPaused {
		return
	}

	e.pauseLock.Lock()
	e.isPaused = true
}

// Continue allows the SerialEngine to trigger more events.
func (e *SerialEngine) Continue() {
	e.isPausedLock.Lock()
	defer e.isPausedLock.Unlock()

	if !e.isPaused {
		return
	}

	e.pauseLock.Unlock()
	e.isPaused = false
}

// CurrentTime returns the current time at which the engine is at.
// Specifically, the run time of the current event.
func (e *SerialEngine) CurrentTime() VTimeInSec {
	return e.readNow()
}
