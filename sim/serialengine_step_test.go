package sim

import (
	"errors"
	"math"
	"reflect"
	"testing"
	"time"
)

type stepTestHandler func(Event) error

func (f stepTestHandler) Handle(evt Event) error { return f(evt) }

type stepTestHook struct{ fn func(HookCtx) }

func (h *stepTestHook) Func(ctx HookCtx) { h.fn(ctx) }

func stepTestEvent(name string, at VTimeInSec, secondary bool, handler Handler) Event {
	evt := NewEventBase(at, handler)
	evt.ID, evt.secondary = name, secondary
	return evt
}

type stepTestRecord struct {
	stage, id string
	at        VTimeInSec
}

func TestSerialEngineStepEmpty(t *testing.T) {
	e := NewSerialEngine()
	if next, present, err := e.PeekNextTime(); next != 0 || present || err != nil {
		t.Fatalf("empty peek: time=%v present=%v err=%v", next, present, err)
	}
	if processed, err := e.StepAt(0); processed || err != nil {
		t.Fatalf("empty step: processed=%v err=%v", processed, err)
	}
	if e.CurrentTime() != 0 {
		t.Fatal("empty step advanced time")
	}
}

func runStepTrace(t *testing.T, stepped bool) []stepTestRecord {
	t.Helper()
	e := NewSerialEngine()
	var trace []stepTestRecord
	record := func(stage string, evt Event) {
		trace = append(trace, stepTestRecord{stage, evt.(*EventBase).ID, e.CurrentTime()})
	}
	var handler stepTestHandler
	handler = func(evt Event) error {
		record("handle", evt)
		if evt.(*EventBase).ID == "first" {
			e.Schedule(stepTestEvent("spawned", 1, false, handler))
		}
		return nil
	}
	e.AcceptHook(&stepTestHook{func(ctx HookCtx) { record(ctx.Pos.Name, ctx.Item.(Event)) }})
	e.Schedule(stepTestEvent("secondary", 1, true, handler))
	e.Schedule(stepTestEvent("first", 1, false, handler))
	e.Schedule(stepTestEvent("peer", 1, false, handler))
	e.Schedule(stepTestEvent("earlier-secondary", 0.5, true, handler))
	e.Schedule(stepTestEvent("last", 2, false, handler))
	if !stepped {
		if err := e.Run(); err != nil {
			t.Fatal(err)
		}
		return trace
	}
	for count := 0; count <= 6; count++ {
		before, now := len(trace), e.CurrentTime()
		next, present, err := e.PeekNextTime()
		if err != nil {
			t.Fatal(err)
		}
		for repeat := 0; repeat < 3; repeat++ {
			again, exists, err := e.PeekNextTime()
			if err != nil || again != next || exists != present || len(trace) != before || e.CurrentTime() != now {
				t.Fatal("peek changed engine state")
			}
		}
		if !present {
			if count != 6 || now != 2 {
				t.Fatalf("incomplete event sequence: count=%d time=%v", count, now)
			}
			return trace
		}
		if done, err := e.StepAt(next); !done || err != nil || len(trace) != before+3 {
			t.Fatalf("step must execute one before/handler/after triple: %v %v", done, err)
		}
	}
	t.Fatal("step failed to drain")
	return nil
}

func TestSerialEngineStepMatchesRun(t *testing.T) {
	legacy, stepped := runStepTrace(t, false), runStepTrace(t, true)
	if !reflect.DeepEqual(legacy, stepped) {
		t.Fatalf("Run/Step traces differ:\nRun=%v\nStep=%v", legacy, stepped)
	}
	if stepped[13].id != "secondary" || stepped[16].id != "last" {
		t.Fatalf("primary events must precede same-time secondary: %v", stepped)
	}
}

func TestSerialEngineStepRejectsExpectedTimeWithoutMutation(t *testing.T) {
	bad := []VTimeInSec{-1, 0.5, 1, 3, VTimeInSec(math.Nextafter(2, 3)),
		VTimeInSec(math.NaN()), VTimeInSec(math.Inf(1)), VTimeInSec(math.Inf(-1))}
	for _, expected := range bad {
		e := NewSerialEngine()
		e.writeNow(1)
		calls := 0
		e.Schedule(stepTestEvent("pending", 2, false, stepTestHandler(func(Event) error { calls++; return nil })))
		if done, err := e.StepAt(expected); done || err == nil {
			t.Fatalf("accepted invalid expected time %v", expected)
		}
		if calls != 0 || e.CurrentTime() != 1 || e.queue.Len() != 1 {
			t.Fatalf("invalid expected time %v mutated the engine", expected)
		}
		if next, present, err := e.PeekNextTime(); err != nil || !present || next != 2 {
			t.Fatalf("failed step changed next event or retained locks: %v %v %v", next, present, err)
		}
	}
	e := NewSerialEngine()
	e.writeNow(2)
	if done, err := e.StepAt(1); done || err == nil {
		t.Fatal("empty engine accepted past expected time")
	}
	if done, err := e.StepAt(100); done || err != nil || e.CurrentTime() != 2 {
		t.Fatal("empty future grant advanced time or failed")
	}
}

func TestSerialEngineStepRejectsInvalidQueueHeads(t *testing.T) {
	for _, bad := range []VTimeInSec{-1, 0.5, VTimeInSec(math.NaN()), VTimeInSec(math.Inf(1))} {
		for _, secondary := range []bool{false, true} {
			e := NewSerialEngine()
			e.writeNow(1)
			handler := stepTestHandler(func(Event) error { t.Fatal("invalid queue was consumed"); return nil })
			// Inject invalid heads directly because legacy Schedule rejects past
			// times but accepts NaN/Inf. Neither queue may hide an invalid head.
			e.queue.Push(stepTestEvent("primary", 2, false, handler))
			e.secondaryQueue.Push(stepTestEvent("secondary", 2, true, handler))
			q := e.queue
			if secondary {
				q = e.secondaryQueue
			}
			q.Pop()
			q.Push(stepTestEvent("invalid", bad, secondary, handler))
			if _, present, err := e.PeekNextTime(); present || err == nil {
				t.Fatalf("peek accepted head %v secondary=%v", bad, secondary)
			}
			if done, err := e.StepAt(2); done || err == nil {
				t.Fatalf("step accepted head %v secondary=%v", bad, secondary)
			}
			if e.queue.Len() != 1 || e.secondaryQueue.Len() != 1 || e.CurrentTime() != 1 {
				t.Fatal("invalid head changed queue or time")
			}
		}
	}
	for _, now := range []VTimeInSec{-1, VTimeInSec(math.NaN()), VTimeInSec(math.Inf(1))} {
		e := NewSerialEngine()
		e.writeNow(now)
		if _, present, err := e.PeekNextTime(); present || err == nil {
			t.Fatal("peek accepted invalid current time")
		}
		if done, err := e.StepAt(0); done || err == nil {
			t.Fatal("step accepted invalid current time")
		}
	}
}

func assertStepBusy(t *testing.T, e *SerialEngine) {
	t.Helper()
	if _, present, err := e.PeekNextTime(); present || err == nil {
		t.Fatal("peek did not report busy")
	}
	if done, err := e.StepAt(1); done || err == nil {
		t.Fatal("step did not report busy")
	}
}

func TestSerialEngineStepBusyAndReentry(t *testing.T) {
	e := NewSerialEngine()
	e.Pause()
	assertStepBusy(t, e)
	e.Continue()
	if _, _, err := e.PeekNextTime(); err != nil {
		t.Fatal("paused call leaked control lock", err)
	}
	for _, legacy := range []bool{false, true} {
		e = NewSerialEngine()
		callbacks := 0
		check := func() { callbacks++; assertStepBusy(t, e) }
		e.AcceptHook(&stepTestHook{func(HookCtx) { check() }})
		e.Schedule(stepTestEvent("reentry", 1, false, stepTestHandler(func(Event) error { check(); return nil })))
		if legacy {
			if err := e.Run(); err != nil {
				t.Fatal(err)
			}
		} else if _, err := e.StepAt(1); err != nil {
			t.Fatal(err)
		}
		if callbacks != 3 {
			t.Fatalf("missing callback reentry coverage: %d", callbacks)
		}
	}
}

func TestSerialEngineStepRejectsConcurrentRun(t *testing.T) {
	e := NewSerialEngine()
	entered, release, ended := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	e.Schedule(stepTestEvent("held", 1, false, stepTestHandler(func(Event) error {
		close(entered)
		<-release
		return nil
	})))
	go func() { ended <- e.Run() }()
	defer func() {
		close(release)
		select {
		case err := <-ended:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(2 * time.Second):
			t.Error("Run did not release after test")
		}
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not start")
	}
	assertStepBusy(t, e)
}

func TestSerialEngineStepHandlerError(t *testing.T) {
	want := errors.New("handler failure")
	for _, legacy := range []bool{false, true} {
		e := NewSerialEngine()
		after := false
		e.AcceptHook(&stepTestHook{func(ctx HookCtx) { after = ctx.Pos == HookPosAfterEvent }})
		e.Schedule(stepTestEvent("failure", 1, false, stepTestHandler(func(Event) error { return want })))
		if legacy {
			if err := e.Run(); err != nil {
				t.Fatalf("legacy Run changed error behavior: %v", err)
			}
		} else if done, err := e.StepAt(1); !done || !errors.Is(err, want) {
			t.Fatalf("handler error lost processed status: %v %v", done, err)
		}
		if !after || e.CurrentTime() != 1 || !e.noMoreEvent() {
			t.Fatal("handler error changed after-hook, time, or dequeue behavior")
		}
	}
}

func TestSerialEngineStepPanicReleasesControl(t *testing.T) {
	for _, site := range []string{"BeforeEvent", "handle", "AfterEvent"} {
		e := NewSerialEngine()
		e.AcceptHook(&stepTestHook{func(ctx HookCtx) {
			if ctx.Pos.Name == site {
				panic(site)
			}
		}})
		e.Schedule(stepTestEvent("panic", 1, false, stepTestHandler(func(Event) error {
			if site == "handle" {
				panic(site)
			}
			return nil
		})))
		func() {
			defer func() {
				if got := recover(); got != site {
					t.Errorf("panic=%v want=%s", got, site)
				}
			}()
			_, _ = e.StepAt(1)
		}()
		// Check lock cleanup, not whether a partially mutated model can recover.
		if !e.singleRunLock.TryLock() {
			t.Fatal("panic leaked run lock")
		}
		e.singleRunLock.Unlock()
		if !e.pauseLock.TryLock() {
			t.Fatal("panic leaked pause lock")
		}
		e.pauseLock.Unlock()
	}
}
