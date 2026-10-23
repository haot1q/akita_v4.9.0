package simplebankedmemory

import (
	"bytes"
	"testing"

	"github.com/sarchlab/akita/v4/mem/mem"
	"github.com/sarchlab/akita/v4/sim"
)

// Hold responses in the actual outgoing port until the test retrieves them.
type heldConnection struct{ sim.HookableBase }

func (*heldConnection) Name() string             { return "Held" }
func (*heldConnection) PlugIn(sim.Port)          {}
func (*heldConnection) Unplug(sim.Port)          {}
func (*heldConnection) NotifyAvailable(sim.Port) {}
func (*heldConnection) NotifySend()              {}

type storageHook struct {
	positions []*sim.HookPos
	accesses  []StorageAccess
	mutate    bool
}

func (h *storageHook) Func(ctx sim.HookCtx) {
	if ctx.Pos != HookPosReadSampled && ctx.Pos != HookPosWriteCommitted {
		return
	}
	access := ctx.Item.(StorageAccess)
	h.positions = append(h.positions, ctx.Pos)
	h.accesses = append(h.accesses, access)
	if h.mutate {
		access.Data[0] = 255
		if access.DirtyMask != nil {
			access.DirtyMask[0] = false
		}
	}
}

func heldMemory() (*middleware, *bank) {
	c := MakeBuilder().WithEngine(sim.NewSerialEngine()).WithNumBanks(1).
		WithTopPortBufferSize(1).Build("Memory")
	c.topPort.SetConnection(&heldConnection{})
	return &middleware{Comp: c}, &c.banks[0]
}

func occupyResponsePort(t *testing.T, m *middleware) {
	t.Helper()
	rsp := mem.WriteDoneRspBuilder{}.WithSrc(m.topPort.AsRemote()).
		WithDst("Requester").WithRspTo("other").Build()
	if err := m.topPort.Send(rsp); err != nil {
		t.Fatal(err)
	}
}

func TestStorageReadObservationOnceAndOwned(t *testing.T) {
	m, b := heldMemory()
	hook := &storageHook{mutate: true}
	m.AcceptHook(hook)
	second := &storageHook{}
	m.AcceptHook(second)
	if err := m.Storage.Write(0, []byte{1, 2, 3, 4}); err != nil {
		t.Fatal(err)
	}
	req := mem.ReadReqBuilder{}.WithSrc("Requester").WithAddress(0).
		WithByteSize(4).Build()
	item := &bankPipelineItem{req: req}
	b.postPipelineBuf.Push(item)
	occupyResponsePort(t, m)
	for i := 0; i < 4; i++ {
		if m.finalizeSingle(b) {
			t.Fatal("response unexpectedly escaped blocked port")
		}
	}
	if len(hook.accesses) != 1 || hook.positions[0] != HookPosReadSampled ||
		hook.accesses[0].RequestID != req.ID || !bytes.Equal(item.readData, []byte{1, 2, 3, 4}) {
		t.Fatal("read snapshot observed repeatedly or exposed mutable model data")
	}
	if len(second.accesses) != 1 || !bytes.Equal(second.accesses[0].Data, []byte{1, 2, 3, 4}) {
		t.Fatal("one observer changed another observer's snapshot")
	}
	if err := m.Storage.Write(0, []byte{9, 9, 9, 9}); err != nil {
		t.Fatal(err)
	}
	m.topPort.RetrieveOutgoing()
	if !m.finalizeSingle(b) {
		t.Fatal("unblocked response did not complete")
	}
	rsp := m.topPort.RetrieveOutgoing().(*mem.DataReadyRsp)
	if !bytes.Equal(rsp.Data, []byte{1, 2, 3, 4}) || len(hook.accesses) != 1 {
		t.Fatal("retry resampled storage or repeated observation")
	}
}

func TestStorageWriteObservationOnceMaskAndOwned(t *testing.T) {
	m, b := heldMemory()
	hook := &storageHook{mutate: true}
	m.AcceptHook(hook)
	second := &storageHook{}
	m.AcceptHook(second)
	if err := m.Storage.Write(0, []byte{10, 20, 30, 40}); err != nil {
		t.Fatal(err)
	}
	req := mem.WriteReqBuilder{}.WithSrc("Requester").WithAddress(0).
		WithData([]byte{1, 2, 3, 4}).Build()
	req.DirtyMask = []bool{true, false, true, false}
	b.postPipelineBuf.Push(&bankPipelineItem{req: req})
	occupyResponsePort(t, m)
	for i := 0; i < 4; i++ {
		if m.finalizeSingle(b) {
			t.Fatal("response unexpectedly escaped blocked port")
		}
	}
	actual, err := m.Storage.Read(0, 4)
	if err != nil || !bytes.Equal(actual, []byte{1, 20, 3, 40}) ||
		len(hook.accesses) != 1 || hook.positions[0] != HookPosWriteCommitted ||
		!bytes.Equal(req.Data, []byte{1, 2, 3, 4}) || !req.DirtyMask[0] {
		t.Fatal("write observation changed payload/mask or did not commit once")
	}
	if len(second.accesses) != 1 || !bytes.Equal(second.accesses[0].Data, []byte{1, 2, 3, 4}) ||
		!second.accesses[0].DirtyMask[0] {
		t.Fatal("one observer changed another observer's payload or mask")
	}
	// Prove retries cannot perform the write a second time.
	if err := m.Storage.Write(0, []byte{9, 9, 9, 9}); err != nil {
		t.Fatal(err)
	}
	m.topPort.RetrieveOutgoing()
	if !m.finalizeSingle(b) {
		t.Fatal("unblocked response did not complete")
	}
	actual, err = m.Storage.Read(0, 4)
	if err != nil || !bytes.Equal(actual, []byte{9, 9, 9, 9}) || len(hook.accesses) != 1 {
		t.Fatal("retry repeated write or observation")
	}
}

func TestStorageObservationAddressConversionAndFullWrite(t *testing.T) {
	m, b := heldMemory()
	m.AddressConverter = zeroConverter{}
	hook := &storageHook{}
	m.AcceptHook(hook)
	req := mem.WriteReqBuilder{}.WithSrc("Requester").WithAddress(4096).
		WithData([]byte{5, 6}).Build()
	b.postPipelineBuf.Push(&bankPipelineItem{req: req})
	if !m.finalizeSingle(b) || len(hook.accesses) != 1 {
		t.Fatal("write did not complete and produce one observation")
	}
	access := hook.accesses[0]
	actual, err := m.Storage.Read(0, 2)
	if err != nil || !bytes.Equal(actual, []byte{5, 6}) || access.Address != 4096 ||
		access.LocalAddress != 0 || access.DirtyMask != nil || !bytes.Equal(access.Data, []byte{5, 6}) {
		t.Fatal("converted address or full-write observation differs")
	}
}
