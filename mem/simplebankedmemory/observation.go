package simplebankedmemory

import (
	"github.com/sarchlab/akita/v4/mem/mem"
	"github.com/sarchlab/akita/v4/sim"
)

// HookPosReadSampled observes the one-time storage snapshot used by a read.
// Response enqueue may happen later when the response port becomes available.
var HookPosReadSampled = &sim.HookPos{Name: "SimpleBankedMemoryReadSampled"}

// HookPosWriteCommitted observes a successful one-time storage write, before
// response enqueue. DirtyMask describes enabled bytes, not partial failures.
var HookPosWriteCommitted = &sim.HookPos{Name: "SimpleBankedMemoryWriteCommitted"}

// StorageAccess contains observation-owned copies. Address remains the external
// request address; LocalAddress is the address used by Storage. Data is the read
// snapshot or write payload. A nil DirtyMask means all write bytes are enabled.
// Hooks must not schedule work or modify the component, request or storage.
type StorageAccess struct {
	RequestID    string
	Address      uint64
	LocalAddress uint64
	Data         []byte
	DirtyMask    []bool
}

func (m *middleware) observeStorageAccess(
	position *sim.HookPos, req mem.AccessReq, local uint64, data []byte,
) {
	if m.NumHooks() == 0 {
		return
	}
	for _, hook := range m.Hooks() {
		access := StorageAccess{
			RequestID: req.Meta().ID, Address: req.GetAddress(), LocalAddress: local,
			Data: append([]byte(nil), data...),
		}
		if write, ok := req.(*mem.WriteReq); ok && write.DirtyMask != nil {
			access.DirtyMask = append([]bool{}, write.DirtyMask...)
		}
		hook.Func(sim.HookCtx{Domain: m.Comp, Pos: position, Item: access})
	}
}
