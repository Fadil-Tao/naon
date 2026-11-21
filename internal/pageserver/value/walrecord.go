package value

import "fmt"

// RecordKind identifies a WAL record type. The numbers are part of the
// on-disk format: never renumber them.
type RecordKind uint8

const (
	KindPostgres               RecordKind = 1
	KindClearVisibilityMap     RecordKind = 2
	KindClogSetCommitted       RecordKind = 3
	KindClogSetAborted         RecordKind = 4
	KindMultixactOffsetCreate  RecordKind = 5
	KindMultixactMembersCreate RecordKind = 6
	KindTest                   RecordKind = 7
)

func (k RecordKind) String() string {
	switch k {
	case KindPostgres:
		return "Postgres"
	case KindClearVisibilityMap:
		return "ClearVisibilityMapFlags"
	case KindClogSetCommitted:
		return "ClogSetCommitted"
	case KindClogSetAborted:
		return "ClogSetAborted"
	case KindMultixactOffsetCreate:
		return "MultixactOffsetCreate"
	case KindMultixactMembersCreate:
		return "MultixactMembersCreate"
	case KindTest:
		return "Test"
	default:
		return fmt.Sprintf("RecordKind(%d)", uint8(k))
	}
}

// WalRecord is a change that must be replayed to produce a page.
// Mirrors Neon's NeonWalRecord (libs/wal_decoder/src/models/record.rs).
type WalRecord interface {
	Kind() RecordKind
	// WillInit reports whether replaying this record needs no older page
	// version. Only Postgres and Test records can set it.
	WillInit() bool
	encodeBody(e *encoder)
}

// OptU32 is an optional uint32 (Rust's Option<u32>).
type OptU32 struct {
	Valid bool
	V     uint32
}

// PostgresRecord is a native Postgres WAL record, replayed by the walredo process.
type PostgresRecord struct {
	Init bool   // will_init
	Rec  []byte // raw record bytes
}

func (r PostgresRecord) Kind() RecordKind { return KindPostgres }
func (r PostgresRecord) WillInit() bool   { return r.Init }
func (r PostgresRecord) encodeBody(e *encoder) {
	e.bytes(r.Rec)
}

// ClearVisibilityMapFlags clears bits in the heap visibility map.
type ClearVisibilityMapFlags struct {
	NewHeapBlkno OptU32
	OldHeapBlkno OptU32
	Flags        uint8
}

func (r ClearVisibilityMapFlags) Kind() RecordKind { return KindClearVisibilityMap }
func (r ClearVisibilityMapFlags) WillInit() bool   { return false }
func (r ClearVisibilityMapFlags) encodeBody(e *encoder) {
	e.optU32(r.NewHeapBlkno)
	e.optU32(r.OldHeapBlkno)
	e.u8(r.Flags)
}

// ClogSetCommitted marks transaction IDs as committed on a CLOG page.
type ClogSetCommitted struct {
	Xids      []uint32
	Timestamp int64 // Postgres TimestampTz
}

func (r ClogSetCommitted) Kind() RecordKind { return KindClogSetCommitted }
func (r ClogSetCommitted) WillInit() bool   { return false }
func (r ClogSetCommitted) encodeBody(e *encoder) {
	e.u32s(r.Xids)
	e.u64(uint64(r.Timestamp))
}

// ClogSetAborted marks transaction IDs as aborted on a CLOG page.
type ClogSetAborted struct {
	Xids []uint32
}

func (r ClogSetAborted) Kind() RecordKind { return KindClogSetAborted }
func (r ClogSetAborted) WillInit() bool   { return false }
func (r ClogSetAborted) encodeBody(e *encoder) {
	e.u32s(r.Xids)
}

// MultixactOffsetCreate extends the multixact offsets SLRU.
type MultixactOffsetCreate struct {
	Mid  uint32
	Moff uint32
}

func (r MultixactOffsetCreate) Kind() RecordKind { return KindMultixactOffsetCreate }
func (r MultixactOffsetCreate) WillInit() bool   { return false }
func (r MultixactOffsetCreate) encodeBody(e *encoder) {
	e.u32(r.Mid)
	e.u32(r.Moff)
}

// MultiXactMember is one member of a multixact.
type MultiXactMember struct {
	Xid    uint32
	Status uint32
}

// MultixactMembersCreate extends the multixact members SLRU.
type MultixactMembersCreate struct {
	Moff    uint32
	Members []MultiXactMember
}

func (r MultixactMembersCreate) Kind() RecordKind { return KindMultixactMembersCreate }
func (r MultixactMembersCreate) WillInit() bool   { return false }
func (r MultixactMembersCreate) encodeBody(e *encoder) {
	e.u32(r.Moff)
	e.u32(uint32(len(r.Members)))
	for _, m := range r.Members {
		e.u32(m.Xid)
		e.u32(m.Status)
	}
}

// TestRecord lets tests check finished pages without Postgres.
// Replay rule: if Clear or Init, start from an empty page; then append Append.
type TestRecord struct {
	Append string
	Clear  bool
	Init   bool // will_init; Clear should also be true when this is set
}

func (r TestRecord) Kind() RecordKind { return KindTest }
func (r TestRecord) WillInit() bool   { return r.Init }
func (r TestRecord) encodeBody(e *encoder) {
	e.bytes([]byte(r.Append))
	e.bool(r.Clear)
}

// TestAppend is a test record that appends s to the page.
func TestAppend(s string) TestRecord { return TestRecord{Append: s} }

// TestInit is a test record that starts a fresh page containing s.
func TestInit(s string) TestRecord { return TestRecord{Append: s, Clear: true, Init: true} }

// decodeRecord decodes a record body for the given kind. willInit comes from
// the fixed-position flag byte in the Value header.
func decodeRecord(kind RecordKind, willInit bool, d *decoder) (WalRecord, error) {
	var rec WalRecord
	switch kind {
	case KindPostgres:
		rec = PostgresRecord{Init: willInit, Rec: d.bytes()}
	case KindClearVisibilityMap:
		rec = ClearVisibilityMapFlags{NewHeapBlkno: d.optU32(), OldHeapBlkno: d.optU32(), Flags: d.u8()}
	case KindClogSetCommitted:
		xids := d.u32s()
		rec = ClogSetCommitted{Xids: xids, Timestamp: int64(d.u64())}
	case KindClogSetAborted:
		rec = ClogSetAborted{Xids: d.u32s()}
	case KindMultixactOffsetCreate:
		rec = MultixactOffsetCreate{Mid: d.u32(), Moff: d.u32()}
	case KindMultixactMembersCreate:
		moff := d.u32()
		n := d.u32()
		if !d.need(int(n) * 8) {
			return nil, d.err
		}
		members := make([]MultiXactMember, n)
		for i := range members {
			members[i] = MultiXactMember{Xid: d.u32(), Status: d.u32()}
		}
		rec = MultixactMembersCreate{Moff: moff, Members: members}
	case KindTest:
		app := d.bytes()
		rec = TestRecord{Append: string(app), Clear: d.bool(), Init: willInit}
	default:
		return nil, fmt.Errorf("%w: unknown record kind %d", ErrCorrupt, kind)
	}
	if err := d.finish(); err != nil {
		return nil, err
	}
	if rec.WillInit() != willInit {
		return nil, fmt.Errorf("%w: kind %s cannot have will_init=%v", ErrCorrupt, kind, willInit)
	}
	return rec, nil
}