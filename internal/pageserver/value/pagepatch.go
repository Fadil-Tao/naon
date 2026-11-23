package value

type PatchRange struct {
	Off  uint32
	Data []byte
}

type PagePatch struct {
	Ranges []PatchRange
}

func (r PagePatch) Kind() RecordKind { return KindPagePatch }
func (r PagePatch) WillInit() bool   { return false }
func (r PagePatch) encodeBody(e *encoder) {
	e.u32(uint32(len(r.Ranges)))
	for _, rg := range r.Ranges {
		e.u32(rg.Off)
		e.bytes(rg.Data)
	}
}

func decodePagePatch(d *decoder) WalRecord {
	n := d.u32()
	if !d.need(int(n) * 8) {
		return PagePatch{}
	}
	ranges := make([]PatchRange, n)
	for i := range ranges {
		ranges[i] = PatchRange{Off: d.u32(), Data: d.bytes()}
	}
	return PagePatch{Ranges: ranges}
}
