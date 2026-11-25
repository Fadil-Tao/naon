package key

import (
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"strconv"
)


// neon/lib/pageserver_api/src/key.rs
type Key struct {
	Field1 uint8 // eg 0x00
	Field2 uint32 // tablespaceID
	Field3 uint32 // databaseID 
	Field4 uint32 // Table file ID 
	Field5 uint8 // fork 
	Field6 uint32  // block number
}

const KeySize = 18 

const HexLen = KeySize * 2

const (
	MetadataPrefixBegin uint8 = 0x60 
	MetadataPrefixEnd   uint8 = 0x7F
	RelationSizePrefix  uint8 = 0x61
	AuxPrefix           uint8 = 0x62
	ReplOriginPrefix    uint8 = 0x63
	DbDirPrefix         uint8 = 0x64
	RelDirPrefix        uint8 = 0x65
)

var (
	MinKey = Key{}
	MaxKey = Key{0xFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0XFF, 0xFFFFFFFF}
)

var (
	ErrKeyOverflow = errors.New("key: overflow past MaxKey")
	ErrKeyLength = errors.New("key: wrong length")
)


// Everything in the engine is sorted by key: the in-memory index, the B-tree inside files,the layer map, KeySpace. 
// If two parts of the system disagree on order, a search lands on the wrong page. 
// Compare is the single source of truth for ordering
// Compare field by field in order (field1 first, field6 last). Returns -1, 0 or 1
func (k Key) Compare(other Key) int {
	return cmp.Or(
		cmp.Compare(k.Field1, other.Field1),
		cmp.Compare(k.Field2, other.Field2),
		cmp.Compare(k.Field3, other.Field3),
		cmp.Compare(k.Field4, other.Field4),
		cmp.Compare(k.Field5, other.Field5),
		cmp.Compare(k.Field6, other.Field6),
		
	)
} 


func (k Key) Less(o Key) bool {return k.Compare(o) < 0}


// Max returns the larger of two keys.
func Max(a, b Key) Key {
	if a.Compare(b) >= 0 {
		return a
	}
	return b
}


func Min(a,b Key) Key {
	if a.Compare(b) <= 0{
		return a
	}
	return b
}


func (k Key) PutBytes(b []byte) {
	_ = b[KeySize-1]
	b[0] = k.Field1
	binary.BigEndian.PutUint32(b[1:5] , k.Field2)
	binary.BigEndian.PutUint32(b[5:9], k.Field3)
	binary.BigEndian.PutUint32(b[9:13], k.Field4)
	b[13] = k.Field5
	binary.BigEndian.PutUint32(b[14:18], k.Field6)
}

func (k Key) Encode() [KeySize] byte{
	var b  [KeySize]byte 
	k.PutBytes(b[:])
	return b	
}

func DecodeKey(b []byte) (Key, error) {
	if len(b) != KeySize {
		return Key{}, fmt.Errorf("%w : got  %d, want %d", ErrKeyLength, len(b),KeySize)
	}

	return Key{
		Field1:  b[0],
		Field2: binary.BigEndian.Uint32(b[1:5]),
		Field3: binary.BigEndian.Uint32(b[5:9]),
		Field4: binary.BigEndian.Uint32(b[9:13]),
		Field5: b[13],
		Field6: binary.BigEndian.Uint32(b[14:18]),
	},  nil
}


// return the 36 char uppercase hex form used in layer file names
func (k Key) String() string {
	return fmt.Sprintf("%02X%08X%08X%08X%02X%08X",
		k.Field1, k.Field2, k.Field3, k.Field4, k.Field5, k.Field6,
	)
}


// parses the output of stringify key hex
func ParseKeyHex(s string) (Key, error)  {
	if len(s) != HexLen {
		return Key{}, fmt.Errorf("%w: hex key %q has %d chars, want %d", ErrKeyLength, s, len(s), HexLen)
	}

	parse := func(part string, bits int) (uint64, error) {
		v, err := strconv.ParseUint(part, 16, bits)
		if err != nil {
			return 0, fmt.Errorf("key: invalid hex %q : %w", s, err)
		}
		return v, nil
	}

	f1, err := parse(s[0:2], 8)
	if err != nil {
		return Key{}, err 
	}

	f2, err := parse(s[2:10], 32) 
	if err != nil {
		return Key{}, err
	}

	f3, err := parse(s[10:18], 32)
	if err != nil{
		return Key{}, err
	}

	f4, err := parse(s[18:26], 32)
	if err != nil{
		return Key{}, err
	}

	f5, err := parse(s[26: 28], 8)
	if err != nil{
		return Key{}, err
	}

	f6, err := parse(s[28:36], 32) 
	if err != nil {
		return Key{}, err
	}

	return Key{
		uint8(f1),
		uint32(f2),
		uint32(f3),
		uint32(f4),
		uint8(f5),
		uint32(f6),
	}, nil
}

func addU32(a , b uint32 ) (uint32, bool) {
	s := a +b
	return s, s < a
}

func addU8(a, b uint8) (uint8, bool) {
	s := a + b 
	return s, s < a 
}

func (k Key) Add(n uint32) (Key, error) {
	var carry bool 
	k.Field6, carry = addU32(k.Field6, n)
	if !carry{
		return k, nil
	}

	k.Field5, carry = addU8(k.Field5, 1)
	if !carry {
		return k, nil
	}

	k.Field4, carry = addU32(k.Field4, 1)
	if !carry {
		return k, nil
	}
	k.Field3, carry = addU32(k.Field3, 1)
	if !carry {
		return k, nil
	}
	k.Field2, carry = addU32(k.Field2, 1)
	if !carry {
		return k, nil
	}
	k.Field1, carry = addU8(k.Field1, 1)
	if carry {
		return k, ErrKeyOverflow
	}

	return k, nil
}

func (k Key) Next() (Key, error) { return k.Add(1) }

func (k Key) IsMetadata() bool {
	return k.Field1 >= MetadataPrefixBegin
}


func (k Key) toBig() *big.Int {
	b:= k.Encode()
	return new(big.Int).SetBytes(b[:])
}

var maxKeyBig = MaxKey.toBig()

func keyFromBig(x *big.Int) (Key, error) {
	if x.Sign() < 0 || x.Cmp(maxKeyBig) > 0{
		return Key{}, ErrKeyOverflow
	}

	var b [KeySize]byte
	x.FillBytes(b[:])
	return DecodeKey(b[:])
}

func Distance(a, b Key) *big.Int {
	if b.Compare(a) <= 0 {
		return new(big.Int)
	}

	return new(big.Int).Sub(b.toBig(), a.toBig())
}

func (k Key) AddBig(n *big.Int) (Key, error) {
	return keyFromBig(new(big.Int).Add(k.toBig(), n))
}
