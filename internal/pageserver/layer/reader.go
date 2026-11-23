package layer

import (
	"github.com/Fadil-Tao/naon/internal/pageserver/key"
	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
	"github.com/Fadil-Tao/naon/internal/pageserver/value"
)


// reader is what the read path needs ffrom any layer, delta or image layer
type Reader interface {
	Desc() Desc

	GetValueReconstructData(k key.Key, lsnRange lsn.Range, st *value.ReconstructState) error

	Close() error
}