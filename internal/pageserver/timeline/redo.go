package timeline

import (
	"github.com/Fadil-Tao/naon/internal/pageserver/key"
	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
	"github.com/Fadil-Tao/naon/internal/pageserver/value"
)

type Redo interface {
	Reconstruct(k key.Key, at lsn.Lsn, in value.ReconstructInput) ([]byte, error)
}

type RedoFunc func(k key.Key, at lsn.Lsn, in value.ReconstructInput) ([]byte, error)

func (f RedoFunc) Reconstruct(k key.Key, at lsn.Lsn, in value.ReconstructInput) ([]byte, error) {
	return f(k, at, in)
}

var TestRedo Redo = RedoFunc(func(_ key.Key, _ lsn.Lsn, in value.ReconstructInput) ([]byte, error) {
	return value.ApplyTest(in)
})
