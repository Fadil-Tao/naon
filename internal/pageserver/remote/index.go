package remote

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Fadil-Tao/naon/internal/pageserver/layer"
)

const IndexVersion = 1

var ErrBadIndex = errors.New("remote: bad index_part")

type LayerInfo struct {
	FileSize int64 `json:"file_size"`
}

type IndexPart struct {
	Version  int                  `json:"version"`
	Layers   map[string]LayerInfo `json:"layer_metadata"`
	Metadata []byte               `json:"metadata_bytes"`
}

func (ip IndexPart) Encode() ([]byte, error) {
	return json.MarshalIndent(ip, "", "  ")
}

func DecodeIndexPart(b []byte) (IndexPart, error) {
	var ip IndexPart
	if err := json.Unmarshal(b, &ip); err != nil {
		return IndexPart{}, fmt.Errorf("%w: %v", ErrBadIndex, err)
	}
	if ip.Version != IndexVersion {
		return IndexPart{}, fmt.Errorf("%w: version %d", ErrBadIndex, ip.Version)
	}
	if ip.Layers == nil {
		ip.Layers = map[string]LayerInfo{}
	}
	for name, info := range ip.Layers {
		if _, err := layer.ParseFileName(name); err != nil {
			return IndexPart{}, fmt.Errorf("%w: %v", ErrBadIndex, err)
		}
		if info.FileSize < 0 {
			return IndexPart{}, fmt.Errorf("%w: %s has size %d", ErrBadIndex, name, info.FileSize)
		}
	}
	return ip, nil
}
