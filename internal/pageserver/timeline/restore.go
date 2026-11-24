package timeline

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/Fadil-Tao/naon/internal/pageserver/ids"
	"github.com/Fadil-Tao/naon/internal/pageserver/remote"
)

var ErrNoRemoteTimeline = errors.New("timeline: not found in remote storage")

func Restore(ctx context.Context, st remote.Storage, dir string, tenant ids.TenantID, id ids.TimelineID) (Metadata, error) {
	base := remote.TimelinePath(tenant, id)
	b, err := st.Get(ctx, base+"/"+remote.IndexPartName)
	if errors.Is(err, remote.ErrNotFound) {
		return Metadata{}, fmt.Errorf("%w: %s", ErrNoRemoteTimeline, id)
	}
	if err != nil {
		return Metadata{}, err
	}
	ip, err := remote.DecodeIndexPart(b)
	if err != nil {
		return Metadata{}, err
	}
	meta, err := DecodeMetadata(ip.Metadata)
	if err != nil {
		return Metadata{}, err
	}
	metaPath := filepath.Join(dir, MetadataFileName)
	if _, err := os.Stat(metaPath); err == nil {
		return Metadata{}, fmt.Errorf("timeline: restore into %s: %w", dir, fs.ErrExist)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return Metadata{}, err
	}
	for name, info := range ip.Layers {
		local := filepath.Join(dir, name)
		if fi, err := os.Stat(local); err == nil && fi.Size() == info.FileSize {
			continue
		}
		if err := st.Download(ctx, base+"/"+name, local); err != nil {
			return Metadata{}, fmt.Errorf("timeline: download %s: %w", name, err)
		}
		fi, err := os.Stat(local)
		if err != nil {
			return Metadata{}, err
		}
		if fi.Size() != info.FileSize {
			return Metadata{}, fmt.Errorf("timeline: %s is %d bytes, index says %d", name, fi.Size(), info.FileSize)
		}
	}
	if err := SaveMetadata(metaPath, meta); err != nil {
		return Metadata{}, err
	}
	return meta, nil
}
