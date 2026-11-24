package timeline

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/Fadil-Tao/naon/internal/pageserver/lsn"
	"github.com/Fadil-Tao/naon/internal/pageserver/remote"
)

const fpAfterLayerUpload = "upload:after-layer"

func (t *Timeline) RemoteConsistentLsn() lsn.Lsn { return lsn.Lsn(t.remoteLsn.Load()) }

func (t *Timeline) savedMetadata() Metadata {
	t.metaMu.Lock()
	defer t.metaMu.Unlock()
	return t.savedMeta
}

func (t *Timeline) requestUpload() {
	t.syncReq.Add(1)
	select {
	case t.uploadCh <- struct{}{}:
	default:
	}
}

func (t *Timeline) UploadAndWait(ctx context.Context) error {
	if t.plugins.Remote == nil {
		return nil
	}
	if err := t.FlushAndWait(ctx); err != nil {
		return err
	}
	t.requestUpload()
	return t.synced.Wait(ctx, lsn.Lsn(t.syncReq.Load()))
}

func (t *Timeline) uploadLoop() {
	defer close(t.upDone)
	if t.plugins.Remote == nil {
		<-t.stopCh
		return
	}
	for {
		select {
		case <-t.stopCh:
			return
		case <-t.uploadCh:
		}
		for {
			req := t.syncReq.Load()
			err := t.syncRemote(t.bgCtx)
			if err == nil {
				t.synced.Advance(lsn.Lsn(req))
				break
			}
			if t.bgCtx.Err() != nil || errors.Is(err, errSimulatedCrash) {
				return
			}
			t.setBackgroundErr(err)
			select {
			case <-t.stopCh:
				return
			case <-time.After(t.cfg.FlushRetryDelay):
			}
		}
	}
}

func (t *Timeline) syncRemote(ctx context.Context) error {
	st := t.plugins.Remote
	base := remote.TimelinePath(t.tenant, t.id)
	if t.remoteLayers == nil {
		b, err := st.Get(ctx, base+"/"+remote.IndexPartName)
		switch {
		case errors.Is(err, remote.ErrNotFound):
			t.remoteLayers = map[string]remote.LayerInfo{}
		case err != nil:
			return err
		default:
			ip, err := remote.DecodeIndexPart(b)
			if err != nil {
				return err
			}
			t.remoteLayers = ip.Layers
			t.remoteIndexed = true
			if m, err := DecodeMetadata(ip.Metadata); err == nil {
				t.remoteLsn.Store(uint64(m.DiskConsistentLsn))
			}
		}
	}

	meta := t.savedMetadata()
	dcl := meta.DiskConsistentLsn
	changed := false
	for _, e := range t.layers.Snapshot().Historic() {
		if e.Desc.LsnRange.End-1 > dcl {
			continue
		}
		name := e.Desc.FileName()
		if _, ok := t.remoteLayers[name]; ok {
			continue
		}
		local := filepath.Join(t.dir, name)
		fi, err := os.Stat(local)
		if err != nil {
			return err
		}
		if err := st.Upload(ctx, local, base+"/"+name); err != nil {
			return err
		}
		if err := t.fp(fpAfterLayerUpload); err != nil {
			return err
		}
		t.remoteLayers[name] = remote.LayerInfo{FileSize: fi.Size()}
		changed = true
	}
	if !changed && t.remoteIndexed && dcl == t.RemoteConsistentLsn() {
		return nil
	}
	ip := remote.IndexPart{Version: remote.IndexVersion, Layers: t.remoteLayers, Metadata: meta.Encode()}
	b, err := ip.Encode()
	if err != nil {
		return err
	}
	if err := st.Put(ctx, base+"/"+remote.IndexPartName, b); err != nil {
		return err
	}
	t.remoteIndexed = true
	t.remoteLsn.Store(uint64(dcl))
	return nil
}
