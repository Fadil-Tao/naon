package remote

import (
	"context"
	"errors"

	"github.com/Fadil-Tao/naon/internal/pageserver/ids"
)

const IndexPartName = "index_part.json"

var ErrNotFound = errors.New("remote: object not found")

type Storage interface {
	Upload(ctx context.Context, localPath, remotePath string) error
	Download(ctx context.Context, remotePath, localPath string) error
	Put(ctx context.Context, remotePath string, data []byte) error
	Get(ctx context.Context, remotePath string) ([]byte, error)
	List(ctx context.Context, remoteDir string) ([]string, error)
}

func TimelinesPath(tenant ids.TenantID) string {
	return "tenants/" + tenant.String() + "/timelines"
}

func TimelinePath(tenant ids.TenantID, timeline ids.TimelineID) string {
	return TimelinesPath(tenant) + "/" + timeline.String()
}
