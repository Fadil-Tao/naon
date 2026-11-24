package remote

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/Fadil-Tao/naon/internal/pageserver/iox"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type S3Config struct {
	Endpoint  string
	Bucket    string
	Prefix    string
	Region    string
	AccessKey string
	SecretKey string
	UseSSL    bool
}

type S3 struct {
	client *minio.Client
	bucket string
	prefix string
}

var _ Storage = (*S3)(nil)

func NewS3(cfg S3Config) (*S3, error) {
	creds := credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, "")
	if cfg.AccessKey == "" {
		creds = credentials.NewEnvAWS()
	}
	c, err := minio.New(cfg.Endpoint, &minio.Options{Creds: creds, Secure: cfg.UseSSL, Region: cfg.Region})
	if err != nil {
		return nil, fmt.Errorf("remote: s3 client: %w", err)
	}
	return &S3{client: c, bucket: cfg.Bucket, prefix: strings.Trim(cfg.Prefix, "/")}, nil
}

func (s *S3) key(p string) string {
	clean := strings.TrimPrefix(path.Clean("/"+p), "/")
	if s.prefix == "" {
		return clean
	}
	return s.prefix + "/" + clean
}

func notFound(err error, p string) error {
	if minio.ToErrorResponse(err).Code == minio.NoSuchKey {
		return fmt.Errorf("%w: %s", ErrNotFound, p)
	}
	return err
}

func (s *S3) Upload(ctx context.Context, localPath, remotePath string) error {
	_, err := s.client.FPutObject(ctx, s.bucket, s.key(remotePath), localPath, minio.PutObjectOptions{})
	if err != nil {
		return fmt.Errorf("remote: upload %s: %w", remotePath, err)
	}
	return nil
}

func (s *S3) Download(ctx context.Context, remotePath, localPath string) error {
	if err := os.MkdirAll(filepath.Dir(localPath), 0o755); err != nil {
		return err
	}
	obj, err := s.client.GetObject(ctx, s.bucket, s.key(remotePath), minio.GetObjectOptions{})
	if err != nil {
		return notFound(err, remotePath)
	}
	defer obj.Close()
	out, err := iox.Create(localPath)
	if err != nil {
		return err
	}
	defer out.Abort()
	if _, err := io.Copy(out, obj); err != nil {
		return notFound(err, remotePath)
	}
	return out.Commit()
}

func (s *S3) Put(ctx context.Context, remotePath string, data []byte) error {
	_, err := s.client.PutObject(ctx, s.bucket, s.key(remotePath), bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})
	if err != nil {
		return fmt.Errorf("remote: put %s: %w", remotePath, err)
	}
	return nil
}

func (s *S3) Get(ctx context.Context, remotePath string) ([]byte, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, s.key(remotePath), minio.GetObjectOptions{})
	if err != nil {
		return nil, notFound(err, remotePath)
	}
	defer obj.Close()
	b, err := io.ReadAll(obj)
	if err != nil {
		return nil, notFound(err, remotePath)
	}
	return b, nil
}

func (s *S3) List(ctx context.Context, remoteDir string) ([]string, error) {
	prefix := s.key(remoteDir) + "/"
	var names []string
	for obj := range s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: prefix}) {
		if obj.Err != nil {
			return nil, fmt.Errorf("remote: list %s: %w", remoteDir, obj.Err)
		}
		name := strings.TrimSuffix(strings.TrimPrefix(obj.Key, prefix), "/")
		if name != "" {
			names = append(names, name)
		}
	}
	return names, nil
}
