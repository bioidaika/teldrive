package api

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/tgdrive/teldrive/v2/internal/api/gen"
	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/telegramstore"
	"github.com/tgdrive/teldrive/v2/internal/transfer"
)

func TestIsClientDisconnect(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "canceled context", err: context.Canceled, want: true},
		{name: "connection reset", err: syscall.ECONNRESET, want: true},
		{name: "broken pipe", err: syscall.EPIPE, want: true},
		{name: "wrapped connection reset", err: fmt.Errorf("write tcp: %w", syscall.ECONNRESET), want: true},
		{name: "stream failure", err: errors.New("upstream read failed"), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := isClientDisconnect(tt.err); got != tt.want {
				t.Fatalf("isClientDisconnect(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

type rawTestCatalog struct {
	file  *sqlcgen.File
	parts []*sqlcgen.FilePart
}

func (c rawTestCatalog) Get(context.Context, int64, uuid.UUID) (*sqlcgen.File, error) {
	return c.file, nil
}

func (c rawTestCatalog) Parts(context.Context, int64, uuid.UUID) ([]*sqlcgen.FilePart, error) {
	return c.parts, nil
}

type rawTestStorage struct {
	telegramstore.Storage
	openRange func(context.Context, telegramstore.RangeRequest) (io.ReadCloser, error)
}

func (s rawTestStorage) OpenRange(ctx context.Context, req telegramstore.RangeRequest) (io.ReadCloser, error) {
	return s.openRange(ctx, req)
}

type failingReader struct {
	err error
}

func (r failingReader) Read([]byte) (int, error) {
	return 0, r.err
}

func TestStreamFileDoesNotCommit206WhenFirstReadFails(t *testing.T) {
	t.Parallel()

	fileID := uuid.New()
	file := &sqlcgen.File{
		ID:      pgtype.UUID{Bytes: fileID, Valid: true},
		UserID:  42,
		Name:    "movie.mkv",
		Kind:    sqlcgen.FileKindFile,
		Status:  sqlcgen.FileStatusActive,
		Size:    pgtype.Int8{Int64: 1024, Valid: true},
		ModTime: pgtype.Timestamptz{Time: time.Unix(1700000000, 0).UTC(), Valid: true},
	}
	parts := []*sqlcgen.FilePart{{
		PartNo:     1,
		ChannelID:  100,
		MessageID:  200,
		PlainSize:  pgtype.Int8{Int64: 1024, Valid: true},
		StoredSize: pgtype.Int8{Int64: 1024, Valid: true},
	}}

	timeoutErr := fmt.Errorf("download Telegram document chunk at 0: rpc error code -503: Timeout: %w", context.DeadlineExceeded)
	downloader := transfer.NewDownloader(
		rawTestCatalog{file: file, parts: parts},
		rawTestStorage{openRange: func(context.Context, telegramstore.RangeRequest) (io.ReadCloser, error) {
			return io.NopCloser(failingReader{err: timeoutErr}), nil
		}},
		nil,
	)
	raw := NewRawHandler(&Handler{Downloader: downloader})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/files/"+fileID.String()+"/content", nil)
	err := raw.streamFile(req.Context(), rec, 42, fileID, file, gen.NewOptString("bytes=0-511"), gen.OptETag{}, false)
	if err == nil {
		t.Fatal("streamFile() error = nil, want error before committing 206 headers")
	}
	if got := rec.Header().Get("Content-Range"); got != "" {
		t.Fatalf("Content-Range header = %q, want empty before error handler runs", got)
	}
	ErrorHandler(req.Context(), rec, req, err)
	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusGatewayTimeout, rec.Body.String())
	}
}

func TestStreamFileCommits206AndStreamsFullRange(t *testing.T) {
	t.Parallel()

	payload := bytes.Repeat([]byte("abcd"), 16*1024) // 64 KiB across initial 32 KiB pre-read + remainder
	fileID := uuid.New()
	file := &sqlcgen.File{
		ID:      pgtype.UUID{Bytes: fileID, Valid: true},
		UserID:  42,
		Name:    "movie.mkv",
		Kind:    sqlcgen.FileKindFile,
		Status:  sqlcgen.FileStatusActive,
		Size:    pgtype.Int8{Int64: int64(len(payload)), Valid: true},
		ModTime: pgtype.Timestamptz{Time: time.Unix(1700000000, 0).UTC(), Valid: true},
	}
	parts := []*sqlcgen.FilePart{{
		PartNo:     1,
		ChannelID:  100,
		MessageID:  200,
		PlainSize:  pgtype.Int8{Int64: int64(len(payload)), Valid: true},
		StoredSize: pgtype.Int8{Int64: int64(len(payload)), Valid: true},
	}}

	downloader := transfer.NewDownloader(
		rawTestCatalog{file: file, parts: parts},
		rawTestStorage{openRange: func(_ context.Context, req telegramstore.RangeRequest) (io.ReadCloser, error) {
			slice := payload[req.Offset : req.Offset+req.Length]
			return io.NopCloser(bytes.NewReader(slice)), nil
		}},
		nil,
	)
	raw := NewRawHandler(&Handler{Downloader: downloader})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/files/"+fileID.String()+"/content", nil)
	err := raw.streamFile(req.Context(), rec, 42, fileID, file, gen.NewOptString("bytes=100-50099"), gen.OptETag{}, false)
	if err != nil {
		t.Fatalf("streamFile() error = %v", err)
	}
	if rec.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusPartialContent)
	}
	if !bytes.Equal(rec.Body.Bytes(), payload[100:50100]) {
		t.Fatalf("body length = %d, want %d", rec.Body.Len(), 50000)
	}
}
