package telegramstore

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

var errMigratingPoolClosed = errors.New("Telegram connection pool closed")

type dcPoolEntry struct {
	ready chan struct{}
	pool  telegram.CloseInvoker
	api   tg.Invoker
	err   error
}

// migratingPool mirrors gotd's invokeDirect/invokeSub handling for file and
// stats redirects without migrating the primary session. Unlike invokeSub's
// single-connection pool, target pools retain the configured connection count.
// Each request starts on the home DC: different files may live on different DCs.
type migratingPool struct {
	home     telegram.CloseInvoker
	api      tg.Invoker
	openHome func() (telegram.CloseInvoker, error)
	open     func(context.Context, int) (telegram.CloseInvoker, error)
	wrap     func(tg.Invoker) tg.Invoker
	mu       sync.Mutex
	pools    map[int]*dcPoolEntry
	fileDCs  map[int64]int
	probing  map[int64]chan struct{}
	closed   bool
}

func newMigratingPool(home telegram.CloseInvoker, open func(context.Context, int) (telegram.CloseInvoker, error), wrap func(tg.Invoker) tg.Invoker) *migratingPool {
	return &migratingPool{
		home:    home,
		api:     wrap(home),
		open:    open,
		wrap:    wrap,
		pools:   make(map[int]*dcPoolEntry),
		fileDCs: make(map[int64]int),
		probing: make(map[int64]chan struct{}),
	}
}

func cacheableDocumentID(input bin.Encoder) (int64, bool) {
	req, ok := input.(*tg.UploadGetFileRequest)
	if !ok || req == nil {
		return 0, false
	}
	loc, ok := req.Location.(*tg.InputDocumentFileLocation)
	if !ok || loc == nil || loc.ID == 0 || (loc.AccessHash == 0 && len(loc.FileReference) == 0) {
		return 0, false
	}
	return loc.ID, true
}

func (p *migratingPool) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	docID, cacheable := cacheableDocumentID(input)
	var ownProbe chan struct{}

	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return errMigratingPoolClosed
		}
		if cacheable {
			if dc, ok := p.fileDCs[docID]; ok {
				if dc > 0 {
					p.mu.Unlock()
					return p.invokeSub(ctx, dc, input, output)
				}
				break
			}
			if waitCh, ok := p.probing[docID]; ok {
				p.mu.Unlock()
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-waitCh:
				}
				continue
			}
			ownProbe = make(chan struct{})
			p.probing[docID] = ownProbe
		}
		break
	}
	home := p.home
	api := p.api
	p.mu.Unlock()

	finishProbe := func(dc int, ok bool) {
		if ownProbe == nil {
			return
		}
		p.mu.Lock()
		if ok && !p.closed {
			p.fileDCs[docID] = dc
		}
		delete(p.probing, docID)
		close(ownProbe)
		ownProbe = nil
		p.mu.Unlock()
	}
	defer finishProbe(0, false)

	err := api.Invoke(ctx, input, output)
	if errors.Is(err, context.DeadlineExceeded) {
		finishProbe(0, false)
		p.resetHome(home)
		return err
	}
	if err == nil {
		finishProbe(0, true)
		return nil
	}
	rpcErr, ok := tgerr.As(err)
	if !ok || !rpcErr.IsOneOf("FILE_MIGRATE", "STATS_MIGRATE") || rpcErr.Argument <= 0 {
		return err
	}
	finishProbe(rpcErr.Argument, true)
	return p.invokeSub(ctx, rpcErr.Argument, input, output)
}

func (p *migratingPool) invokeSub(ctx context.Context, dc int, input bin.Encoder, output bin.Decoder) error {
	entry, openErr := p.target(ctx, dc)
	if openErr != nil {
		return fmt.Errorf("open Telegram target DC %d: %w", dc, openErr)
	}
	// Like gotd's invokeSub, invoke directly on the authorized target rather
	// than recursively redirecting or changing the primary session.
	err := entry.api.Invoke(ctx, input, output)
	if errors.Is(err, context.DeadlineExceeded) {
		p.evictTarget(dc, entry)
	}
	return err
}

func (p *migratingPool) target(ctx context.Context, dc int) (*dcPoolEntry, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, errMigratingPoolClosed
	}
	if entry, ok := p.pools[dc]; ok {
		p.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-entry.ready:
			return entry, entry.err
		}
	}
	entry := &dcPoolEntry{ready: make(chan struct{})}
	p.pools[dc] = entry
	p.mu.Unlock()

	pool, err := p.open(ctx, dc)
	p.mu.Lock()
	if p.closed && err == nil {
		err = errors.Join(errMigratingPoolClosed, pool.Close())
	}
	if err != nil {
		// A cancelled request or failed authorization must not poison later reads.
		delete(p.pools, dc)
	} else {
		entry.pool, entry.api = pool, p.wrap(pool)
	}
	entry.err = err
	close(entry.ready)
	p.mu.Unlock()
	return entry, entry.err
}

func (p *migratingPool) resetHome(failed telegram.CloseInvoker) {
	if p.openHome == nil {
		return
	}
	p.mu.Lock()
	if p.closed || p.home != failed {
		p.mu.Unlock()
		return
	}
	replacement, err := p.openHome()
	if err != nil {
		p.mu.Unlock()
		return
	}
	p.home = replacement
	p.api = p.wrap(replacement)
	for docID, cachedDC := range p.fileDCs {
		if cachedDC == 0 {
			delete(p.fileDCs, docID)
		}
	}
	p.mu.Unlock()
	if failed != nil {
		_ = failed.Close()
	}
}

func (p *migratingPool) evictTarget(dc int, failed *dcPoolEntry) {
	if failed == nil {
		return
	}
	p.mu.Lock()
	if p.closed || p.pools[dc] != failed {
		p.mu.Unlock()
		return
	}
	delete(p.pools, dc)
	for docID, cachedDC := range p.fileDCs {
		if cachedDC == dc {
			delete(p.fileDCs, docID)
		}
	}
	p.mu.Unlock()
	if failed.pool != nil {
		_ = failed.pool.Close()
	}
}

func (p *migratingPool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	var pools []telegram.CloseInvoker
	for _, entry := range p.pools {
		if entry.pool != nil {
			pools = append(pools, entry.pool)
		}
	}
	p.mu.Unlock()
	err := p.home.Close()
	for _, pool := range pools {
		err = errors.Join(err, pool.Close())
	}
	return err
}
