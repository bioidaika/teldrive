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

type documentDCContextKey struct{}

func withDocumentDC(ctx context.Context, dc int) context.Context {
	if dc <= 0 {
		return ctx
	}
	return context.WithValue(ctx, documentDCContextKey{}, dc)
}

func documentDC(ctx context.Context) (int, bool) {
	dc, ok := ctx.Value(documentDCContextKey{}).(int)
	return dc, ok && dc > 0
}

// migratingPool mirrors gotd's invokeDirect/invokeSub handling for file and
// stats redirects without migrating the primary session. Unlike invokeSub's
// single-connection pool, target pools retain the configured connection count.
// Each request starts on the home DC unless a document DC hint routes an
// upload.getFile call directly to a non-home target DC.
type migratingPool struct {
	home     telegram.CloseInvoker
	api      tg.Invoker
	homeDC   func() int
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

func (p *migratingPool) currentHomeDC() int {
	if p.homeDC == nil {
		return 0
	}
	return p.homeDC()
}

func isUploadGetFileRequest(input bin.Encoder) bool {
	req, ok := input.(*tg.UploadGetFileRequest)
	return ok && req != nil
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
	hintDC, hasHint := 0, false
	if isUploadGetFileRequest(input) {
		hintDC, hasHint = documentDC(ctx)
	}
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
		}
		if hasHint {
			homeDC := p.currentHomeDC()
			if homeDC <= 0 || hintDC != homeDC {
				p.mu.Unlock()
				return p.invokeHintedSub(ctx, docID, cacheable, hintDC, input, output)
			}
			// Document metadata already points to the home DC, so parallel chunk
			// reads can proceed immediately without serializing on a probe.
			break
		}
		if cacheable {
			if waitCh, ok := p.probing[docID]; ok {
				p.mu.Unlock()
				select {
				case <-ctx.Done():
					return markNonPoolTimeout(ctx.Err())
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
			if ok && cacheable {
				p.recordFileDC(docID, dc)
			}
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

	usedHome, err := p.invokeCurrentHome(ctx, home, api, input, output)
	if isPoolTimeout(err) {
		finishProbe(0, false)
		p.resetHome(usedHome)
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

func (p *migratingPool) invokeCurrentHome(ctx context.Context, home telegram.CloseInvoker, api tg.Invoker, input bin.Encoder, output bin.Decoder) (telegram.CloseInvoker, error) {
	for attempt := range 2 {
		err := api.Invoke(ctx, input, output)
		if attempt == 0 && errors.Is(err, errMigratingPoolClosed) && ctx.Err() == nil {
			p.mu.Lock()
			if !p.closed && p.home != home {
				home = p.home
				api = p.api
				p.mu.Unlock()
				continue
			}
			p.mu.Unlock()
		}
		return home, err
	}
	return home, errMigratingPoolClosed
}

func (p *migratingPool) recordFileDC(docID int64, dc int) {
	p.mu.Lock()
	if !p.closed {
		p.fileDCs[docID] = dc
	}
	p.mu.Unlock()
}

func (p *migratingPool) invokeHintedSub(ctx context.Context, docID int64, cacheable bool, dc int, input bin.Encoder, output bin.Decoder) error {
	err := p.invokeSub(ctx, dc, input, output)
	if err == nil {
		if cacheable {
			p.recordFileDC(docID, dc)
		}
		return nil
	}
	rpcErr, ok := tgerr.As(err)
	if !ok || !rpcErr.IsOneOf("FILE_MIGRATE", "STATS_MIGRATE") || rpcErr.Argument <= 0 || rpcErr.Argument == dc {
		return err
	}
	targetDC := rpcErr.Argument
	if homeDC := p.currentHomeDC(); homeDC > 0 && targetDC == homeDC {
		if cacheable {
			p.recordFileDC(docID, 0)
		}
		return p.invokeHomeOnly(ctx, input, output)
	}
	if cacheable {
		p.recordFileDC(docID, targetDC)
	}
	return p.invokeSub(ctx, targetDC, input, output)
}

func (p *migratingPool) invokeHomeOnly(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return errMigratingPoolClosed
	}
	home := p.home
	api := p.api
	p.mu.Unlock()
	usedHome, err := p.invokeCurrentHome(ctx, home, api, input, output)
	if isPoolTimeout(err) {
		p.resetHome(usedHome)
	}
	return err
}

func (p *migratingPool) invokeSub(ctx context.Context, dc int, input bin.Encoder, output bin.Decoder) error {
	for attempt := range 2 {
		entry, openErr := p.target(ctx, dc)
		if openErr != nil {
			return fmt.Errorf("open Telegram target DC %d: %w", dc, openErr)
		}
		// Like gotd's invokeSub, invoke directly on the authorized target rather
		// than recursively redirecting or changing the primary session.
		err := entry.api.Invoke(ctx, input, output)
		if isPoolTimeout(err) {
			p.evictTarget(dc, entry)
			return err
		}
		if attempt == 0 && errors.Is(err, errMigratingPoolClosed) && ctx.Err() == nil && p.isTargetEvicted(dc, entry) {
			continue
		}
		return err
	}
	return errMigratingPoolClosed
}

func (p *migratingPool) isTargetEvicted(dc int, entry *dcPoolEntry) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return !p.closed && p.pools[dc] != entry
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
			return nil, markNonPoolTimeout(ctx.Err())
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
