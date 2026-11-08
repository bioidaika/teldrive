package telegramstore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

type testDCPool struct {
	telegram.InvokeFunc
	closes atomic.Int32
}

func (p *testDCPool) Close() error { p.closes.Add(1); return nil }

func unwrappedPool(api tg.Invoker) tg.Invoker { return api }

func fileRedirectPool(dc int) *testDCPool {
	return &testDCPool{InvokeFunc: func(context.Context, bin.Encoder, bin.Decoder) error {
		return fmt.Errorf("invoke pool: %w", tgerr.New(303, fmt.Sprintf("FILE_MIGRATE_%d", dc)))
	}}
}

func TestMigratingPoolConcurrentReadsReuseTarget(t *testing.T) {
	home := fileRedirectPool(4)
	var opens, reads atomic.Int32
	target := &testDCPool{InvokeFunc: func(_ context.Context, input bin.Encoder, output bin.Decoder) error {
		request := input.(*tg.UploadGetFileRequest)
		output.(*tg.UploadFileBox).File = &tg.UploadFile{Bytes: []byte{byte(request.Offset)}}
		reads.Add(1)
		return nil
	}}
	router := newMigratingPool(home, func(_ context.Context, dc int) (telegram.CloseInvoker, error) {
		opens.Add(1)
		if dc != 4 {
			return nil, fmt.Errorf("unexpected DC %d", dc)
		}
		return target, nil
	}, unwrappedPool)
	var wg sync.WaitGroup
	for n := range 32 {
		wg.Go(func() {
			var output tg.UploadFileBox
			err := router.Invoke(context.Background(), &tg.UploadGetFileRequest{Offset: int64(n)}, &output)
			if err != nil {
				t.Errorf("read: %v", err)
				return
			}
			if got := output.File.(*tg.UploadFile).Bytes; len(got) != 1 || got[0] != byte(n) {
				t.Errorf("wrong chunk: %v", got)
			}
		})
	}
	wg.Wait()
	if opens.Load() != 1 || reads.Load() != 32 {
		t.Fatalf("opens=%d reads=%d", opens.Load(), reads.Load())
	}
	if err := router.Close(); err != nil {
		t.Fatal(err)
	}
	if err := router.Close(); err != nil {
		t.Fatal(err)
	}
	if home.closes.Load() != 1 || target.closes.Load() != 1 {
		t.Fatal("pools not closed exactly once")
	}
	if err := router.Invoke(context.Background(), nil, nil); !errors.Is(err, errMigratingPoolClosed) {
		t.Fatalf("after close: %v", err)
	}
}

func TestMigratingPoolDoesNotRerouteOtherErrors(t *testing.T) {
	for _, rpcErr := range []error{
		nil,
		tgerr.New(400, "FILE_REFERENCE_EXPIRED"),
		tgerr.New(303, "PHONE_MIGRATE_4"),
		tgerr.New(303, "USER_MIGRATE_4"),
		tgerr.New(303, "NETWORK_MIGRATE_4"),
		tgerr.New(303, "FILE_MIGRATE_0"),
		tgerr.New(303, "STATS_MIGRATE_0"),
	} {
		t.Run(fmt.Sprint(rpcErr), func(t *testing.T) {
			home := &testDCPool{InvokeFunc: func(context.Context, bin.Encoder, bin.Decoder) error { return rpcErr }}
			router := newMigratingPool(home, func(context.Context, int) (telegram.CloseInvoker, error) {
				t.Fatal("unexpected DC pool")
				return nil, nil
			}, unwrappedPool)
			defer router.Close()
			if err := router.Invoke(context.Background(), nil, nil); !errors.Is(err, rpcErr) {
				t.Fatalf("got %v want %v", err, rpcErr)
			}
		})
	}
}

func TestMigratingPoolFailedAuthorizationCanRetry(t *testing.T) {
	authErr := errors.New("export authorization failed")
	var opens int
	target := &testDCPool{InvokeFunc: func(context.Context, bin.Encoder, bin.Decoder) error { return nil }}
	router := newMigratingPool(fileRedirectPool(2), func(context.Context, int) (telegram.CloseInvoker, error) {
		opens++
		if opens == 1 {
			return nil, authErr
		}
		return target, nil
	}, unwrappedPool)
	defer router.Close()
	if err := router.Invoke(context.Background(), nil, nil); !errors.Is(err, authErr) {
		t.Fatalf("authorization error: %v", err)
	}
	if err := router.Invoke(context.Background(), nil, nil); err != nil {
		t.Fatal(err)
	}
	if opens != 2 {
		t.Fatalf("opens=%d", opens)
	}
}

func TestMigratingPoolBoundsRedirects(t *testing.T) {
	target := fileRedirectPool(1)
	router := newMigratingPool(fileRedirectPool(4), func(context.Context, int) (telegram.CloseInvoker, error) { return target, nil }, unwrappedPool)
	defer router.Close()
	err := router.Invoke(context.Background(), nil, nil)
	if rpcErr, ok := tgerr.As(err); !ok || rpcErr.Argument != 1 {
		t.Fatalf("target redirect: %v", err)
	}
}

func TestMigratingPoolCancelledWaiterAndCloseDuringOpen(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	target := &testDCPool{InvokeFunc: func(context.Context, bin.Encoder, bin.Decoder) error { t.Error("invoked closed target"); return nil }}
	router := newMigratingPool(fileRedirectPool(4), func(context.Context, int) (telegram.CloseInvoker, error) {
		close(started)
		<-release
		return target, nil
	}, unwrappedPool)
	done := make(chan error, 1)
	go func() { done <- router.Invoke(context.Background(), nil, nil) }()
	<-started
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := router.Invoke(ctx, nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("waiter: %v", err)
	}
	if err := router.Close(); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; !errors.Is(err, errMigratingPoolClosed) {
		t.Fatalf("opener: %v", err)
	}
	if target.closes.Load() != 1 {
		t.Fatal("late pool leaked")
	}
}

func TestMigratingPoolEvictsTimedOutTargetAndReopensOnRetry(t *testing.T) {
	var opens int
	firstTarget := &testDCPool{InvokeFunc: func(context.Context, bin.Encoder, bin.Decoder) error {
		return context.DeadlineExceeded
	}}
	secondTarget := &testDCPool{InvokeFunc: func(context.Context, bin.Encoder, bin.Decoder) error {
		return nil
	}}
	router := newMigratingPool(fileRedirectPool(4), func(context.Context, int) (telegram.CloseInvoker, error) {
		opens++
		if opens == 1 {
			return firstTarget, nil
		}
		return secondTarget, nil
	}, unwrappedPool)
	defer router.Close()

	if err := router.Invoke(context.Background(), nil, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first Invoke() = %v, want context.DeadlineExceeded", err)
	}
	if firstTarget.closes.Load() != 1 {
		t.Fatalf("first target closes = %d, want 1", firstTarget.closes.Load())
	}
	if err := router.Invoke(context.Background(), nil, nil); err != nil {
		t.Fatalf("second Invoke() = %v, want nil", err)
	}
	if opens != 2 {
		t.Fatalf("target opens = %d, want 2", opens)
	}
}

func TestMigratingPoolResetsTimedOutHomePool(t *testing.T) {
	firstHome := &testDCPool{InvokeFunc: func(context.Context, bin.Encoder, bin.Decoder) error {
		return context.DeadlineExceeded
	}}
	secondHome := &testDCPool{InvokeFunc: func(context.Context, bin.Encoder, bin.Decoder) error {
		return nil
	}}
	router := newMigratingPool(firstHome, func(context.Context, int) (telegram.CloseInvoker, error) {
		t.Fatal("unexpected target DC open")
		return nil, nil
	}, unwrappedPool)
	var homeOpens int
	router.openHome = func() (telegram.CloseInvoker, error) {
		homeOpens++
		return secondHome, nil
	}
	defer router.Close()

	if err := router.Invoke(context.Background(), nil, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first Invoke() = %v, want context.DeadlineExceeded", err)
	}
	if firstHome.closes.Load() != 1 || homeOpens != 1 {
		t.Fatalf("firstHome closes = %d, homeOpens = %d; want 1 and 1", firstHome.closes.Load(), homeOpens)
	}
	if err := router.Invoke(context.Background(), nil, nil); err != nil {
		t.Fatalf("second Invoke() = %v, want nil", err)
	}
}

func TestMigratingPoolCachesRealDocumentDCAndDeduplicatesInitialProbe(t *testing.T) {
	var homeCalls, targetCalls atomic.Int32
	home := &testDCPool{InvokeFunc: func(_ context.Context, _ bin.Encoder, _ bin.Decoder) error {
		homeCalls.Add(1)
		return tgerr.New(303, "FILE_MIGRATE_4")
	}}
	target := &testDCPool{InvokeFunc: func(_ context.Context, input bin.Encoder, output bin.Decoder) error {
		targetCalls.Add(1)
		req := input.(*tg.UploadGetFileRequest)
		output.(*tg.UploadFileBox).File = &tg.UploadFile{Bytes: []byte{byte(req.Offset)}}
		return nil
	}}
	router := newMigratingPool(home, func(_ context.Context, dc int) (telegram.CloseInvoker, error) {
		if dc != 4 {
			return nil, fmt.Errorf("unexpected DC %d", dc)
		}
		return target, nil
	}, unwrappedPool)
	defer router.Close()

	loc := &tg.InputDocumentFileLocation{
		ID:            999,
		AccessHash:    12345,
		FileReference: []byte{1, 2, 3},
	}
	var wg sync.WaitGroup
	for n := range 8 {
		wg.Go(func() {
			var out tg.UploadFileBox
			if err := router.Invoke(context.Background(), &tg.UploadGetFileRequest{Location: loc, Offset: int64(n)}, &out); err != nil {
				t.Errorf("Invoke(%d) = %v", n, err)
			}
		})
	}
	wg.Wait()

	if got := homeCalls.Load(); got != 1 {
		t.Fatalf("homeCalls = %d, want 1", got)
	}
	if got := targetCalls.Load(); got != 8 {
		t.Fatalf("targetCalls = %d, want 8", got)
	}
}

func TestShieldedDCPoolDetachesCancelAndAbortsQueuedWaiters(t *testing.T) {
	inFlightStarted := make(chan struct{})
	releaseInFlight := make(chan struct{})
	inFlightDone := make(chan error, 1)
	var innerCalls atomic.Int32

	inner := &testDCPool{InvokeFunc: func(ctx context.Context, _ bin.Encoder, _ bin.Decoder) error {
		if innerCalls.Add(1) == 1 {
			close(inFlightStarted)
			<-releaseInFlight
			inFlightDone <- ctx.Err()
			return ctx.Err()
		}
		return nil
	}}
	pool := newShieldedDCPool(inner, 1)
	defer pool.Close()

	ctx1, cancel1 := context.WithCancel(context.Background())
	err1Ch := make(chan error, 1)
	go func() {
		err1Ch <- pool.Invoke(ctx1, nil, nil)
	}()
	<-inFlightStarted

	ctx2, cancel2 := context.WithCancel(context.Background())
	cancel2()
	if err := pool.Invoke(ctx2, nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("queued cancelled caller = %v, want context.Canceled", err)
	}

	cancel1()
	if err := <-err1Ch; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled active caller = %v, want context.Canceled", err)
	}

	close(releaseInFlight)
	if innerErr := <-inFlightDone; innerErr != nil {
		t.Fatalf("inner RPC saw cancellation %v, want nil (detached from context.Canceled)", innerErr)
	}
	if got := innerCalls.Load(); got != 1 {
		t.Fatalf("innerCalls = %d, want 1", got)
	}
}

func TestShieldedDCPoolRunsConfiguredConcurrency(t *testing.T) {
	const parallel = 4
	allStarted := make(chan struct{})
	release := make(chan struct{})
	var active atomic.Int32

	inner := &testDCPool{InvokeFunc: func(_ context.Context, _ bin.Encoder, _ bin.Decoder) error {
		if active.Add(1) == parallel {
			close(allStarted)
		}
		<-release
		return nil
	}}
	pool := newShieldedDCPool(inner, parallel)
	defer pool.Close()

	var wg sync.WaitGroup
	for range parallel {
		wg.Go(func() {
			if err := pool.Invoke(context.Background(), nil, nil); err != nil {
				t.Errorf("Invoke() = %v", err)
			}
		})
	}
	<-allStarted

	ctxExtra, cancelExtra := context.WithCancel(context.Background())
	cancelExtra()
	if err := pool.Invoke(ctxExtra, nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("fifth caller while full = %v, want context.Canceled", err)
	}

	close(release)
	wg.Wait()
}

func TestMigratingPoolRoutesHintedCrossDCDocumentWithoutProbingHome(t *testing.T) {
	var homeCalls, opens, targetCalls atomic.Int32
	home := &testDCPool{InvokeFunc: func(_ context.Context, input bin.Encoder, _ bin.Decoder) error {
		homeCalls.Add(1)
		if _, ok := input.(*tg.UploadGetFileRequest); ok {
			return tgerr.New(-503, "Timeout")
		}
		return nil
	}}
	target := &testDCPool{InvokeFunc: func(_ context.Context, input bin.Encoder, output bin.Decoder) error {
		targetCalls.Add(1)
		req := input.(*tg.UploadGetFileRequest)
		output.(*tg.UploadFileBox).File = &tg.UploadFile{Bytes: []byte{byte(req.Offset)}}
		return nil
	}}
	router := newMigratingPool(home, func(_ context.Context, dc int) (telegram.CloseInvoker, error) {
		if dc != 4 {
			return nil, fmt.Errorf("unexpected target DC %d", dc)
		}
		opens.Add(1)
		return target, nil
	}, unwrappedPool)
	router.homeDC = func() int { return 5 }
	defer router.Close()

	// Non-upload.getFile RPCs must stay on the home DC even if a DC hint is attached.
	if err := router.Invoke(withDocumentDC(context.Background(), 4), &tg.ChannelsGetMessagesRequest{}, nil); err != nil {
		t.Fatalf("metadata Invoke() = %v", err)
	}
	if got := homeCalls.Load(); got != 1 {
		t.Fatalf("metadata homeCalls = %d, want 1", got)
	}

	loc := &tg.InputDocumentFileLocation{
		ID:            777,
		AccessHash:    999,
		FileReference: []byte{1, 2, 3},
	}
	var wg sync.WaitGroup
	for n := range 8 {
		wg.Go(func() {
			var out tg.UploadFileBox
			ctx := withDocumentDC(context.Background(), 4)
			if err := router.Invoke(ctx, &tg.UploadGetFileRequest{Location: loc, Offset: int64(n)}, &out); err != nil {
				t.Errorf("Invoke(%d) = %v", n, err)
				return
			}
			if got := out.File.(*tg.UploadFile).Bytes; len(got) != 1 || got[0] != byte(n) {
				t.Errorf("chunk %d = %v", n, got)
			}
		})
	}
	wg.Wait()

	if got := homeCalls.Load(); got != 1 {
		t.Fatalf("homeCalls after cross-DC reads = %d, want 1 (no home UploadGetFile probe)", got)
	}
	if got := opens.Load(); got != 1 {
		t.Fatalf("target opens = %d, want 1", got)
	}
	if got := targetCalls.Load(); got != 8 {
		t.Fatalf("targetCalls = %d, want 8", got)
	}
}

func TestMigratingPoolUsesHomeDirectlyWhenDocumentHintMatchesHomeDC(t *testing.T) {
	var homeCalls atomic.Int32
	home := &testDCPool{InvokeFunc: func(_ context.Context, input bin.Encoder, output bin.Decoder) error {
		homeCalls.Add(1)
		req := input.(*tg.UploadGetFileRequest)
		output.(*tg.UploadFileBox).File = &tg.UploadFile{Bytes: []byte{byte(req.Offset)}}
		return nil
	}}
	router := newMigratingPool(home, func(_ context.Context, dc int) (telegram.CloseInvoker, error) {
		t.Fatalf("unexpected sub-DC open for home DC %d", dc)
		return nil, nil
	}, unwrappedPool)
	router.homeDC = func() int { return 5 }
	defer router.Close()

	loc := &tg.InputDocumentFileLocation{ID: 888, AccessHash: 111, FileReference: []byte{9}}
	var wg sync.WaitGroup
	for n := range 4 {
		wg.Go(func() {
			var out tg.UploadFileBox
			if err := router.Invoke(withDocumentDC(context.Background(), 5), &tg.UploadGetFileRequest{Location: loc, Offset: int64(n)}, &out); err != nil {
				t.Errorf("Invoke(%d) = %v", n, err)
			}
		})
	}
	wg.Wait()

	if got := homeCalls.Load(); got != 4 {
		t.Fatalf("homeCalls = %d, want 4", got)
	}
}

func TestMigratingPoolReroutesStaleDocumentDCHint(t *testing.T) {
	var dc2Calls, dc4Calls atomic.Int32
	home := &testDCPool{InvokeFunc: func(context.Context, bin.Encoder, bin.Decoder) error {
		t.Fatal("unexpected home call")
		return nil
	}}
	dc2 := &testDCPool{InvokeFunc: func(context.Context, bin.Encoder, bin.Decoder) error {
		dc2Calls.Add(1)
		return tgerr.New(303, "FILE_MIGRATE_4")
	}}
	dc4 := &testDCPool{InvokeFunc: func(_ context.Context, _ bin.Encoder, output bin.Decoder) error {
		dc4Calls.Add(1)
		output.(*tg.UploadFileBox).File = &tg.UploadFile{Bytes: []byte{4}}
		return nil
	}}
	router := newMigratingPool(home, func(_ context.Context, dc int) (telegram.CloseInvoker, error) {
		switch dc {
		case 2:
			return dc2, nil
		case 4:
			return dc4, nil
		default:
			return nil, fmt.Errorf("unexpected DC %d", dc)
		}
	}, unwrappedPool)
	router.homeDC = func() int { return 5 }
	defer router.Close()

	loc := &tg.InputDocumentFileLocation{ID: 555, AccessHash: 222, FileReference: []byte{7}}
	for range 2 {
		var out tg.UploadFileBox
		if err := router.Invoke(withDocumentDC(context.Background(), 2), &tg.UploadGetFileRequest{Location: loc}, &out); err != nil {
			t.Fatalf("Invoke() = %v", err)
		}
	}
	if dc2Calls.Load() != 1 || dc4Calls.Load() != 2 {
		t.Fatalf("dc2Calls=%d dc4Calls=%d, want 1 and 2", dc2Calls.Load(), dc4Calls.Load())
	}
}

func TestMigratingPoolDoesNotResetPoolOnServerRPCTimeout(t *testing.T) {
	siblingStarted := make(chan struct{})
	var startOnce sync.Once
	releaseSibling := make(chan struct{})
	innerHome := &testDCPool{InvokeFunc: func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		req := input.(*tg.UploadGetFileRequest)
		if req.Offset == 0 {
			return tgerr.New(-503, "Timeout")
		}
		startOnce.Do(func() { close(siblingStarted) })
		select {
		case <-releaseSibling:
			output.(*tg.UploadFileBox).File = &tg.UploadFile{Bytes: []byte{1}}
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	home := newShieldedDCPool(innerHome, 2)
	router := newMigratingPool(home, func(context.Context, int) (telegram.CloseInvoker, error) {
		t.Fatal("unexpected target DC open")
		return nil, nil
	}, func(invoker tg.Invoker) tg.Invoker {
		return (retryMiddleware{max: 5}).Handle(invoker)
	})
	var homeOpens atomic.Int32
	router.openHome = func() (telegram.CloseInvoker, error) {
		homeOpens.Add(1)
		return newShieldedDCPool(innerHome, 2), nil
	}
	router.homeDC = func() int { return 5 }
	defer router.Close()

	loc := &tg.InputDocumentFileLocation{ID: 321, AccessHash: 654, FileReference: []byte{1}}
	siblingErrCh := make(chan error, 1)
	go func() {
		var out tg.UploadFileBox
		siblingErrCh <- router.Invoke(withDocumentDC(context.Background(), 5), &tg.UploadGetFileRequest{Location: loc, Offset: 1}, &out)
	}()
	<-siblingStarted

	slowCtx, cancelSlow := context.WithTimeout(withDocumentDC(context.Background(), 5), 25*time.Millisecond)
	defer cancelSlow()
	var slowOut tg.UploadFileBox
	slowErr := router.Invoke(slowCtx, &tg.UploadGetFileRequest{Location: loc, Offset: 0}, &slowOut)
	if !errors.Is(slowErr, context.DeadlineExceeded) {
		t.Fatalf("slow Invoke() = %v, want context.DeadlineExceeded", slowErr)
	}
	if _, ok := tgerr.As(slowErr); !ok {
		t.Fatalf("slow Invoke() = %v, want preserved tgerr.Error", slowErr)
	}
	if got := homeOpens.Load(); got != 0 {
		t.Fatalf("homeOpens = %d, want 0 (server -503 Timeout must not reset pool)", got)
	}

	close(releaseSibling)
	if err := <-siblingErrCh; err != nil {
		t.Fatalf("sibling Invoke() = %v, want nil (in-flight sibling must not fail with pool closed)", err)
	}
}

func TestMigratingPoolReplaysSiblingCallAcrossHomeReset(t *testing.T) {
	siblingStarted := make(chan struct{})
	var startOnce sync.Once
	firstInner := &testDCPool{InvokeFunc: func(ctx context.Context, input bin.Encoder, _ bin.Decoder) error {
		req := input.(*tg.UploadGetFileRequest)
		if req.Offset == 0 {
			return context.DeadlineExceeded
		}
		startOnce.Do(func() { close(siblingStarted) })
		<-ctx.Done()
		return ctx.Err()
	}}
	secondInner := &testDCPool{InvokeFunc: func(_ context.Context, _ bin.Encoder, output bin.Decoder) error {
		output.(*tg.UploadFileBox).File = &tg.UploadFile{Bytes: []byte{42}}
		return nil
	}}
	firstHome := newShieldedDCPool(firstInner, 2)
	router := newMigratingPool(firstHome, func(context.Context, int) (telegram.CloseInvoker, error) {
		t.Fatal("unexpected target DC open")
		return nil, nil
	}, unwrappedPool)
	router.openHome = func() (telegram.CloseInvoker, error) {
		return newShieldedDCPool(secondInner, 2), nil
	}
	router.homeDC = func() int { return 5 }
	defer router.Close()

	loc := &tg.InputDocumentFileLocation{ID: 999, AccessHash: 111, FileReference: []byte{2}}
	siblingErrCh := make(chan error, 1)
	var siblingOut tg.UploadFileBox
	go func() {
		siblingErrCh <- router.Invoke(withDocumentDC(context.Background(), 5), &tg.UploadGetFileRequest{Location: loc, Offset: 1}, &siblingOut)
	}()
	<-siblingStarted

	var timedOut tg.UploadFileBox
	if err := router.Invoke(withDocumentDC(context.Background(), 5), &tg.UploadGetFileRequest{Location: loc, Offset: 0}, &timedOut); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timed-out Invoke() = %v, want context.DeadlineExceeded", err)
	}
	if err := <-siblingErrCh; err != nil {
		t.Fatalf("sibling Invoke() = %v, want replay on replacement pool without error", err)
	}
	if got := siblingOut.File.(*tg.UploadFile).Bytes; len(got) != 1 || got[0] != 42 {
		t.Fatalf("sibling payload = %v, want [42]", got)
	}
}

func TestShieldedDCPoolSemaphoreQueueTimeoutIsNotPoolTimeout(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	inner := &testDCPool{InvokeFunc: func(_ context.Context, _ bin.Encoder, _ bin.Decoder) error {
		close(started)
		<-release
		return nil
	}}
	pool := newShieldedDCPool(inner, 1)
	defer pool.Close()

	done := make(chan error, 1)
	go func() { done <- pool.Invoke(context.Background(), nil, nil) }()
	<-started

	waitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := pool.Invoke(waitCtx, nil, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queued waiter Invoke() = %v, want context.DeadlineExceeded", err)
	}
	if isPoolTimeout(err) {
		t.Fatalf("isPoolTimeout(%v) = true, want false for semaphore queue timeout", err)
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("active Invoke() = %v", err)
	}
}

func TestMigratingPoolReplaysSiblingCallAcrossTargetEviction(t *testing.T) {
	siblingStarted := make(chan struct{})
	var startOnce sync.Once
	firstTargetInner := &testDCPool{InvokeFunc: func(ctx context.Context, input bin.Encoder, _ bin.Decoder) error {
		req := input.(*tg.UploadGetFileRequest)
		if req.Offset == 0 {
			return context.DeadlineExceeded
		}
		startOnce.Do(func() { close(siblingStarted) })
		<-ctx.Done()
		return ctx.Err()
	}}
	secondTargetInner := &testDCPool{InvokeFunc: func(_ context.Context, _ bin.Encoder, output bin.Decoder) error {
		output.(*tg.UploadFileBox).File = &tg.UploadFile{Bytes: []byte{77}}
		return nil
	}}
	var opens atomic.Int32
	router := newMigratingPool(&testDCPool{InvokeFunc: func(context.Context, bin.Encoder, bin.Decoder) error {
		t.Fatal("unexpected home call")
		return nil
	}}, func(_ context.Context, dc int) (telegram.CloseInvoker, error) {
		if dc != 4 {
			return nil, fmt.Errorf("unexpected target DC %d", dc)
		}
		if opens.Add(1) == 1 {
			return newShieldedDCPool(firstTargetInner, 2), nil
		}
		return newShieldedDCPool(secondTargetInner, 2), nil
	}, unwrappedPool)
	router.homeDC = func() int { return 5 }
	defer router.Close()

	loc := &tg.InputDocumentFileLocation{ID: 808, AccessHash: 909, FileReference: []byte{3}}
	siblingErrCh := make(chan error, 1)
	var siblingOut tg.UploadFileBox
	go func() {
		siblingErrCh <- router.Invoke(withDocumentDC(context.Background(), 4), &tg.UploadGetFileRequest{Location: loc, Offset: 1}, &siblingOut)
	}()
	<-siblingStarted

	var timedOut tg.UploadFileBox
	if err := router.Invoke(withDocumentDC(context.Background(), 4), &tg.UploadGetFileRequest{Location: loc, Offset: 0}, &timedOut); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timed-out Invoke() = %v, want context.DeadlineExceeded", err)
	}
	if err := <-siblingErrCh; err != nil {
		t.Fatalf("sibling target Invoke() = %v, want replay on reopened target pool", err)
	}
	if got := siblingOut.File.(*tg.UploadFile).Bytes; len(got) != 1 || got[0] != 77 {
		t.Fatalf("sibling target payload = %v, want [77]", got)
	}
}
