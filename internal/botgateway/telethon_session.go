package botgateway

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/gotd/td/session"

	"github.com/tgdrive/teldrive/v2/internal/telethonsession"

	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/secureblob"
)

const telethonSessionVersion byte = '1'

type botSessionStorage struct {
	mu      sync.Mutex
	queries *sqlcgen.Queries
	cipher  *secureblob.Cipher
	userID  int64
	botID   int64
	stored  []byte
	encoded string
}

func (s *botSessionStorage) HasSession() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.stored) > 0
}

func (s *botSessionStorage) LoadSession(ctx context.Context) ([]byte, error) {
	if s == nil {
		return nil, session.ErrNotFound
	}
	s.mu.Lock()
	if len(s.stored) == 0 {
		s.mu.Unlock()
		return nil, session.ErrNotFound
	}
	stored := append([]byte(nil), s.stored...)
	s.mu.Unlock()

	plain, err := s.cipher.Open("bot-session", stored)
	if err != nil {
		return nil, fmt.Errorf("decrypt bot session: %w", err)
	}
	encoded := string(plain)
	s.mu.Lock()
	if s.encoded == "" {
		s.encoded = encoded
	}
	s.mu.Unlock()
	return telethonsession.DecodeToGotd(ctx, encoded)
}

func (s *botSessionStorage) StoreSession(ctx context.Context, raw []byte) error {
	if s == nil || s.queries == nil || s.cipher == nil || s.userID <= 0 || s.botID <= 0 || len(raw) == 0 {
		return errors.New("invalid bot session storage")
	}
	encoded, err := telethonsession.EncodeGotd(ctx, raw)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if encoded == s.encoded && len(s.stored) > 0 {
		return nil
	}
	ciphertext, err := s.cipher.Seal("bot-session", []byte(encoded))
	if err != nil {
		return fmt.Errorf("encrypt bot session: %w", err)
	}
	count, err := s.queries.UpdateBotSession(ctx, sqlcgen.UpdateBotSessionParams{
		Session: ciphertext,
		UserID:  s.userID,
		BotID:   s.botID,
	})
	if err != nil {
		return fmt.Errorf("persist bot session: %w", err)
	}
	if count == 0 {
		return errors.New("bot session row no longer exists")
	}
	s.stored = append([]byte(nil), ciphertext...)
	s.encoded = encoded
	return nil
}
