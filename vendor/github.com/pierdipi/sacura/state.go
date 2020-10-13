package sacura

import (
	"sync"

	"github.com/google/go-cmp/cmp"
	"k8s.io/apimachinery/pkg/util/sets"
)

type StateManager struct {
	received   sets.String
	receivedMu sync.Mutex

	sent   sets.String
	sentMu sync.RWMutex
}

func NewStateManager() *StateManager {
	return &StateManager{
		received: sets.NewString(),
		sent:     sets.NewString(),
	}
}

func (s *StateManager) ReadSent(sent <-chan string) {
	go func(set *StateManager) {
		for id := range sent {
			set.insertSent(id)
		}
	}(s)
}

func (s *StateManager) ReadReceived(received <-chan string) {
	go func(set *StateManager) {
		for id := range received {
			set.insertReceived(id)
		}
	}(s)
}

func (s *StateManager) insertSent(id string) {
	s.sentMu.Lock()
	defer s.sentMu.Unlock()

	s.sent.Insert(id)
}

func (s *StateManager) insertReceived(id string) {

	s.sentMu.RLock()
	isSent := s.sent.Has(id)
	s.sentMu.RUnlock()
	if isSent {

		// The received message is in the sent set so we can remove from the sent set
		// and not add it the the received set.
		s.sentMu.Lock()
		s.sent.Delete(id)
		s.sentMu.Unlock()

		return
	}

	s.receivedMu.Lock()
	defer s.receivedMu.Unlock()

	s.received.Insert(id)
}

func (s *StateManager) Lock() {
	s.receivedMu.Lock()
	s.sentMu.Lock()
}

func (s *StateManager) Unlock() {
	s.receivedMu.Unlock()
	s.sentMu.Unlock()
}

func (s *StateManager) Diff() string {
	s.Lock()
	defer s.Unlock()

	return cmp.Diff(s.received.List(), s.sent.List())
}
