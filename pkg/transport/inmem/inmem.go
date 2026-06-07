// Package inmem provides an in-process Transport driver. It is intended for
// unit tests, examples, and single-binary deployments. It is not safe for
// cross-process communication.
package inmem

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"

	"github.com/ModulationAI/openagentio/pkg/transport"
)

// Driver is an in-memory pub/sub broker.
type Driver struct {
	mu     sync.RWMutex
	subs   map[string][]*subscription
	rr     map[string]*uint64 // queue group round-robin counters, keyed by "subject\x00queue"
	closed bool
}

type subscription struct {
	driver  *Driver
	subject string
	queue   string
	handler transport.Handler
}

// New constructs an empty in-memory driver.
func New() *Driver {
	return &Driver{
		subs: make(map[string][]*subscription),
		rr:   make(map[string]*uint64),
	}
}

func (*Driver) Connect(context.Context) error { return nil }

func (d *Driver) Close() error {
	d.mu.Lock()
	d.closed = true
	d.subs = nil
	d.rr = nil
	d.mu.Unlock()
	return nil
}

func (*Driver) Capabilities() transport.Capabilities {
	return transport.Capabilities{
		Streaming:   true,
		Persistence: false,
		QueueGroup:  true,
		Headers:     true,
	}
}

func (d *Driver) Publish(ctx context.Context, msg *transport.RawMessage) error {
	if msg == nil {
		return errors.New("inmem: nil message")
	}

	d.mu.RLock()
	if d.closed {
		d.mu.RUnlock()
		return errors.New("inmem: driver closed")
	}
	subs := append([]*subscription(nil), d.subs[msg.Subject]...)
	d.mu.RUnlock()

	// Group by queue: empty queue == fan-out; non-empty queue == round-robin.
	var fanout []*subscription
	groups := map[string][]*subscription{}
	for _, s := range subs {
		if s.queue == "" {
			fanout = append(fanout, s)
		} else {
			groups[s.queue] = append(groups[s.queue], s)
		}
	}

	for _, s := range fanout {
		if err := s.handler(ctx, msg); err != nil {
			return err
		}
	}
	for q, members := range groups {
		idx := d.nextRR(msg.Subject, q, uint64(len(members)))
		if err := members[idx].handler(ctx, msg); err != nil {
			return err
		}
	}
	return nil
}

func (d *Driver) nextRR(subject, queue string, n uint64) uint64 {
	if n == 0 {
		return 0
	}
	key := subject + "\x00" + queue
	d.mu.Lock()
	c, ok := d.rr[key]
	if !ok {
		c = new(uint64)
		d.rr[key] = c
	}
	d.mu.Unlock()
	v := atomic.AddUint64(c, 1) - 1
	return v % n
}

func (d *Driver) Subscribe(_ context.Context, subject, queue string, h transport.Handler) (transport.Subscription, error) {
	if h == nil {
		return nil, errors.New("inmem: nil handler")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil, errors.New("inmem: driver closed")
	}
	s := &subscription{driver: d, subject: subject, queue: queue, handler: h}
	d.subs[subject] = append(d.subs[subject], s)
	return s, nil
}

func (s *subscription) Unsubscribe() error {
	d := s.driver
	d.mu.Lock()
	defer d.mu.Unlock()
	list := d.subs[s.subject]
	for i, sub := range list {
		if sub == s {
			d.subs[s.subject] = append(list[:i], list[i+1:]...)
			return nil
		}
	}
	return nil
}

func (d *Driver) Request(ctx context.Context, msg *transport.RawMessage) (*transport.RawMessage, error) {
	// Reserved for v0.1 bus implementation; the bus layer wraps OpenInbox to
	// cover both request/reply and streaming.
	return nil, transport.ErrNotImplemented
}

func (d *Driver) OpenInbox(ctx context.Context) (transport.Inbox, error) {
	subject := "_INBOX.inmem." + uuid.NewString()
	box := &inbox{
		subject: subject,
		ch:      make(chan *transport.RawMessage, 64),
		done:    make(chan struct{}),
	}
	sub, err := d.Subscribe(ctx, subject, "", func(ctx context.Context, m *transport.RawMessage) error {
		select {
		case box.ch <- m:
			return nil
		case <-box.done:
			return errors.New("inmem inbox: closed")
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	if err != nil {
		return nil, err
	}
	box.sub = sub
	return box, nil
}

type inbox struct {
	subject string
	ch      chan *transport.RawMessage
	done    chan struct{}
	sub     transport.Subscription
	once    sync.Once
}

func (i *inbox) Subject() string { return i.subject }

func (i *inbox) Recv(ctx context.Context) (*transport.RawMessage, error) {
	select {
	case m := <-i.ch:
		return m, nil
	case <-i.done:
		return nil, errors.New("inmem inbox: closed")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (i *inbox) Close() error {
	var err error
	i.once.Do(func() {
		close(i.done)
		if i.sub != nil {
			err = i.sub.Unsubscribe()
		}
	})
	return err
}

// Compile-time interface check.
var _ transport.Transport = (*Driver)(nil)
