package messaging

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/lyeith/eventbus/internal/devactivity"
	"github.com/lyeith/eventbus/internal/sqsevent"
)

// Development custody accounts for accepted native messages while an enabled
// Lambda mapping can execute them. It owns no receive, retry or redrive policy.
// Broker.mu protects construction; Queue.mu protects registrations and leases.
type devBrokerCustody struct {
	activity devactivity.Activity
	created  bool
}

type devQueueCustody struct {
	activity      devactivity.Activity
	registrations int
	messages      map[string]func(error)
}

// SetDevActivity configures an optional observer before any queue exists. The
// observer must not call back into the broker, including from completion.
func (b *Broker) SetDevActivity(activity devactivity.Activity) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.devCustody != nil && (b.devCustody.created || b.devCustody.activity != nil) {
		return errors.New("SQS development activity must be configured before queue creation")
	}
	b.devCustody = &devBrokerCustody{activity: activity}
	return nil
}

func newDevQueueCustodyLocked(b *Broker) *devQueueCustody {
	if b.devCustody == nil {
		b.devCustody = &devBrokerCustody{}
	}
	b.devCustody.created = true
	if b.devCustody.activity == nil {
		return nil
	}
	return &devQueueCustody{activity: b.devCustody.activity, messages: make(map[string]func(error))}
}

// RegisterSQSLambdaCustody binds a mapping to this exact queue instance. The
// first mapping adopts queued and in-flight messages before publishing its
// registration. A partial observer failure leaves native queue state untouched.
// Release only after every mapping worker joins; the final release leaves all
// remaining native messages paused, with no autonomous mapping able to run them.
func (b *Broker) RegisterSQSLambdaCustody(q *Queue) (func(), error) {
	if _, err := b.QueueInfo(q); err != nil {
		return nil, err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.deleted {
		return nil, ErrQueueUnavailable
	}
	owner := q.devCustody
	if owner == nil {
		return func() {}, nil
	}
	if owner.registrations == 0 {
		leases := make(map[string]func(error), len(q.messages)+len(q.inFlight))
		adopt := func(message *Message) error {
			if _, exists := leases[message.ID]; exists {
				return nil
			}
			complete, err := beginDevSQSMessage(owner, message.ID)
			if err != nil {
				return err
			}
			leases[message.ID] = complete
			return nil
		}
		var err error
		for _, message := range q.messages {
			if err = adopt(message); err != nil {
				break
			}
		}
		if err == nil {
			for _, message := range q.inFlight {
				if err = adopt(message); err != nil {
					break
				}
			}
		}
		if err != nil {
			for _, complete := range leases {
				complete(nil)
			}
			return nil, err
		}
		owner.messages = leases
	}
	owner.registrations++
	notifyQueueLocked(q)
	var once sync.Once
	return func() {
		once.Do(func() {
			q.mu.Lock()
			defer q.mu.Unlock()
			owner.registrations--
			if owner.registrations == 0 {
				releaseAllDevSQSCustodyLocked(q)
			}
			notifyQueueLocked(q)
		})
	}, nil
}

// SQSLambdaCustodyState captures custody and its wake signal under the native
// queue lock. Delays and invisible leases remain pending until native settlement
// or removal; a queue with the same ARN after recreation is a different owner.
func (b *Broker) SQSLambdaCustodyState(q *Queue) (bool, <-chan struct{}, error) {
	if _, err := b.QueueInfo(q); err != nil {
		return false, nil, err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.deleted {
		return false, nil, ErrQueueUnavailable
	}
	return q.devCustody != nil && len(q.devCustody.messages) != 0, q.notify, nil
}

// ReceiveOwnedSQSLambdaEventContext uses the canonical native receive and JSON
// byte admission. Only retained message IDs can gain a new receipt. Empty
// custody ends a continuation long poll immediately, including after peer ack.
func (b *Broker) ReceiveOwnedSQSLambdaEventContext(ctx context.Context, q *Queue, max int, wait time.Duration, budget int) (sqsevent.Event, error) {
	return b.receiveSQSLambdaEventContext(ctx, q, max, wait, budget, true)
}

func beginDevSQSMessage(owner *devQueueCustody, id string) (func(error), error) {
	complete, err := owner.activity.BeginActivity("sqs_message", id)
	if err != nil {
		return nil, err
	}
	if complete == nil {
		return nil, errors.New("development SQS activity returned no completion")
	}
	return complete, nil
}

// The native caller must invoke this after validation and before mutating queue
// data. A mapped destination acquires ownership before the source releases it.
func admitDevSQSMessageLocked(q *Queue, id string) error {
	owner := q.devCustody
	if owner == nil || owner.registrations == 0 {
		return nil
	}
	if _, exists := owner.messages[id]; exists {
		return nil
	}
	complete, err := beginDevSQSMessage(owner, id)
	if err != nil {
		return newSQSError("ServiceUnavailable", "Development SQS ownership admission is unavailable")
	}
	owner.messages[id] = complete
	return nil
}

func releaseDevSQSMessageLocked(q *Queue, id string) {
	if q.devCustody == nil {
		return
	}
	if complete, exists := q.devCustody.messages[id]; exists {
		delete(q.devCustody.messages, id)
		complete(nil)
	}
}

func releaseAllDevSQSCustodyLocked(q *Queue) {
	if q.devCustody == nil {
		return
	}
	for id := range q.devCustody.messages {
		releaseDevSQSMessageLocked(q, id)
	}
}

func hasDevSQSCustodyLocked(q *Queue) bool {
	return q.devCustody == nil || len(q.devCustody.messages) != 0
}

func ownsDevSQSMessageLocked(q *Queue, id string) bool {
	if q.devCustody == nil {
		return true // Ordinary mode retains its existing native receive behavior.
	}
	_, owned := q.devCustody.messages[id]
	return owned
}
