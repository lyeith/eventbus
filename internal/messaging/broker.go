package messaging

import (
	"context"
	"sync"
	"time"
)

const maxWaitTime = 20 * time.Second

const (
	defaultRegion            = "us-east-1"
	defaultAccountID         = "000000000000"
	defaultVisibilityTimeout = 30 * time.Second
	defaultRetentionPeriod   = 4 * 24 * time.Hour // 4 days
)

type Broker struct {
	topics     map[string]*Topic // ARN → Topic
	queues     map[string]*Queue // name → Queue
	arnIndex   map[string]*Queue // ARN → Queue
	mu         sync.RWMutex
	region     string
	accountID  string
	port       int
	sns        *snsState
	sqs        *sqsBrokerState
	capture    *SNSCapture
	firehose   FirehoseDelivery
	lambda     LambdaDelivery
	devCustody *devBrokerCustody
}

func NewBroker(region, accountID string, port int) *Broker {
	if region == "" {
		region = defaultRegion
	}
	if accountID == "" {
		accountID = defaultAccountID
	}
	return &Broker{
		topics:    make(map[string]*Topic),
		queues:    make(map[string]*Queue),
		arnIndex:  make(map[string]*Queue),
		region:    region,
		accountID: accountID,
		port:      port,
	}
}

// StartRequeueLoop runs a background goroutine that periodically requeues
// expired in-flight messages across all queues. Cancel the context to stop.
func (b *Broker) StartRequeueLoop(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				b.mu.RLock()
				queues := make([]*Queue, 0, len(b.queues))
				for _, q := range b.queues {
					queues = append(queues, q)
				}
				b.mu.RUnlock()

				b.advanceSQSMessageMoveTasks(time.Now())
				for _, q := range queues {
					b.RequeueExpired(q)
				}
			}
		}
	}()
	return done
}
