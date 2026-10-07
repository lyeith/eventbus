package messaging

import (
	"context"
	"fmt"
	"regexp"
)

// FirehoseDelivery is owned by SNS, the producer. App composition supplies a
// local Firehose core; messaging imports no destination service or store.
type FirehoseDelivery interface {
	PutFirehoseRecord(context.Context, string, []byte) (string, error)
}

func (broker *Broker) SetFirehoseDelivery(delivery FirehoseDelivery) {
	broker.mu.Lock()
	defer broker.mu.Unlock()
	broker.firehose = delivery
}
func (broker *Broker) firehoseDelivery() FirehoseDelivery {
	broker.mu.RLock()
	defer broker.mu.RUnlock()
	return broker.firehose
}

var snsFirehoseNameRE = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)
var snsFirehoseRoleRE = regexp.MustCompile(`^arn:aws(?:-[a-z]+)?:iam::[0-9]{12}:role/[a-zA-Z_0-9+=,.@\-_/]+$`)

func (broker *Broker) validateFirehoseEndpoint(endpoint string) error {
	prefix := fmt.Sprintf("arn:aws:firehose:%s:%s:deliverystream/", broker.region, broker.accountID)
	if len(endpoint) <= len(prefix) || endpoint[:len(prefix)] != prefix || !snsFirehoseNameRE.MatchString(endpoint[len(prefix):]) {
		return snsInvalid("Firehose endpoint must identify a local delivery stream in this topic's region and account")
	}
	return nil
}
