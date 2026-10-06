// Package consumer owns the local execution harness: consumer configuration,
// polling, subprocess invocation and batch settlement. It depends on the queue
// behavior declared by QueueBroker, not HTTP routing or another service store.
package consumer
