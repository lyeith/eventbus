// Package messaging owns one in-memory broker for SNS topics, subscriptions,
// filters and SQS queues, including fanout and receipt visibility. Its HTTP
// adapters expose AWS Query and SQS JSON operations over that shared state.
// Other packages change queue state through Broker methods.
package messaging
