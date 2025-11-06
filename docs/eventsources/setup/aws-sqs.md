# AWS SQS

SQS event-source listens to messages on AWS SQS queue and helps sensor trigger workloads.

## Event Structure

The structure of an event dispatched by the event-source over the eventbus looks like following,

            {
                "context": {
                  "type": "type_of_event_source",
                  "specversion": "cloud_events_version",
                  "source": "name_of_the_event_source",
                  "id": "unique_event_id",
                  "time": "event_time",
                  "datacontenttype": "type_of_data",
                  "subject": "name_of_the_configuration_within_event_source"
                },
                "data": {
                 "messageId": "message id",
                 // Each message attribute consists of a Name, Type, and Value. For more information,
                 // see Amazon SQS Message Attributes
                 // (https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-attributes.html)
                 // in the Amazon Simple Queue Service Developer Guide.
                 "messageAttributes": "message attributes",
                   "body": "Body is the message data",
                }
            }

<br/>

## Setup

1. Create a queue called `test` either using aws cli or AWS SQS management console.

1. Fetch your access and secret key for AWS account and base64 encode them.

1. Create a secret called `aws-secret` as follows.

        apiVersion: v1
        kind: Secret
        metadata:
          name: aws-secret
        type: Opaque
        data:
          accesskey: <base64-access-key>
          secretkey: <base64-secret-key>

1. Deploy the secret.

        kubectl -n argo-events apply -f aws-secret.yaml

1. Create the event source by running the following command.

        kubectl apply -n argo-events -f https://raw.githubusercontent.com/argoproj/argo-events/stable/examples/event-sources/aws-sqs.yaml

1. Inspect the event-source pod logs to make sure it was able to subscribe to the queue specified in the event source to consume messages.

1. Create the sensor by running the following command.

        kubectl apply -n argo-events -f https://raw.githubusercontent.com/argoproj/argo-events/stable/examples/sensors/aws-sqs.yaml

1. Dispatch a message on sqs queue.

        aws sqs send-message --queue-url https://sqs.us-east-1.amazonaws.com/XXXXX/test --message-body '{"message": "hello"}'

1. Once a message is published, an argo workflow will be triggered. Run `argo list` to find the workflow.

## Configuration Options

### Capacity-Based Polling Control

The `skipPollingWhenEventBusFull` option enables intelligent backpressure handling to prevent message loss when the event bus is at capacity.

**How it works:**

- When enabled, the event source checks the JetStream event bus capacity before each SQS poll
- If the event bus is full or unavailable, SQS polling is skipped
- Messages remain in SQS with their visibility timeout intact
- Polling automatically resumes when event bus capacity becomes available
- Prevents SQS messages from being moved to DLQ due to event bus capacity issues

**Configuration:**

```yaml
apiVersion: argoproj.io/v1alpha1
kind: EventSource
metadata:
  name: aws-sqs
spec:
  sqs:
    example:
      # ... other configuration ...

      # Skip polling SQS when event bus is at capacity
      # Recommended: true (default: false)
      skipPollingWhenEventBusFull: true

      # Wait duration (in seconds) between EventBus capacity checks when full
      # Default: 10 seconds
      # Optional: Configure to adjust check frequency based on your needs
      eventBusFullWaitSeconds: 10

      # SQS batch size: number of messages to fetch per ReceiveMessage call
      # Valid values: 1 to 10. Default: 10
      # Optional
      batchSize: 10
```

**Benefits:**

- **Prevents message loss**: Messages stay in SQS when the event bus can't accept them
- **Reduces API calls**: Avoids unnecessary SQS polling when downstream is saturated
- **Self-healing**: Automatically resumes when capacity is available

**Recommendations:**

- Enable this option when using JetStream event bus with limited capacity

## Troubleshoot

Please read the [FAQ](https://argoproj.github.io/argo-events/FAQ/).
