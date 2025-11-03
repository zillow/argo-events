/*
Copyright 2018 BlackRock, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package awssqs

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	sqslib "github.com/aws/aws-sdk-go/service/sqs"
	"go.uber.org/zap"

	"github.com/argoproj/argo-events/common"
	"github.com/argoproj/argo-events/common/logging"
	eventbuscommon "github.com/argoproj/argo-events/eventbus/common"
	eventsource "github.com/argoproj/argo-events/eventbus/jetstream/eventsource"
	eventsourcecommon "github.com/argoproj/argo-events/eventsources/common"
	awscommon "github.com/argoproj/argo-events/eventsources/common/aws"
	"github.com/argoproj/argo-events/eventsources/sources"
	metrics "github.com/argoproj/argo-events/metrics"
	apicommon "github.com/argoproj/argo-events/pkg/apis/common"
	"github.com/argoproj/argo-events/pkg/apis/events"
	"github.com/argoproj/argo-events/pkg/apis/eventsource/v1alpha1"
)

// EventListener implements Eventing for aws sqs event source
type EventListener struct {
	EventSourceName string
	EventName       string
	SQSEventSource  v1alpha1.SQSEventSource
	Metrics         *metrics.Metrics
	// EventBus connection for capacity checking (set during initialization)
	// Stores reference to the connection, which is automatically updated on reconnection
	EventBusConn eventbuscommon.EventSourceConnection
}

// GetEventSourceName returns name of event source
func (el *EventListener) GetEventSourceName() string {
	return el.EventSourceName
}

// GetEventName returns name of event
func (el *EventListener) GetEventName() string {
	return el.EventName
}

// GetEventSourceType return type of event server
func (el *EventListener) GetEventSourceType() apicommon.EventSourceType {
	return apicommon.SQSEvent
}

// The connection reference is automatically updated on reconnection,
// ensuring capacity checks always use the current active connection
func (el *EventListener) SetEventBusConnection(conn eventbuscommon.EventSourceConnection) {
	el.EventBusConn = conn
}

// isEventBusFull checks if the event bus is at capacity or unavailable
func (el *EventListener) isEventBusFull(log *zap.SugaredLogger) bool {
	// Check if connection is available and active
	if el.EventBusConn == nil || el.EventBusConn.IsClosed() {
		log.Warnw("EventBus connection not available or closed, treating as unavailable to prevent message loss",
			zap.String("eventSource", el.GetEventSourceName()),
			zap.String("eventName", el.GetEventName()))
		return true
	}

	jetstreamConn := el.EventBusConn.(*eventsource.JetstreamSourceConn)

	if jetstreamConn.JSContext == nil {
		log.Warnw("JetStream context not initialized, treating as unavailable to prevent message loss",
			zap.String("eventSource", el.GetEventSourceName()),
			zap.String("eventName", el.GetEventName()))
		return true
	}

	// Use the current connection's JSContext (automatically updated on reconnection)
	streamInfo, err := jetstreamConn.JSContext.StreamInfo(common.JetStreamStreamName)
	if err != nil {
		// If we can't check capacity, treat as unavailable to prevent message loss
		log.Warnw("Failed to get stream info, treating as unavailable to prevent message loss",
			zap.String("eventSource", el.GetEventSourceName()),
			zap.String("eventName", el.GetEventName()),
			zap.Error(err))
		return true
	}

	// Check if stream is at capacity based on MaxMsgs
	if streamInfo.Config.MaxMsgs > 0 && streamInfo.State.Msgs >= uint64(streamInfo.Config.MaxMsgs) {
		log.Infow("EventBus at capacity - MaxMsgs limit reached",
			zap.String("eventSource", el.GetEventSourceName()),
			zap.String("eventName", el.GetEventName()),
			zap.Uint64("currentMsgs", streamInfo.State.Msgs),
			zap.Int64("maxMsgs", streamInfo.Config.MaxMsgs))
		return true
	}

	// Event bus has available capacity
	log.Debugw("EventBus has available capacity, proceeding with SQS poll",
		zap.String("eventSource", el.GetEventSourceName()),
		zap.String("eventName", el.GetEventName()),
		zap.Uint64("currentMsgs", streamInfo.State.Msgs),
		zap.Int64("maxMsgs", streamInfo.Config.MaxMsgs),
		zap.Uint64("currentBytes", streamInfo.State.Bytes),
		zap.Int64("maxBytes", streamInfo.Config.MaxBytes))

	return false
}

// StartListening starts listening events
func (el *EventListener) StartListening(ctx context.Context, dispatch func([]byte, ...eventsourcecommon.Option) error) error {
	log := logging.FromContext(ctx).
		With(logging.LabelEventSourceType, el.GetEventSourceType(), logging.LabelEventName, el.GetEventName())
	log.Info("started processing the AWS SQS event source...")
	defer sources.Recover(el.GetEventName())

	sqsEventSource := &el.SQSEventSource
	sqsClient, err := el.createSqsClient()
	if err != nil {
		return err
	}

	log.Info("fetching queue url...")
	getQueueURLInput := &sqslib.GetQueueUrlInput{
		QueueName: &sqsEventSource.Queue,
	}
	if sqsEventSource.QueueAccountID != "" {
		getQueueURLInput = getQueueURLInput.SetQueueOwnerAWSAccountId(sqsEventSource.QueueAccountID)
	}

	queueURL, err := sqsClient.GetQueueUrl(getQueueURLInput)
	if err != nil {
		log.Errorw("Error getting SQS Queue URL", zap.Error(err))
		return fmt.Errorf("failed to get the queue url for %s, %w", el.GetEventName(), err)
	}

	if sqsEventSource.JSONBody {
		log.Info("assuming all events have a json body...")
	}

	log.Info("listening for messages on the queue...")

	// Log capacity-based polling control status
	if sqsEventSource.SkipPollingWhenEventBusFull {
		log.Info("Capacity-based polling control enabled - will check event bus capacity before each poll")
	}

	var eventBusFullStartTime *time.Time

	for {
		select {
		case <-ctx.Done():
			log.Info("exiting SQS event listener...")
			return nil
		default:
		}

		// Check event bus capacity before polling if enabled
		if sqsEventSource.SkipPollingWhenEventBusFull {
			// Verify we have a JetStream connection before checking capacity
			if el.EventBusConn != nil && !el.EventBusConn.IsClosed() {
				if _, ok := el.EventBusConn.(*eventsource.JetstreamSourceConn); ok {
					if el.isEventBusFull(log) {
						// Track when EventBus becomes full
						if eventBusFullStartTime == nil {
							now := time.Now()
							eventBusFullStartTime = &now
							el.Metrics.SetEventBusFull(el.GetEventSourceName(), el.GetEventName(), true)
							log.Infow("EventBus is full, skipping SQS poll",
								zap.String("eventSource", el.GetEventSourceName()),
								zap.String("eventName", el.GetEventName()))
						}

						time.Sleep(10 * time.Second) // Wait before checking capacity again
						continue
					}

					// EventBus capacity available - record duration if it was full
					if eventBusFullStartTime != nil {
						duration := time.Since(*eventBusFullStartTime)
						el.Metrics.EventBusFullDuration(el.GetEventSourceName(), el.GetEventName(), duration.Seconds())
						el.Metrics.SetEventBusFull(el.GetEventSourceName(), el.GetEventName(), false)
						log.Infow("EventBus capacity available again, resuming SQS polling",
							zap.String("eventSource", el.GetEventSourceName()),
							zap.String("eventName", el.GetEventName()),
							zap.Duration("wasFull", duration))
						eventBusFullStartTime = nil
					}
				}
			}
		}

		messages, err := fetchMessages(ctx, sqsClient, *queueURL.QueueUrl, 10, sqsEventSource.WaitTimeSeconds)
		if err != nil {
			log.Errorw("failed to get messages from SQS", zap.Error(err))
			awsError, ok := err.(awserr.Error)
			if ok && awsError.Code() == "ExpiredToken" && el.SQSEventSource.SessionToken != nil {
				log.Info("credentials expired, reading credentials again")
				newSqsClient, err := el.createSqsClient()
				if err != nil {
					log.Errorw("Error creating SQS client", zap.Error(err))
				} else if newSqsClient != nil {
					sqsClient = newSqsClient
				}
			}

			time.Sleep(2 * time.Second)
			continue
		}
		for _, m := range messages {
			el.processMessage(m, dispatch, func() {
				_, err = sqsClient.DeleteMessage(&sqslib.DeleteMessageInput{
					QueueUrl:      queueURL.QueueUrl,
					ReceiptHandle: m.ReceiptHandle,
				})
				if err != nil {
					log.Errorw("Failed to delete message", zap.Error(err))
					awsError, ok := err.(awserr.Error)
					if ok && awsError.Code() == "ExpiredToken" && el.SQSEventSource.SessionToken != nil {
						log.Info("credentials expired, reading credentials again")
						newSqsClient, err := el.createSqsClient()
						if err != nil {
							log.Errorw("Error creating SQS client", zap.Error(err))
						} else if newSqsClient != nil {
							sqsClient = newSqsClient
						}
					}
				}
			}, log)
		}
	}
}

func (el *EventListener) processMessage(message *sqslib.Message, dispatch func([]byte, ...eventsourcecommon.Option) error, ack func(), log *zap.SugaredLogger) {
	defer func(start time.Time) {
		el.Metrics.EventProcessingDuration(el.GetEventSourceName(), el.GetEventName(), float64(time.Since(start)/time.Millisecond))
	}(time.Now())

	data := &events.SQSEventData{
		MessageId:         *message.MessageId,
		MessageAttributes: message.MessageAttributes,
		Metadata:          el.SQSEventSource.Metadata,
	}
	if el.SQSEventSource.JSONBody {
		body := []byte(*message.Body)
		data.Body = (*json.RawMessage)(&body)
	} else {
		data.Body = []byte(*message.Body)
	}
	eventBytes, err := json.Marshal(data)
	if err != nil {
		log.Errorw("failed to marshal event data, will process next message...", zap.Error(err))
		el.Metrics.EventProcessingFailed(el.GetEventSourceName(), el.GetEventName())
		// Don't ack if a DLQ is configured to allow to forward the message to the DLQ
		if !el.SQSEventSource.DLQ {
			ack()
		}
		return
	}
	if err = dispatch(eventBytes); err != nil {
		log.Errorw("failed to dispatch SQS event", zap.Error(err))
		el.Metrics.EventProcessingFailed(el.GetEventSourceName(), el.GetEventName())
	} else {
		ack()
	}
}

func fetchMessages(ctx context.Context, q *sqslib.SQS, url string, maxSize, waitSeconds int64) ([]*sqslib.Message, error) {
	if waitSeconds == 0 {
		// Defaults to 3 seconds
		waitSeconds = 3
	}
	result, err := q.ReceiveMessageWithContext(ctx, &sqslib.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqslib.MessageSystemAttributeNameSentTimestamp),
		},
		MessageAttributeNames: []*string{
			aws.String(sqslib.QueueAttributeNameAll),
		},
		QueueUrl:            &url,
		MaxNumberOfMessages: aws.Int64(maxSize),
		VisibilityTimeout:   aws.Int64(120), // 120 seconds
		WaitTimeSeconds:     aws.Int64(waitSeconds),
	})
	if err != nil {
		return nil, err
	}
	return result.Messages, nil
}

func (el *EventListener) createAWSSession() (*session.Session, error) {
	sqsEventSource := &el.SQSEventSource
	awsSession, err := awscommon.CreateAWSSessionWithCredsInVolume(sqsEventSource.Region, sqsEventSource.RoleARN, sqsEventSource.AccessKey, sqsEventSource.SecretKey, sqsEventSource.SessionToken)
	if err != nil {
		return nil, fmt.Errorf("failed to create aws session for %s, %w", el.GetEventName(), err)
	}
	return awsSession, nil
}

func (el *EventListener) createSqsClient() (*sqslib.SQS, error) {
	awsSession, err := el.createAWSSession()
	if err != nil {
		return nil, err
	}

	var sqsClient *sqslib.SQS
	if el.SQSEventSource.Endpoint == "" {
		sqsClient = sqslib.New(awsSession)
	} else {
		sqsClient = sqslib.New(awsSession, &aws.Config{Endpoint: &el.SQSEventSource.Endpoint, Region: &el.SQSEventSource.Region})
	}

	return sqsClient, nil
}
