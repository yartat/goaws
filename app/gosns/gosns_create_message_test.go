package gosns

import (
	"encoding/json"
	"testing"

	"github.com/p4tin/goaws/app"
	"github.com/stretchr/testify/assert"
)

const (
	messageKey            = "Message"
	subjectKey            = "Subject"
	messageStructureJSON  = "json"
	messageStructureEmpty = ""
)

// When simple message string is passed,
// it must be used for all subscribers (no matter the protocol)
func TestCreateMessageBody_NonJson(t *testing.T) {
	// Arrange
	assert := assert.New(t)
	const message = "message text"
	const subject = "subject"
	subs := &app.Subscription{
		Protocol:        "sqs",
		TopicArn:        "topic-arn",
		SubscriptionArn: "subs-arn",
		Raw:             false,
	}

	// Act
	snsMessage, err := CreateMessageBody(subs, message, subject, messageStructureEmpty, make(map[string]app.MessageAttributeValue))

	// Asserts
	assert.NoError(err)
	var unmarshalled map[string]interface{}
	err = json.Unmarshal(snsMessage, &unmarshalled)
	assert.NoError(err)
	receivedMessage, ok := unmarshalled[messageKey]
	assert.Truef(ok, `SNS message "%s" does not contain key "%s"`, snsMessage, messageKey)
	assert.Equal(message, receivedMessage)
	receivedSubject, ok := unmarshalled[subjectKey]
	assert.Truef(ok, `SNS message "%s" does not contain key "%s"`, snsMessage, subjectKey)
	assert.Equal(subject, receivedSubject)
}

// When no protocol specific message is passed,
// default message must be forwarded
func TestCreateMessageBody_OnlyDefaultValueInJson(t *testing.T) {
	// Arrange
	assert := assert.New(t)
	subs := &app.Subscription{
		Protocol:        "sqs",
		TopicArn:        "topic-arn",
		SubscriptionArn: "subs-arn",
		Raw:             false,
	}
	const message = `{"default": "default message text", "http": "HTTP message text"}`
	const subject = "subject"

	// Act
	snsMessage, err := CreateMessageBody(subs, message, subject, messageStructureJSON, nil)

	// Asserts
	assert.NoError(err)
	var unmarshalled map[string]interface{}
	err = json.Unmarshal(snsMessage, &unmarshalled)
	assert.NoError(err)
	receivedMessage, ok := unmarshalled[messageKey]
	assert.Truef(ok, `SNS message "%s" does not contain key "%s"`, snsMessage, messageKey)
	const expected = "default message text"
	assert.Equal(expected, receivedMessage)
	receivedSubject, ok := unmarshalled[subjectKey]
	assert.Truef(ok, `SNS message "%s" does not contain key "%s"`, snsMessage, subjectKey)
	assert.Equal(subject, receivedSubject)
}

// When only protocol specific message is passed,
// error must be returned
func TestCreateMessageBody_OnlySqsValueInJson(t *testing.T) {
	// Arrange
	assert := assert.New(t)
	subs := &app.Subscription{
		Protocol:        "sqs",
		TopicArn:        "topic-arn",
		SubscriptionArn: "subs-arn",
		Raw:             false,
	}
	const message = `{"sqs": "message text"}`
	const subject = "subject"

	// Act
	snsMessage, err := CreateMessageBody(subs, message, subject, messageStructureJSON, nil)

	// Asserts
	assert.Errorf(err, `error expected but instead SNS message was returned: %s`, snsMessage)
}

// when both default and protocol specific messages are passed,
// protocol specific message must be used
func TestCreateMessageBody_BothDefaultAndSqsValuesInJson(t *testing.T) {
	// Arrange
	assert := assert.New(t)
	subs := &app.Subscription{
		Protocol:        "sqs",
		TopicArn:        "topic-arn",
		SubscriptionArn: "subs-arn",
		Raw:             false,
	}
	const message = `{"default": "default message text", "sqs": "sqs message text"}`
	const subject = "subject"

	// Act
	snsMessage, err := CreateMessageBody(subs, message, subject, messageStructureJSON, nil)

	// Asserts
	assert.NoErrorf(err, `error creating SNS message`)
	var unmarshalled map[string]interface{}
	err = json.Unmarshal(snsMessage, &unmarshalled)
	assert.NoErrorf(err, `error unmarshalling SNS message "%s"`, snsMessage)
	receivedMessage, ok := unmarshalled[messageKey]
	assert.Truef(ok, `SNS message "%s" does not contain key "%s"`, snsMessage, messageKey)
	const expected = "sqs message text"
	assert.Equal(expected, receivedMessage)
	receivedSubject, ok := unmarshalled[subjectKey]
	assert.Truef(ok, `SNS message "%s" does not contain key "%s"`, snsMessage, subjectKey)
	assert.Equal(subject, receivedSubject)
}

// When simple message string is passed,
// it must be used as is (even if it contains JSON)
func TestCreateMessageBody_NonJsonContainingJson(t *testing.T) {
	// Arrange
	assert := assert.New(t)
	subs := &app.Subscription{
		Protocol:        "sns",
		TopicArn:        "topic-arn",
		SubscriptionArn: "subs-arn",
		Raw:             false,
	}
	const message = `{"default": "default message text", "sqs": "sqs message text"}`
	const subject = "subject"

	// Act
	snsMessage, err := CreateMessageBody(subs, message, subject, "", nil)

	// Asserts
	assert.NoErrorf(err, `error creating SNS message`)
	var unmarshalled map[string]interface{}
	err = json.Unmarshal(snsMessage, &unmarshalled)
	assert.NoError(err, `error unmarshalling SNS message "%s"`, snsMessage)
	receivedMessage, ok := unmarshalled[messageKey]
	assert.Truef(ok, `SNS message "%s" does not contain key "%s"`, snsMessage, messageKey)
	const expected = `{"default": "default message text", "sqs": "sqs message text"}`
	assert.Equal(expected, receivedMessage)
	receivedSubject, ok := unmarshalled[subjectKey]
	assert.Truef(ok, `SNS message "%s" does not contain key "%s"`, snsMessage, subjectKey)
	assert.Equal(subject, receivedSubject)
}
