package conf

import (
	"testing"

	"github.com/p4tin/goaws/app"
	assert2 "github.com/stretchr/testify/assert"
)

func TestConfig_NoQueuesOrTopics(t *testing.T) {
	// Arrange
	assert := assert2.New(t)
	env := "NoQueuesOrTopics"

	// Act
	port := LoadYamlConfig("./mock-data/mock-config.yaml", env)

	// Asserts
	assert.Equalf("4100", port[0], "Expected port number 4100")
	assert.Emptyf(envs[env].Queues, "Expected zero queues to be in the environment")
	assert.Emptyf(app.SyncQueues.Queues, "Expected zero queues to be in the sqs topics")
	assert.Emptyf(envs[env].Topics, "Expected zero topics to be in the environment")
	assert.Emptyf(app.SyncTopics.Topics, "Expected zero topics to be in the sns topics")
}

func TestConfig_CreateQueuesTopicsAndSubscriptions(t *testing.T) {
	// Arrange
	assert := assert2.New(t)
	env := "Local"

	// Act
	port := LoadYamlConfig("./mock-data/mock-config.yaml", env)

	// Asserts
	assert.Equalf("4100", port[0], "Expected port number 4100")
	assert.Equalf(3, len(envs[env].Queues), "Expected three queues to be in the environment")
	assert.Equalf(5, len(app.SyncQueues.Queues), "Expected five queues to be in the sqs topics")
	assert.Equalf(2, len(envs[env].Topics), "Expected two topics to be in the environment")
	assert.Equalf(2, len(app.SyncTopics.Topics), "Expected two topics to be in the sns topics")
}

func TestConfig_QueueAttributes(t *testing.T) {
	// Arrange
	assert := assert2.New(t)
	env := "Local"

	// Act
	port := LoadYamlConfig("./mock-data/mock-config.yaml", env)

	// Assert
	assert.Equalf("4100", port[0], "Expected port number 4100")
	assert.Equalf(10, app.SyncQueues.Queues["local-queue1"].ReceiveWaitTimeSecs, "Expected local-queue1 Queue to be configured with ReceiveMessageWaitTimeSeconds: 10")
	assert.Equalf(10, app.SyncQueues.Queues["local-queue1"].TimeoutSecs, "Expected local-queue1 Queue to be configured with VisibilityTimeout: 10")
	assert.Equalf(20, app.SyncQueues.Queues["local-queue2"].ReceiveWaitTimeSecs, "Expected local-queue2 Queue to be configured with ReceiveMessageWaitTimeSeconds: 20")
}

func TestConfig_NoQueueAttributeDefaults(t *testing.T) {
	// Arrange
	assert := assert2.New(t)
	env := "NoQueueAttributeDefaults"

	// Act
	LoadYamlConfig("./mock-data/mock-config.yaml", env)

	// Assert
	assert.Equalf(0, app.SyncQueues.Queues["local-queue1"].ReceiveWaitTimeSecs, "Expected local-queue1 Queue to be configured with ReceiveMessageWaitTimeSeconds: 0")
	assert.Equalf(30, app.SyncQueues.Queues["local-queue1"].TimeoutSecs, "Expected local-queue1 Queue to be configured with VisibilityTimeout: 30")
	assert.Equalf(20, app.SyncQueues.Queues["local-queue2"].ReceiveWaitTimeSecs, "Expected local-queue2 Queue to be configured with ReceiveMessageWaitTimeSeconds: 20")
}
