package queue

import (
	"context"
	"errors"

	"github.com/byte-power/gorich/cloud"
)

type QueueService interface {
	CreateProducer() (Producer, error)
	CreateConsumer() (Consumer, error)
	Close() error
}

type Producer interface {
	SendMessage(ctx context.Context, body string) error
	Close() error
}

type Consumer interface {
	ReceiveMessages(ctx context.Context, maxCount int) ([]Message, error)
	AckMessage(ctx context.Context, message Message) error
	Close() error
}

type Message interface {
	Body() string
}

// GetQueueService is deprecated, use GetQueueServiceWithOption instead.
func GetQueueService(queueOrTopicSubName string, option cloud.Option) (QueueService, error) {
	if option.GetProvider() == cloud.TencentCloudProvider {
		return GetTencentCloudQueueService(queueOrTopicSubName, option)
	} else if option.GetProvider() == cloud.AWSProvider {
		return GetAWSQueueService(queueOrTopicSubName, option)
	} else if option.GetProvider() == cloud.StandaloneRedisProvider {
		return GetStandaloneRedisQueueService(queueOrTopicSubName, option)
	} else if option.GetProvider() == cloud.ClusterRedisProvider {
		return GetClusterRedisQueueService(queueOrTopicSubName, option)
	} else if option.GetProvider() == cloud.StandaloneRedisProviderV7 {
		return getStandaloneRedisQueueServiceForV7(queueOrTopicSubName, option)
	} else if option.GetProvider() == cloud.ClusterRedisProviderV7 {
		return getClusterRedisQueueServiceV7(queueOrTopicSubName, option)
	} else if option.GetProvider() == cloud.AliCloudProvider {
		return getAliMNSQueueService(queueOrTopicSubName, option)
	}
	return nil, cloud.ErrUnsupportedCloudProvider
}

var ErrQueueNameRequired = errors.New("queue_name is required")

type QueueOption struct {
	Provider          cloud.Provider             `json:"provider" yaml:"provider"`
	QueueName         string                     `json:"queue_name" yaml:"queue_name"`
	SQS               cloud.AWSOption            `json:"sqs" yaml:"sqs"`
	MNS               AliMNSClientOption         `json:"mns" yaml:"mns"`
	StandaloneRedis   StandaloneRedisQueueOption `json:"standalone_redis" yaml:"standalone_redis"`
	ClusterRedis      ClusterRedisQueueOption    `json:"cluster_redis" yaml:"cluster_redis"`
	StandaloneRedisV7 ClusterRedisQueueOptionV7  `json:"standalone_redis_v7" yaml:"standalone_redis_v7"`
	ClusterRedisV7    ClusterRedisQueueOptionV7  `json:"cluster_redis_v7" yaml:"cluster_redis_v7"`
	Pulsar            TencentCloudQueueOption    `json:"pulsar" yaml:"pulsar"`
}

func (option QueueOption) Check() error {
	if option.QueueName == "" {
		return ErrQueueNameRequired
	}
	return nil
}

func GetQueueServiceWithOption(option QueueOption) (QueueService, error) {
	if err := option.Check(); err != nil {
		return nil, err
	}
	switch option.Provider {
	case cloud.AWSProvider:
		return getAWSQueueService(option.QueueName, option.SQS)
	case cloud.AliCloudProvider:
		return getAliMNSQueueService(option.QueueName, option.MNS)
	case cloud.StandaloneRedisProvider:
		return GetStandaloneRedisQueueService(option.QueueName, option.StandaloneRedis)
	case cloud.ClusterRedisProvider:
		return GetClusterRedisQueueService(option.QueueName, option.ClusterRedis)
	case cloud.StandaloneRedisProviderV7:
		return getStandaloneRedisQueueServiceForV7(option.QueueName, option.StandaloneRedisV7)
	case cloud.ClusterRedisProviderV7:
		return getClusterRedisQueueServiceV7(option.QueueName, option.ClusterRedisV7)
	case cloud.TencentCloudProvider:
		return GetTencentCloudQueueService(option.QueueName, option.Pulsar)
	default:
		return nil, cloud.ErrUnsupportedCloudProvider
	}
}
