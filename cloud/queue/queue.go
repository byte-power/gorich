package queue

import (
	"context"

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
	}
	return nil, cloud.ErrUnsupportedCloudProvider
}
