package queue

import (
	"github.com/byte-power/gorich/cloud"
)

// StandaloneRedisQueueOptionV7 用于 Redis Standalone 7.x 实例

type StandaloneRedisQueueOptionV7 struct {
	StandaloneRedisQueueOption
}

func (option StandaloneRedisQueueOptionV7) GetProvider() cloud.Provider {
	return cloud.StandaloneRedisProviderV7
}

// ClusterRedisQueueOptionV7 用于 Redis Cluster 7.x 集群

type ClusterRedisQueueOptionV7 struct {
	ClusterRedisQueueOption
}

func (option ClusterRedisQueueOptionV7) GetProvider() cloud.Provider {
	return cloud.ClusterRedisProviderV7
}
