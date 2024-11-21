package queue

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	ali_mns "github.com/aliyun/aliyun-mns-go-sdk"
	"github.com/aliyun/credentials-go/credentials"
	"github.com/byte-power/gorich/cloud"
)

const (
	ContextKeyAliMNSMessagePriority        = "gorich_mns_message_priority"
	ContextKeyAliMNSLongPollingWaitSeconds = "gorich_mns_long_polling_wait_seconds"

	AliMNSMessageDefaultPriority = 1
)

type AliMNSQueueMessage struct {
	message *ali_mns.MessageReceiveResponse
}

func (message *AliMNSQueueMessage) Body() string {
	return message.message.MessageBody
}

type AliMNSQueueService struct {
	queueName       string
	queue           ali_mns.AliMNSQueue
	credential      credentials.Credential
	credentialModel *credentials.CredentialModel
	option          *AliMNSClientOption
	lock            *sync.RWMutex
	stopCh          *chan struct{}
}

type AliMNSClientOption struct {
	EndPoint        string `json:"endpoint"`
	TimeoutSecond   int64  `json:"timeout_second"`
	MaxConnsPerHost int    `json:"max_conns_per_host"`
	QueueQPS        int32  `json:"queue_qps"`
	// MessagePriority is used to set message priority when sending messages
	MessagePriority                      int `json:"message_priority"`
	ReceiveMessageLongPollingWaitSeconds int `json:"receive_message_long_polling_wait_seconds"`

	CredentialType cloud.AliCloudCredentialType `json:"credential_type"`

	// required when CredentialType is AliCloudAccessKeyCredentialType, get from env if not provided
	AccessKeyId     string `json:"access_key_id"`
	AccessKeySecret string `json:"access_key_secret"`

	// optional when CredentialType is AliCloudECSRamRoleCredentialType
	RoleName string `json:"role_name"`

	// required when CredentialType is AliCloudOIDCRoleARNCredentialType, get from env if not provided
	RoleArn           string `json:"role_arn"`
	OIDCProviderArn   string `json:"oidc_provider_arn"`
	OIDCTokenFilePath string `json:"oidc_token_file_path"`

	// optional when CredentialType is AliCloudOIDCRoleARNCredentialType
	// RoleSessionName will get from env if not provided
	RoleSessionName       string `json:"role_session_name"`
	Policy                string `json:"policy"`
	RoleSessionExpiration int    `json:"role_session_expiration"`
}

func (option AliMNSClientOption) check() error {
	if option.EndPoint == "" {
		return errors.New("endpoint is required")
	}
	if option.TimeoutSecond < 0 {
		return errors.New("timeout_second must be >= 0")
	}
	if option.MaxConnsPerHost < 0 {
		return errors.New("max_conns_per_host must be >= 0")
	}
	if option.QueueQPS < 0 {
		return errors.New("queue_qps must be >= 0")
	}
	if option.MessagePriority < 0 {
		return errors.New("message_priority must be >= 0")
	}
	if option.ReceiveMessageLongPollingWaitSeconds < 0 {
		return errors.New("receive_message_long_polling_wait_seconds must be >= 0")
	}
	if option.CredentialType == "" {
		return errors.New("credential_type is required")
	}
	switch option.CredentialType {
	case cloud.AliCloudAccessKeyCredentialType:
		if option.AccessKeyId == "" {
			return fmt.Errorf("access_key_id is required with credential_type: %s", option.CredentialType)
		}
		if option.AccessKeySecret == "" {
			return fmt.Errorf("access_key_secret is required with credential_type: %s", option.CredentialType)
		}
	case cloud.AliCloudECSRamRoleCredentialType:
	case cloud.AliCloudOIDCRoleARNCredentialType:
		if option.RoleArn == "" {
			return fmt.Errorf("role_arn is required with credential_type: %s", option.CredentialType)
		}
		if option.OIDCProviderArn == "" {
			return fmt.Errorf("oidc_provider_arn is required with credential_type: %s", option.CredentialType)
		}
		if option.OIDCTokenFilePath == "" {
			return fmt.Errorf("oidc_token_file_path is required with credential_type: %s", option.CredentialType)
		}
	default:
		return fmt.Errorf("no supported credential_type: %s", option.CredentialType)
	}
	return nil
}

func (option AliMNSClientOption) GetProvider() cloud.Provider {
	return cloud.AliCloudMNSQueueProvider
}

func (option AliMNSClientOption) GetSecretID() string {
	return option.AccessKeyId
}

func (option AliMNSClientOption) GetSecretKey() string {
	return option.AccessKeySecret
}

func (option AliMNSClientOption) GetAssumeRoleArn() string {
	return option.RoleArn
}

func (option AliMNSClientOption) GetRegion() string {
	return ""
}

func (option AliMNSClientOption) GetAssumeRegion() string {
	return ""
}

func (option AliMNSClientOption) CheckAWS() error {
	return cloud.ErrProviderNotAWS
}

func (option AliMNSClientOption) CheckTencentCloud() error {
	return cloud.ErrProviderNotTencentCloud
}

func (option AliMNSClientOption) CheckStandaloneRedis() error {
	return cloud.ErrProviderNotStandaloneRedis
}

func (option AliMNSClientOption) CheckClusterRedis() error {
	return cloud.ErrProviderNotClusterRedis
}

func (option AliMNSClientOption) CheckAliCloudStorage() error {
	return cloud.ErrProviderNotAliCloudStorage
}

func (option *AliMNSClientOption) mergeDefaultOptions() AliMNSClientOption {
	switch option.CredentialType {
	case cloud.AliCloudAccessKeyCredentialType:
		if option.AccessKeyId == "" {
			option.AccessKeyId = os.Getenv(cloud.AliCloudEnvAccessKeyID)
		}
		if option.AccessKeySecret == "" {
			option.AccessKeySecret = os.Getenv(cloud.AliCloudEnvAccessKeySecret)
		}
	case cloud.AliCloudOIDCRoleARNCredentialType:
		if option.RoleArn == "" {
			option.RoleArn = os.Getenv(cloud.AliCloudEnvRoleArn)
		}
		if option.OIDCProviderArn == "" {
			option.OIDCProviderArn = os.Getenv(cloud.AliCloudEnvOIDCProviderArn)
		}
		if option.OIDCTokenFilePath == "" {
			option.OIDCTokenFilePath = os.Getenv(cloud.AliCloudEnvOIDCTokenFile)
		}
		if option.RoleSessionName == "" {
			option.RoleSessionName = os.Getenv(cloud.AliCloudEnvRoleSessionName)
		}
	}
	if option.QueueQPS == 0 {
		option.QueueQPS = ali_mns.DefaultQueueQPSLimit
	}
	if option.MessagePriority == 0 {
		option.MessagePriority = AliMNSMessageDefaultPriority
	}
	return *option
}

var ErrAliMNSQueueNameEmpty = errors.New("mns queue name is empty")

func getAliMNSQueueService(queueName string, option cloud.Option) (QueueService, error) {
	if queueName == "" {
		return nil, ErrAliMNSQueueNameEmpty
	}
	mnsOption, ok := option.(AliMNSClientOption)
	if !ok {
		return nil, fmt.Errorf("option parameter %+v should be AliMNSClientOption type", option)
	}
	mnsOption = mnsOption.mergeDefaultOptions()
	if err := mnsOption.check(); err != nil {
		return nil, err
	}
	service := &AliMNSQueueService{
		queueName:       queueName,
		queue:           nil,
		credential:      nil,
		credentialModel: nil,
		option:          &mnsOption,
		lock:            nil,
		stopCh:          nil,
	}
	return service, nil
}

func (service *AliMNSQueueService) initializeQueue() (*AliMNSQueueService, error) {
	if err := service.option.check(); err != nil {
		return nil, err
	}
	config := ali_mns.AliMNSClientConfig{
		EndPoint:        service.option.EndPoint,
		TimeoutSecond:   service.option.TimeoutSecond,
		MaxConnsPerHost: service.option.MaxConnsPerHost,
	}
	var credentialConfig *credentials.Config
	switch service.option.CredentialType {
	case cloud.AliCloudAccessKeyCredentialType:
		credentialConfig = &credentials.Config{
			Type:            (*string)(&service.option.CredentialType),
			AccessKeyId:     &service.option.AccessKeyId,
			AccessKeySecret: &service.option.AccessKeySecret,
		}
	case cloud.AliCloudECSRamRoleCredentialType:
		credentialConfig = &credentials.Config{
			Type:     (*string)(&service.option.CredentialType),
			RoleName: &service.option.RoleName,
		}
	case cloud.AliCloudOIDCRoleARNCredentialType:
		credentialConfig = &credentials.Config{
			Type:                  (*string)(&service.option.CredentialType),
			OIDCProviderArn:       &service.option.OIDCProviderArn,
			OIDCTokenFilePath:     &service.option.OIDCTokenFilePath,
			RoleArn:               &service.option.RoleArn,
			RoleSessionName:       &service.option.RoleSessionName,
			RoleSessionExpiration: &service.option.RoleSessionExpiration,
			Policy:                &service.option.Policy,
		}
	}
	credential, err := credentials.NewCredential(credentialConfig)
	if err != nil {
		return nil, fmt.Errorf("new credential error %w", err)
	}
	credentialModel, err := credential.GetCredential()
	if err != nil {
		return nil, fmt.Errorf("get credential model error %w", err)
	}
	if credentialModel.AccessKeyId != nil {
		config.AccessKeyId = *credentialModel.AccessKeyId
	}
	if credentialModel.AccessKeySecret != nil {
		config.AccessKeySecret = *credentialModel.AccessKeySecret
	}
	if credentialModel.SecurityToken != nil {
		config.Token = *credentialModel.SecurityToken
	}
	client := ali_mns.NewAliMNSClientWithConfig(config)
	queue := ali_mns.NewMNSQueue(service.queueName, client, service.option.QueueQPS)
	stopCh := make(chan struct{}, 1)
	initializedService := &AliMNSQueueService{
		queueName:       service.queueName,
		queue:           queue,
		credential:      credential,
		credentialModel: credentialModel,
		option:          service.option,
		lock:            &sync.RWMutex{},
		stopCh:          &stopCh,
	}
	if service.needRefreshCredential() {
		go initializedService.refreshCredential()
	}
	return initializedService, nil
}

func (service *AliMNSQueueService) needRefreshCredential() bool {
	return service.option.CredentialType != cloud.AliCloudAccessKeyCredentialType
}

func (service *AliMNSQueueService) _refreshCredential() error {
	config := ali_mns.AliMNSClientConfig{
		EndPoint:        service.option.EndPoint,
		TimeoutSecond:   service.option.TimeoutSecond,
		MaxConnsPerHost: service.option.MaxConnsPerHost,
	}
	// GetCredential will update credential if the current one is going to expire.
	credentialModel, err := service.credential.GetCredential()
	if err != nil {
		return fmt.Errorf("get credential model error when updating credential %w", err)
	}
	if service.isCredentialNotUpdated(credentialModel) || credentialModel == nil {
		return nil
	}
	if credentialModel.AccessKeyId != nil {
		config.AccessKeyId = *credentialModel.AccessKeyId
	}
	if credentialModel.AccessKeySecret != nil {
		config.AccessKeySecret = *credentialModel.AccessKeySecret
	}
	if credentialModel.SecurityToken != nil {
		config.Token = *credentialModel.SecurityToken
	}
	client := ali_mns.NewAliMNSClientWithConfig(config)
	service.lock.Lock()
	defer service.lock.Unlock()
	service.queue = ali_mns.NewMNSQueue(service.queue.Name(), client, service.option.QueueQPS)
	service.credentialModel = credentialModel
	return nil
}

func (service *AliMNSQueueService) isCredentialNotUpdated(credentialModel *credentials.CredentialModel) bool {
	if service.credentialModel == nil || credentialModel == nil {
		return service.credentialModel == credentialModel
	}

	return isStringPtrEqual(service.credentialModel.AccessKeyId, credentialModel.AccessKeyId) &&
		isStringPtrEqual(service.credentialModel.AccessKeySecret, credentialModel.AccessKeySecret) &&
		isStringPtrEqual(service.credentialModel.SecurityToken, credentialModel.SecurityToken)
}

func isStringPtrEqual(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func (service *AliMNSQueueService) refreshCredential() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := service._refreshCredential(); err != nil {
				fmt.Printf("Error updating credential: %v\n", err)
			}
		case <-*service.stopCh:
			return
		}
	}
}

func (service *AliMNSQueueService) CreateProducer() (Producer, error) {
	return service.initializeQueue()
}

func (service *AliMNSQueueService) CreateConsumer() (Consumer, error) {
	return service.initializeQueue()
}

func (service *AliMNSQueueService) Close() error {
	if service.stopCh != nil && service.needRefreshCredential() {
		*service.stopCh <- struct{}{}
	}
	return nil
}

func (service *AliMNSQueueService) SendMessage(ctx context.Context, body string) error {
	priority, ok := ctx.Value(ContextKeyAliMNSMessagePriority).(int)
	if !ok {
		priority = service.option.MessagePriority
	}
	input := ali_mns.MessageSendRequest{MessageBody: body, Priority: int64(priority)}
	service.lock.RLock()
	defer service.lock.RUnlock()
	_, err := service.queue.SendMessage(input)
	return err
}

func (service *AliMNSQueueService) ReceiveMessages(ctx context.Context, maxCount int) ([]Message, error) {
	respChan := make(chan ali_mns.BatchMessageReceiveResponse, 1)
	errChan := make(chan error, 1)
	waitSeconds, ok := ctx.Value(ContextKeyAliMNSLongPollingWaitSeconds).(int)
	service.lock.RLock()
	if ok {
		service.queue.BatchReceiveMessage(respChan, errChan, int32(maxCount), int64(waitSeconds))
	} else if service.option.ReceiveMessageLongPollingWaitSeconds > 0 {
		service.queue.BatchReceiveMessage(respChan, errChan, int32(maxCount), int64(service.option.ReceiveMessageLongPollingWaitSeconds))
	} else {
		service.queue.BatchReceiveMessage(respChan, errChan, int32(maxCount))
	}
	service.lock.RUnlock()
	select {
	case resp := <-respChan:
		messages := make([]Message, 0, len(resp.Messages))
		for _, msg := range resp.Messages {
			aliMessage := msg
			message := &AliMNSQueueMessage{message: &aliMessage}
			messages = append(messages, message)
		}
		return messages, nil
	case err := <-errChan:
		return nil, err
	}
}

func (service *AliMNSQueueService) AckMessage(ctx context.Context, message Message) error {
	msg, ok := message.(*AliMNSQueueMessage)
	if !ok {
		return errors.New("invalid message type, should be AliMNSQueueMessage")
	}
	service.lock.RLock()
	defer service.lock.RUnlock()
	return service.queue.DeleteMessage(msg.message.ReceiptHandle)
}
