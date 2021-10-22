package cloud

type Provider string

const (
	AWSProvider          Provider = "aws"
	TencentCloudProvider Provider = "tencent"
)

type Options struct {
	Provider  Provider
	SecretID  string
	SecretKey string
	Region    string
}

func (options Options) Check() error {
	return nil
}
