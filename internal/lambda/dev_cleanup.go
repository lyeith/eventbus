package lambda

import (
	"net/http"
	"strings"
)

// DevInvokeTarget projects the registered execution target and native
// reference namespace. Apps own any resource-namespace rule.
type DevInvokeTarget struct {
	TargetInfo
	Region, AccountID string
}

// DescribeDevTarget projects native reference metadata after ordinary target
// validation. The service owns reference syntax; apps own namespace policy.
func (service *Service) DescribeDevTarget(reference, qualifier string) (DevInvokeTarget, error) {
	target, err := service.DescribeTarget(reference, qualifier)
	if err != nil {
		return DevInvokeTarget{}, err
	}
	result := DevInvokeTarget{TargetInfo: target}
	if strings.HasPrefix(reference, "arn:") {
		parts := strings.SplitN(reference, ":", 7)
		result.Region, result.AccountID = parts[3], parts[4]
	} else if account, _, partial := strings.Cut(reference, ":function:"); partial {
		result.AccountID = account
	}
	return result, nil
}

// DevRequestResponseTarget inspects a native Invoke's immutable metadata without
// reading its payload. Applications declare exact cleanup targets and retain
// responsibility for payload authorization and cleanup business behavior.
func (service *Service) DevRequestResponseTarget(request *http.Request) (DevInvokeTarget, bool) {
	if service == nil || request.Method != http.MethodPost || !service.Match(request) || !strings.HasSuffix(request.URL.Path, invokeSuffix) {
		return DevInvokeTarget{}, false
	}
	mode := request.Header.Get("X-Amz-Invocation-Type")
	if mode != "" && mode != "RequestResponse" {
		return DevInvokeTarget{}, false
	}
	reference := requestFunctionName(request)
	target, err := service.DescribeDevTarget(reference, request.URL.Query().Get("Qualifier"))
	return target, err == nil
}
