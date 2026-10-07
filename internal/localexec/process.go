// Package localexec owns the operating-system process groups used by local
// handler runners. Service protocols, execution results and policies stay with
// the caller.
package localexec

import "errors"

var ErrUnsupported = errors.New("local subprocess execution requires Linux or macOS process groups")
