//go:build !linux && !darwin

package devcapture

import (
	"errors"
	"os"
)

var errPrivateUnsupported = errors.New("private capture files require Linux or macOS")

func privateOpenFlags() (int, error)      { return 0, errPrivateUnsupported }
func checkPrivateOwner(os.FileInfo) error { return errPrivateUnsupported }
