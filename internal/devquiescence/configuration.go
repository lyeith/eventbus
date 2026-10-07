package devquiescence

import (
	"errors"
	"net/url"
)

var ErrConfiguration = errors.New("retained-owner callback origin must be configured before use")

// SetCallbackOrigin binds this process owner to its actual native callback peer.
// App composition sets it once before activity or management traffic. The value
// is diagnostic configuration; gateway admission still validates owner identity.
func (c *Coordinator) SetCallbackOrigin(origin string) error {
	parsed, err := url.Parse(origin)
	if err != nil || parsed.User != nil || parsed.Host == "" || parsed.Scheme != "http" && parsed.Scheme != "https" || parsed.Path != "" && parsed.Path != "/" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return ErrConfiguration
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.used {
		return ErrConfiguration
	}
	c.callbackOrigin = parsed.Scheme + "://" + parsed.Host
	return nil
}
