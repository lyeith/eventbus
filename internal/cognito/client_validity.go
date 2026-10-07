// Native app-client token lifetimes are persisted resource configuration.
package cognito

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type TokenValidityUnits struct {
	AccessToken  string `json:"AccessToken" yaml:"access_token"`
	IdToken      string `json:"IdToken" yaml:"id_token"`
	RefreshToken string `json:"RefreshToken" yaml:"refresh_token"`
}

type ClientTokenValidity struct {
	AccessTokenValidity  int                `json:"AccessTokenValidity"`
	IdTokenValidity      int                `json:"IdTokenValidity"`
	RefreshTokenValidity int                `json:"RefreshTokenValidity"`
	TokenValidityUnits   TokenValidityUnits `json:"TokenValidityUnits"`
}

type clientValidityRequest struct {
	AccessTokenValidity  *int                `json:"AccessTokenValidity"`
	IdTokenValidity      *int                `json:"IdTokenValidity"`
	RefreshTokenValidity *int                `json:"RefreshTokenValidity"`
	TokenValidityUnits   *TokenValidityUnits `json:"TokenValidityUnits"`
}

func normalizeClientValidity(req clientValidityRequest) (*ClientTokenValidity, error) {
	result := &ClientTokenValidity{AccessTokenValidity: 1, IdTokenValidity: 1, RefreshTokenValidity: 30, TokenValidityUnits: TokenValidityUnits{AccessToken: "hours", IdToken: "hours", RefreshToken: "days"}}
	if req.TokenValidityUnits != nil {
		if req.TokenValidityUnits.AccessToken != "" {
			result.TokenValidityUnits.AccessToken = req.TokenValidityUnits.AccessToken
		}
		if req.TokenValidityUnits.IdToken != "" {
			result.TokenValidityUnits.IdToken = req.TokenValidityUnits.IdToken
		}
		if req.TokenValidityUnits.RefreshToken != "" {
			result.TokenValidityUnits.RefreshToken = req.TokenValidityUnits.RefreshToken
		}
	}
	if req.AccessTokenValidity != nil {
		result.AccessTokenValidity = *req.AccessTokenValidity
	}
	if req.IdTokenValidity != nil {
		result.IdTokenValidity = *req.IdTokenValidity
	}
	if req.RefreshTokenValidity != nil && *req.RefreshTokenValidity != 0 {
		result.RefreshTokenValidity = *req.RefreshTokenValidity
	}
	// A supplied unit without a numeric validity preserves the native duration.
	for _, item := range []struct {
		value    *int
		supplied *int
		unit     *string
		seconds  int
	}{
		{&result.AccessTokenValidity, req.AccessTokenValidity, &result.TokenValidityUnits.AccessToken, 3600},
		{&result.IdTokenValidity, req.IdTokenValidity, &result.TokenValidityUnits.IdToken, 3600},
		{&result.RefreshTokenValidity, req.RefreshTokenValidity, &result.TokenValidityUnits.RefreshToken, 30 * 86400},
	} {
		factor, err := validityUnitSeconds(*item.unit)
		if err != nil {
			return nil, err
		}
		if item.supplied == nil || item.value == &result.RefreshTokenValidity && *item.supplied == 0 {
			if item.seconds%factor != 0 {
				return nil, errors.New("TokenValidityUnits requires an explicit validity when the default cannot be expressed in that unit")
			}
			*item.value = item.seconds / factor
		}
	}
	if _, _, _, err := result.Durations(); err != nil {
		return nil, err
	}
	return result, nil
}

func validityUnitSeconds(unit string) (int, error) {
	switch unit {
	case "seconds":
		return 1, nil
	case "minutes":
		return 60, nil
	case "hours":
		return 3600, nil
	case "days":
		return 86400, nil
	default:
		return 0, fmt.Errorf("invalid TokenValidityUnits value %q", unit)
	}
}

func (v *ClientTokenValidity) Durations() (access, id, refresh time.Duration, err error) {
	if v == nil {
		return 0, 0, 0, errors.New("client token validity is not configured")
	}
	fields := []struct {
		name     string
		value    int
		unit     string
		min, max int64
	}{
		{"AccessTokenValidity", v.AccessTokenValidity, v.TokenValidityUnits.AccessToken, 300, 86400},
		{"IdTokenValidity", v.IdTokenValidity, v.TokenValidityUnits.IdToken, 300, 86400},
		{"RefreshTokenValidity", v.RefreshTokenValidity, v.TokenValidityUnits.RefreshToken, 3600, 315360000},
	}
	durations := make([]time.Duration, 3)
	for i, field := range fields {
		factor, e := validityUnitSeconds(field.unit)
		if e != nil {
			return 0, 0, 0, e
		}
		if field.value < 0 || int64(field.value) > field.max/int64(factor) {
			return 0, 0, 0, fmt.Errorf("%s is outside its supported duration range", field.name)
		}
		secs := int64(field.value) * int64(factor)
		if secs < field.min {
			return 0, 0, 0, fmt.Errorf("%s must be between %d and %d seconds", field.name, field.min, field.max)
		}
		durations[i] = time.Duration(secs) * time.Second
	}
	return durations[0], durations[1], durations[2], nil
}

func (s *Handler) clientTokenDurations(client *CognitoClient) (time.Duration, time.Duration, time.Duration, error) {
	if client.TokenValidity != nil {
		return client.TokenValidity.Durations()
	}
	return s.accessTokenTTL, s.accessTokenTTL, s.refreshTokenTTL, nil
}

// JSON member presence distinguishes an omitted unit from an invalid empty enum.
func (units *TokenValidityUnits) UnmarshalJSON(raw []byte) error {
	type plain TokenValidityUnits
	var decoded plain
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	for name, rawValue := range fields {
		if name != "AccessToken" && name != "IdToken" && name != "RefreshToken" {
			return &CapabilityError{Field: "TokenValidityUnits." + name}
		}
		var unit string
		if err := json.Unmarshal(rawValue, &unit); err != nil {
			return err
		}
		if _, err := validityUnitSeconds(unit); err != nil {
			return err
		}
	}
	*units = TokenValidityUnits(decoded)
	return nil
}
