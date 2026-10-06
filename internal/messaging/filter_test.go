package messaging

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFilterPolicyNil(t *testing.T) {
	var fp *FilterPolicy

	result := fp.Matches(map[string]MessageAttribute{
		"event_type": {DataType: "String", StringValue: "anything"},
	})

	assert.True(t, result, "nil policy should match everything")
}

func TestFilterPolicyEmpty(t *testing.T) {
	fp := &FilterPolicy{Attributes: map[string][]string{}}

	result := fp.Matches(map[string]MessageAttribute{
		"event_type": {DataType: "String", StringValue: "anything"},
	})

	assert.True(t, result, "empty attributes should match everything")
}

func TestFilterPolicyExactMatch(t *testing.T) {
	fp := &FilterPolicy{
		Attributes: map[string][]string{
			"event_type": {"import.created"},
		},
	}

	result := fp.Matches(map[string]MessageAttribute{
		"event_type": {DataType: "String", StringValue: "import.created"},
	})

	assert.True(t, result)
}

func TestFilterPolicyNoMatch(t *testing.T) {
	fp := &FilterPolicy{
		Attributes: map[string][]string{
			"event_type": {"import.created"},
		},
	}

	result := fp.Matches(map[string]MessageAttribute{
		"event_type": {DataType: "String", StringValue: "file.uploaded"},
	})

	assert.False(t, result)
}

func TestFilterPolicyMissingAttribute(t *testing.T) {
	fp := &FilterPolicy{
		Attributes: map[string][]string{
			"event_type": {"import.created"},
		},
	}

	result := fp.Matches(map[string]MessageAttribute{
		"org_id": {DataType: "String", StringValue: "org-123"},
	})

	assert.False(t, result, "missing required attribute should not match")
}

func TestFilterPolicyMultiValue(t *testing.T) {
	fp := &FilterPolicy{
		Attributes: map[string][]string{
			"event_type": {"import.created", "file.uploaded"},
		},
	}

	t.Run("matches first value", func(t *testing.T) {
		result := fp.Matches(map[string]MessageAttribute{
			"event_type": {DataType: "String", StringValue: "import.created"},
		})
		assert.True(t, result)
	})

	t.Run("matches second value", func(t *testing.T) {
		result := fp.Matches(map[string]MessageAttribute{
			"event_type": {DataType: "String", StringValue: "file.uploaded"},
		})
		assert.True(t, result)
	})

	t.Run("no match for unlisted value", func(t *testing.T) {
		result := fp.Matches(map[string]MessageAttribute{
			"event_type": {DataType: "String", StringValue: "user.deleted"},
		})
		assert.False(t, result)
	})
}

func TestFilterPolicyMultipleKeys(t *testing.T) {
	fp := &FilterPolicy{
		Attributes: map[string][]string{
			"event_type": {"import.created"},
			"org_id":     {"org-1"},
		},
	}

	result := fp.Matches(map[string]MessageAttribute{
		"event_type": {DataType: "String", StringValue: "import.created"},
		"org_id":     {DataType: "String", StringValue: "org-1"},
	})

	assert.True(t, result, "all keys matching should pass")
}

func TestFilterPolicyMultipleKeysPartialMatch(t *testing.T) {
	fp := &FilterPolicy{
		Attributes: map[string][]string{
			"event_type": {"import.created"},
			"org_id":     {"org-1"},
		},
	}

	result := fp.Matches(map[string]MessageAttribute{
		"event_type": {DataType: "String", StringValue: "import.created"},
		"org_id":     {DataType: "String", StringValue: "org-999"},
	})

	assert.False(t, result, "partial match (first key yes, second key no) should fail")
}

func TestParseFilterPolicy(t *testing.T) {
	fp, err := ParseFilterPolicy(`{"event_type": ["import.created", "file.uploaded"], "org_id": ["org-1"]}`)

	assert.NoError(t, err)
	assert.NotNil(t, fp)
	assert.Equal(t, []string{"import.created", "file.uploaded"}, fp.Attributes["event_type"])
	assert.Equal(t, []string{"org-1"}, fp.Attributes["org_id"])
}

func TestParseFilterPolicyEmpty(t *testing.T) {
	fp, err := ParseFilterPolicy("")

	assert.NoError(t, err)
	assert.Nil(t, fp)
}

func TestParseFilterPolicyInvalid(t *testing.T) {
	fp, err := ParseFilterPolicy("{not valid json")

	assert.Error(t, err)
	assert.Nil(t, fp)
}

func TestFilterPolicyNilAttrs(t *testing.T) {
	fp := &FilterPolicy{
		Attributes: map[string][]string{
			"event_type": {"import.created"},
		},
	}

	result := fp.Matches(nil)

	assert.False(t, result, "nil attrs should not match a non-empty policy")
}

func TestFilterPolicyEmptyAttrs(t *testing.T) {
	fp := &FilterPolicy{
		Attributes: map[string][]string{
			"event_type": {"import.created"},
		},
	}

	result := fp.Matches(map[string]MessageAttribute{})

	assert.False(t, result, "empty attrs should not match a non-empty policy")
}

func TestFilterPolicyEqualIgnoresValueOrderAndTreatsNilAsEmpty(t *testing.T) {
	a := &FilterPolicy{Attributes: map[string][]string{"event_type": {"a", "b"}}}
	b := &FilterPolicy{Attributes: map[string][]string{"event_type": {"b", "a"}}}
	assert.True(t, a.Equal(b))
	assert.True(t, (*FilterPolicy)(nil).Equal(&FilterPolicy{}))
	assert.False(t, a.Equal(nil))
	assert.False(t, a.Equal(&FilterPolicy{Attributes: map[string][]string{"event_type": {"a"}}}))
	assert.False(t, a.Equal(&FilterPolicy{Attributes: map[string][]string{"source": {"a", "b"}}}))
}
