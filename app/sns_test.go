package app

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFilterPolicy_IsSatisfiedBy(t *testing.T) {
	// Arrange
	var tests = []struct {
		filterPolicy      *FilterPolicy
		messageAttributes map[string]MessageAttributeValue
		expected          bool
	}{
		{
			&FilterPolicy{"foo": {"bar"}},
			map[string]MessageAttributeValue{"foo": {DataType: "String", Value: "bar"}},
			true,
		},
		{
			&FilterPolicy{"foo": {"bar", "xyz"}},
			map[string]MessageAttributeValue{"foo": {DataType: "String", Value: "xyz"}},
			true,
		},
		{
			&FilterPolicy{"foo": {"bar", "xyz"}, "abc": {"def"}},
			map[string]MessageAttributeValue{"foo": {DataType: "String", Value: "xyz"},
				"abc": {DataType: "String", Value: "def"}},
			true,
		},
		{
			&FilterPolicy{"foo": {"bar"}},
			map[string]MessageAttributeValue{"foo": {DataType: "String", Value: "baz"}},
			false,
		},
		{
			&FilterPolicy{"foo": {"bar"}},
			map[string]MessageAttributeValue{},
			false,
		},
		{
			&FilterPolicy{"foo": {"bar"}, "abc": {"def"}},
			map[string]MessageAttributeValue{"foo": {DataType: "String", Value: "bar"}},
			false,
		},
		{
			&FilterPolicy{"foo": {"bar"}},
			map[string]MessageAttributeValue{"foo": {DataType: "Binary", Value: "bar"}},
			false,
		},
	}

	// Act
	for i, tt := range tests {
		actual := tt.filterPolicy.IsSatisfiedBy(tt.messageAttributes)
		assert.Equalf(t, tt.expected, actual, "#%d test", i)
	}
}
