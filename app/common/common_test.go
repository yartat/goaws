package common

import (
	"testing"

	"github.com/p4tin/goaws/app"
	"github.com/stretchr/testify/assert"
)

func TestUUID_alwaysgood(t *testing.T) {
	// Arrange
	uuid, _ := NewUUID()

	// Asserts
	assert.NotEmptyf(t, uuid, "Failed to return UUID as expected")
}

func TestGetMD5Hash(t *testing.T) {
	// Act
	hash1 := GetMD5Hash("This is a test")
	hash2 := GetMD5Hash("This is a test")

	// Asserts
	assert.Equalf(t, hash1, hash2, "hashs and hash2 should be the same, but were not")

	// Act
	hash1 = GetMD5Hash("This is a test")
	hash2 = GetMD5Hash("This is a tfst")

	// Asserts
	assert.NotEqualf(t, hash1, hash2, "hashs and hash2 are the same, but should not be")
}

func TestSortedKeys(t *testing.T) {
	// Arrange
	attributes := map[string]app.MessageAttributeValue{
		"b": {},
		"a": {},
	}

	// Act
	keys := sortedKeys(attributes)

	// Asserts
	assert.Equal(t, "a", keys[0])
	assert.Equal(t, "b", keys[1])
}
