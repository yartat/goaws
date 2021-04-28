package common

import (
	"testing"

	"github.com/p4tin/goaws/app"
	"github.com/stretchr/testify/assert"
)

func TestUUID_alwaysgood(t *testing.T) {
	uuid, _ := NewUUID()
	assert.NotEmptyf(t, uuid, "Failed to return UUID as expected")
}

func TestGetMD5Hash(t *testing.T) {
	hash1 := GetMD5Hash("This is a test")
	hash2 := GetMD5Hash("This is a test")
	assert.Equalf(t, hash1, hash2, "hashs and hash2 should be the same, but were not")

	hash1 = GetMD5Hash("This is a test")
	hash2 = GetMD5Hash("This is a tfst")
	assert.NotEqualf(t, hash1, hash2, "hashs and hash2 are the same, but should not be")
}

func TestSortedKeys(t *testing.T) {
	attributes := map[string]app.MessageAttributeValue{
		"b": {},
		"a": {},
	}

	keys := sortedKeys(attributes)
	assert.Equal(t, "a", keys[0])
	assert.Equal(t, "b", keys[1])
}
