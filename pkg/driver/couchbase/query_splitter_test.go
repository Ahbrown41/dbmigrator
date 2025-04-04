package couchbase

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSplitQueries(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		assert.Equal(t, 0, len(splitQueries("")))
	})
	t.Run("single", func(t *testing.T) {
		res := splitQueries("CREATE PRIMARY INDEX ON ${BUCKET}._default.profiles;")
		assert.Equal(t, 1, len(res))
	})
	t.Run("multiple", func(t *testing.T) {
		query := `
			CREATE COLLECTION ${BUCKET}._default.posts IF NOT EXISTS;
			CREATE COLLECTION ${BUCKET}._default.comments IF NOT EXISTS;
			`
		res := splitQueries(query)
		assert.Equal(t, 2, len(res))
	})
	t.Run("comment", func(t *testing.T) {
		query := `
			-- Insert post
			CREATE COLLECTION ${BUCKET}._default.profiles IF NOT EXISTS;
			CREATE COLLECTION ${BUCKET}._default.settings IF NOT EXISTS;
		`
		res := splitQueries(query)
		assert.Equal(t, 2, len(res))
	})
	t.Run("multiple comments", func(t *testing.T) {
		query := `
			-- Insert 1
			CREATE COLLECTION ${BUCKET}._default.profiles IF NOT EXISTS;
			-- Insert 2
			CREATE COLLECTION ${BUCKET}._default.settings IF NOT EXISTS;
		`
		res := splitQueries(query)
		assert.Equal(t, 2, len(res))
	})
	t.Run("multiple lines", func(t *testing.T) {
		query := `
			-- Insert user profile
			INSERT INTO ${BUCKET}._default.profiles (KEY, VALUE)
			VALUES ("profile::1", {
				"id": "1",
				"name": "Test User",
				"email": "test@example.com",
				"bio": "Test bio",
				"createdAt": NOW_STR()
			});
			
			-- Insert user settings
			INSERT INTO ${BUCKET}._default.settings (KEY, VALUE)
			VALUES ("settings::1", {
				"userId": "1",
				"theme": "light",
				"notifications": true,
				"createdAt": NOW_STR()
			});
		`
		res := splitQueries(query)
		assert.Equal(t, 2, len(res))
	})
}
