package drivers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnescapeUnicodeSequencesFromURL(t *testing.T) {
	testData := []struct {
		name      string
		url       string
		expected  string
		expectErr bool
	}{
		{
			name:      "should unescape the escape sequences",
			url:       "root:mostest@tcp(localhost:3306)/mysql-test?charset=utf8mb4,utf8\u0026readTimeout=50s\u0026writeTimeout=50s\u0026multiStatements=true",
			expected:  "root:mostest@tcp(localhost:3306)/mysql-test?charset=utf8mb4,utf8&readTimeout=50s&writeTimeout=50s&multiStatements=true",
			expectErr: false,
		},
		{
			name:      "should not break the string if there are no escape sequences",
			url:       "root:mostest@tcp(localhost:3306)/mysql-test?charset=utf8mb4,utf8&readTimeout=50s&writeTimeout=50s&multiStatements=true",
			expected:  "root:mostest@tcp(localhost:3306)/mysql-test?charset=utf8mb4,utf8&readTimeout=50s&writeTimeout=50s&multiStatements=true",
			expectErr: false,
		},
	}
	for _, data := range testData {
		t.Run(data.name, func(t *testing.T) {
			actual, err := UnescapeUnicodeSequencesFromURL(data.url)
			require.True(t, data.expectErr == (err != nil))
			require.Equal(t, data.expected, actual)
		})
	}
}
