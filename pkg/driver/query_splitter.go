package driver

import "strings"

// SplitQueries splits migration content into separate queries
// handling multi-line statements and preserving formatting.
// It removes empty lines and lines containing only comments.
func SplitQueries(content string) []string {
	var queries []string
	var currentQuery strings.Builder
	inSingleLineComment := false
	inMultiLineComment := false
	inSingleQuote := false
	inDoubleQuote := false

	lines := strings.Split(content, "\n")
	for lineIndex, line := range lines {
		trimmedLine := strings.TrimSpace(line)

		// Skip empty lines
		if trimmedLine == "" {
			continue
		}

		// Skip lines that are just single-line comments
		if strings.HasPrefix(trimmedLine, "--") {
			continue
		}

		// Process the line character by character
		for i := 0; i < len(line); i++ {
			char := line[i]

			// Handle comments and quotes
			if !inSingleLineComment && !inMultiLineComment {
				// Check for start of comments
				if char == '-' && i+1 < len(line) && line[i+1] == '-' {
					inSingleLineComment = true
					currentQuery.WriteByte(char)
					continue
				} else if char == '/' && i+1 < len(line) && line[i+1] == '*' {
					inMultiLineComment = true
					currentQuery.WriteByte(char)
					continue
				} else if char == '\'' && !inDoubleQuote {
					inSingleQuote = !inSingleQuote
				} else if char == '"' && !inSingleQuote {
					inDoubleQuote = !inDoubleQuote
				}
			} else {
				// Check for end of comments
				if inSingleLineComment {
					// Single line comments end at the end of line
					// We'll reset this after the line is processed
				} else if inMultiLineComment && char == '*' && i+1 < len(line) && line[i+1] == '/' {
					inMultiLineComment = false
					currentQuery.WriteByte(char)
					currentQuery.WriteByte(line[i+1])
					i++
					continue
				}
			}

			// Check for end of query (semicolon outside of quotes and comments)
			if char == ';' && !inSingleQuote && !inDoubleQuote && !inSingleLineComment && !inMultiLineComment {
				currentQuery.WriteByte(char)
				queryStr := strings.TrimSpace(currentQuery.String())
				if queryStr != "" && queryStr != ";" {
					queries = append(queries, queryStr)
				}
				currentQuery.Reset()
			} else {
				currentQuery.WriteByte(char)
			}
		}

		// Add a newline character if this isn't the last line and we're building a query
		if lineIndex < len(lines)-1 && currentQuery.Len() > 0 {
			currentQuery.WriteByte('\n')
		}

		// Reset single-line comment state at end of line
		inSingleLineComment = false
	}

	// Add the last query if it doesn't end with a semicolon
	queryStr := strings.TrimSpace(currentQuery.String())
	if queryStr != "" {
		queries = append(queries, queryStr)
	}

	return queries
}
