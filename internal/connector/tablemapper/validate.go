package tablemapper

import (
	"fmt"
	"unicode"
)

// ValidateColumnName validates that a column name contains only allowed characters.
// The only allowed characters are alphanumeric and underscores.
func ValidateColumnName(name string) error {
	if name == "" {
		return fmt.Errorf("column name is required")
	}
	for _, c := range name {
		if !unicode.IsLetter(c) && !unicode.IsDigit(c) && c != '_' {
			return fmt.Errorf("column name contains invalid characters")
		}
	}
	return nil
}

// ValidateTableName validates that a table name contains only allowed characters.
// The only allowed characters are alphanumeric and underscores.
func ValidateTableName(name string) error {
	if name == "" {
		return fmt.Errorf("table name is required")
	}
	for _, c := range name {
		if !unicode.IsLetter(c) && !unicode.IsDigit(c) && c != '_' {
			return fmt.Errorf("table name contains invalid characters")
		}
	}
	return nil
}
