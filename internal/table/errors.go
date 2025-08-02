package table

import "fmt"

func ErrTableWithNameExists(name string) error {
	return fmt.Errorf("table with name %s already exists", name)
}

func ErrTableWithNameDoesntExist(name string) error {
	return fmt.Errorf("table with name %s already exists", name)
}

func ErrRecordDoesntMatchSchema(name string) error {
	return fmt.Errorf("record doesnt match schema of table %s", name)
}

func ErrNoSuchColumnInSchema(column string) error {
	return fmt.Errorf("no column with name %s in schema", column)
}

func ErrIncompatibleTypes(schemaType, inputType string) error {
	return fmt.Errorf(
		"schema type %s is incompatible with input type %s", 
		schemaType, 
		inputType,
	)
}

func ErrFieldNotProvided(fieldName string) error {
	return fmt.Errorf("field %s not provided", fieldName)
}

func ErrRecordNotFound() error {
	return fmt.Errorf("record not found")
}

func ErrUniqueConstraintViolation(violatedFields []string) error {
	return fmt.Errorf("unique constraint violation on fields: %v", violatedFields)
}