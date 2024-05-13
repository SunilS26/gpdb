package utils

import (
	"gopkg.in/yaml.v3"
)

// Reads yaml file and unmarshall contents
// into the struct object
// To be noted that: irrelevant fields will be ignored,
// and default/empty values will be set to the variables in the struct
func ReadYamlFile[T any](contents *T, filePath string) error {
	yfile, err := System.ReadFile(filePath)
	if err != nil {
		return err
	}

	err = yaml.Unmarshal(yfile, &contents)
	return err
}
