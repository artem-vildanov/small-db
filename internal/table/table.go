package table

import (
	"time"

	"github.com/artem-vildanov/small-db/internal/schema"
)

type Table struct {
	Path      string
	Name      string
	NumPages  int
	Schema    *schema.Schema
	CreatedAt time.Time
}

type TableMetadata struct {
	SchemaID  string    `json:"schemaId"`
	NumPages  int       `json:"numPages"`
	CreatedAt time.Time `json:"createdAt"`
}
