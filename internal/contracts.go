package internal

import (
	"database/sql"
	"github.com/hashicorp/go-hclog"
	"github.com/naveego/plugin-pub-mssql/internal/meta"
	"github.com/naveego/plugin-pub-mssql/internal/pub"
)

var Driver = "mssql"

const (
	MSSQLDriver     = "mssql"
	SnowflakeDriver = "snowflake"
)

type MetadataSource interface {
	GetSettings([]byte) (Settings, error)
	GetSchemaDiscoverer(log hclog.Logger) (SchemaDiscoverer, error)
	GetWriter(session *OpSession, req *pub.PrepareWriteRequest) (Writer, error)
	GetRealTimeHelper() (RealTimeHelper, error)
	GetSchemaInfoMap(db *sql.DB) (map[string]*meta.Schema, error)
	GetStoredProcedures(db *sql.DB) ([]string, error)
	GetStoredProcedureProperties(db *sql.DB, formData ConfigureWriteFormData) ([]*pub.Property, error)
}

type Settings interface {
	Validate() error
	GetConnectionString() (string, error)
	GetPrePublishQuery() string
	GetPostPublishQuery() string
	GetDatabase() string
}

type SchemaDiscoverer interface {
	DiscoverSchemas(session *OpSession, req *pub.DiscoverSchemasRequest) (<-chan *pub.Schema, error)
	DiscoverSchemasSync(session *OpSession, req *pub.DiscoverSchemasRequest) ([]*pub.Schema, error)
}

type Writer interface {
	Write(session *OpSession, record *pub.UnmarshalledRecord) error
}

type RealTimeHelper interface {
	ConfigureRealTime(session *OpSession, req *pub.ConfigureRealTimeRequest) (*pub.ConfigureRealTimeResponse, error)
}
