package snowflake

import (
	"database/sql"
	"github.com/naveego/plugin-pub-mssql/internal"
	"github.com/naveego/plugin-pub-mssql/internal/adapters"
	"github.com/naveego/plugin-pub-mssql/internal/meta"
	"github.com/naveego/plugin-pub-mssql/internal/pub"
)

type MetadataSource struct {
}

func (m MetadataSource) GetWriter(session *internal.OpSession, req *pub.PrepareWriteRequest) (adapters.Writer, error) {
	panic("implement me")
}

func (m MetadataSource) GetStoredProcedureProperties(db *sql.DB, formData internal.ConfigureWriteFormData) ([]*pub.Property, error) {
	panic("implement me")
}

func (m MetadataSource) GetStoredProcedures(db *sql.DB) ([]string, error) {
	panic("implement me")
}

func (m MetadataSource) GetConnectionString() (string, error) {
	panic("implement me")
}

func (m MetadataSource) GetSettings([]byte) (adapters.Settings, error) {
	panic("implement me")
}

func NewMetadataSource() (*MetadataSource, error) {
	return &MetadataSource{}, nil
}

func (m MetadataSource) GetSchemaInfoMap(db *sql.DB) (map[string]*meta.Schema, error) {
	panic("implement me")
}

var _ adapters.MetadataSource = &MetadataSource{}
