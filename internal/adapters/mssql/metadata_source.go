package mssql

import (
	"database/sql"
	"fmt"
	"github.com/hashicorp/go-hclog"
	"github.com/naveego/plugin-pub-mssql/internal"
	"github.com/naveego/plugin-pub-mssql/internal/adapters"
	"github.com/naveego/plugin-pub-mssql/internal/meta"
	"github.com/naveego/plugin-pub-mssql/internal/pub"
	"github.com/pkg/errors"
	"regexp"
	"sort"
	"strings"
)

type MetadataSource struct {
}

func (m MetadataSource) GetRealTimeHelper() (adapters.RealTimeHelper, error) {
	return NewRealTimeHelper()
}

func NewMetadataSource() (*MetadataSource, error) {
	return &MetadataSource{}, nil
}

func (m MetadataSource) GetSchemaInfoMap(db *sql.DB) (map[string]*meta.Schema, error) {
	var schemaInfo map[string]*meta.Schema
	rows, err := db.Query(`SELECT t.TABLE_NAME
     , t.TABLE_SCHEMA
     , t.TABLE_TYPE
     , c.COLUMN_NAME
     , tc.CONSTRAINT_TYPE
, CASE
  WHEN exists (SELECT 1 FROM sys.change_tracking_tables WHERE object_id = OBJECT_ID(t.TABLE_SCHEMA + '.' + t.TABLE_NAME))
  THEN 1
  ELSE 0
  END AS CHANGE_TRACKING
FROM INFORMATION_SCHEMA.TABLES AS t
       INNER JOIN INFORMATION_SCHEMA.COLUMNS AS c ON c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME
       LEFT OUTER JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS ccu
                       ON ccu.COLUMN_NAME = c.COLUMN_NAME AND ccu.TABLE_NAME = t.TABLE_NAME AND
                          ccu.TABLE_SCHEMA = t.TABLE_SCHEMA
       LEFT OUTER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
                       ON tc.CONSTRAINT_NAME = ccu.CONSTRAINT_NAME AND tc.CONSTRAINT_SCHEMA = ccu.CONSTRAINT_SCHEMA

ORDER BY TABLE_NAME`)
	if err != nil {
		return nil, errors.Errorf("could not read database schema: %s", err)
	}

	// Collect table names for display in UIs.
	for rows.Next() {
		var (
			schema, table, typ, columnName string
			constraint                     *string
			changeTracking                 bool
		)
		err = rows.Scan(&table, &schema, &typ, &columnName, &constraint, &changeTracking)
		if err != nil {
			return nil, errors.Wrap(err, "could not read table schema")
		}
		id := getSchemaID(schema, table)
		info, ok := schemaInfo[id]
		if !ok {
			info = &meta.Schema{
				ID:               id,
				IsTable:          typ == "BASE TABLE",
				IsChangeTracking: changeTracking,
			}
			schemaInfo[id] = info
		}
		columnName = fmt.Sprintf("[%s]", columnName)
		columnInfo, ok := info.LookupColumn(columnName)
		if !ok {
			columnInfo = info.AddColumn(&meta.Column{ID: columnName})
		}
		columnInfo.IsKey = columnInfo.IsKey || constraint != nil && *constraint == "PRIMARY KEY"
	}

	return schemaInfo, nil
}

func (m MetadataSource) GetStoredProcedures(db *sql.DB) ([]string, error) {
	var storedProcedures []string

	rows, err := db.Query("SELECT ROUTINE_SCHEMA, ROUTINE_NAME FROM information_schema.routines WHERE routine_type = 'PROCEDURE'")
	if err != nil {
		return nil, errors.Errorf("could not read stored procedures from database: %s", err)
	}

	for rows.Next() {
		var schema, name string
		var safeName string
		err = rows.Scan(&schema, &name)
		if err != nil {
			return nil, errors.Wrap(err, "could not read stored procedure schema")
		}
		if schema == "dbo" {
			safeName = makeSQLNameSafe(name)
		} else {
			safeName = fmt.Sprintf("%s.%s", makeSQLNameSafe(schema), makeSQLNameSafe(name))
		}
		storedProcedures = append(storedProcedures, safeName)
	}
	sort.Strings(storedProcedures)

	return storedProcedures, nil
}

func (m MetadataSource) GetStoredProcedureProperties(db *sql.DB, formData internal.ConfigureWriteFormData) ([]*pub.Property, error) {
	var query string
	var properties []*pub.Property
	var data string
	var row *sql.Row
	var stmt *sql.Stmt
	var rows *sql.Rows
	var sprocSchema, sprocName string

	if formData.StoredProcedure == "" {
		return nil, nil
	}

	sprocSchema, sprocName = decomposeSafeName(formData.StoredProcedure)
	// check if stored procedure exists
	query = `SELECT 1
FROM information_schema.routines
WHERE routine_type = 'PROCEDURE'
AND SPECIFIC_SCHEMA = @schema
AND SPECIFIC_NAME = @name
`
	stmt, err := db.Prepare(query)
	if err != nil {
		return nil, errors.Wrap(err, "error checking stored procedure")
	}

	row = stmt.QueryRow(sql.Named("schema", sprocSchema), sql.Named("name", sprocName))

	err = row.Scan(&data)
	if err != nil {
		return nil, errors.Wrap(err, "stored procedure does not exist")
	}

	// get params for stored procedure
	query = `SELECT PARAMETER_NAME AS Name, DATA_TYPE AS Type
FROM INFORMATION_SCHEMA.PARAMETERS
WHERE SPECIFIC_SCHEMA = @schema
AND SPECIFIC_NAME = @name
`
	stmt, err = db.Prepare(query)
	if err != nil {
		return nil, errors.Wrap(err, "error preparing to get parameters for stored procedure")
	}

	rows, err = stmt.Query(sql.Named("schema", sprocSchema), sql.Named("name", sprocName))
	if err != nil {
		return nil, errors.Wrap(err, "error getting parameters for stored procedure")
	}

	// add all params to properties of schema
	for rows.Next() {
		var colName, colType string
		err := rows.Scan(&colName, &colType)
		if err != nil {
			return nil, errors.Wrap(err, "error getting parameters for stored procedure")
		}

		properties = append(properties, &pub.Property{
			Id:           strings.TrimPrefix(colName, "@"),
			TypeAtSource: colType,
			Type:         ConvertSQLTypeToPluginType(colType, 0),
			Name:         strings.TrimPrefix(colName, "@"),
		})
	}

	return properties, nil
}

func (m MetadataSource) GetSettings(settingsJson []byte) (adapters.Settings, error) {
	return NewSettings(settingsJson)
}

func (m MetadataSource) GetSchemaDiscoverer(log hclog.Logger) (adapters.SchemaDiscoverer, error) {
	return NewSchemaDiscoverer(log)
}

func (m MetadataSource) GetWriter(session *internal.OpSession, req *pub.PrepareWriteRequest) (adapters.Writer, error) {
	if req.Replication == nil {
		return NewDefaultWriteHandler(session, req)
	}

	return NewReplicationWriteHandler(session, req)
}

func makeSQLNameSafe(name string) string {
	if ok, _ := regexp.MatchString(`\W`, name); !ok {
		return name
	}
	return fmt.Sprintf("[%s]", name)
}

func decomposeSafeName(safeName string) (schema, name string) {
	segs := strings.Split(safeName, ".")
	switch len(segs) {
	case 0:
		return "", ""
	case 1:
		return "dbo", strings.Trim(segs[0], "[]")
	case 2:
		return strings.Trim(segs[0], "[]"), strings.Trim(segs[1], "[]")
	default:
		return "", ""
	}
}

func getSchemaID(schemaName, tableName string) string {
	if schemaName == "dbo" {
		return fmt.Sprintf("[%s]", tableName)
	} else {
		return fmt.Sprintf("[%s].[%s]", schemaName, tableName)
	}
}

var _ adapters.MetadataSource = &MetadataSource{}
