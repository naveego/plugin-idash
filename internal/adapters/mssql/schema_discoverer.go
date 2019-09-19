package mssql

import (
	"context"
	"fmt"
	"github.com/hashicorp/go-hclog"
	"github.com/naveego/plugin-pub-mssql/internal"
	"github.com/naveego/plugin-pub-mssql/internal/adapters"
	"github.com/naveego/plugin-pub-mssql/internal/pub"
	"github.com/pkg/errors"
	"regexp"
	"sort"
	"sync"
	"time"
)

type SchemaDiscoverer struct {
	Log hclog.Logger
}

func NewSchemaDiscoverer(log hclog.Logger) (adapters.SchemaDiscoverer, error) {
	return &SchemaDiscoverer{
		Log: log,
	}, nil
}

func (s SchemaDiscoverer) DiscoverSchemas(session *internal.OpSession, req *pub.DiscoverSchemasRequest) (<-chan *pub.Schema, error) {
	var err error
	var schemas []*pub.Schema

	if req.Mode == pub.DiscoverSchemasRequest_ALL {
		s.Log.Debug("Discovering all tables and views...")
		schemas, err = getAllSchemas(session)
		s.Log.Debug("Discovered tables and views.", "count", len(schemas))

		if err != nil {
			return nil, errors.Errorf("could not load tables and views from SQL: %s", err)
		}
	} else {
		s.Log.Debug("Refreshing schemas from request.", "count", len(req.ToRefresh))
		for _, s := range req.ToRefresh {
			schemas = append(schemas, s)
		}
	}

	resp := &pub.DiscoverSchemasResponse{}

	out := make(chan *pub.Schema)

	sort.Sort(pub.SortableShapes(resp.Schemas))

	go func() {
		wait := new(sync.WaitGroup)

		defer close(out)

		for i := range schemas {

			wait.Add(1)

			// concurrently get details for shape
			go func(schema *pub.Schema) {

				defer func() {
					out <- schema
					wait.Done()
				}()

				s.Log.Debug("Got details for discovered schema", "id", schema.Id)

				if req.Mode == pub.DiscoverSchemasRequest_REFRESH {
					s.Log.Debug("Getting count for discovered schema", "id", schema.Id)
					schema.Count, err = getCount(session, schema)
					if err != nil {
						s.Log.With("shape", schema.Id).With("err", err).Error("Error getting row count.")
						schema.Errors = append(schema.Errors, fmt.Sprintf("Could not get row count for shape: %s", err))
						return
					}
					s.Log.Debug("Got count for discovered schema", "id", schema.Id, "count", schema.Count.String())
				} else {
					schema.Count = &pub.Count{Kind: pub.Count_UNAVAILABLE}
				}

				// TODO: After publisher pulled out
				if req.SampleSize > 0 {
					s.Log.Debug("Getting sample for discovered schema", "id", schema.Id, "size", req.SampleSize)
					//publishReq := &pub.ReadRequest{
					//	Schema: schema,
					//	Limit:  req.SampleSize,
					//}

					//collector := new(RecordCollector)
					//handler, innerRequest, err := BuildHandlerAndRequest(session, publishReq, collector)
					//if err != nil {
					//	s.Log.With("shape", schema.Id).With("err", err).Error("Error getting sample.")
					//	schema.Errors = append(schema.Errors, fmt.Sprintf("Could not get sample for shape: %s", err))
					//	return
					//}
					//
					//err = handler.Handle(innerRequest)
					//
					//for _, record := range collector.Records {
					//	schema.Sample = append(schema.Sample, record)
					//}

					if err != nil {
						s.Log.With("shape", schema.Id).With("err", err).Error("Error collecting sample.")
						schema.Errors = append(schema.Errors, fmt.Sprintf("Could not collect sample: %s", err))
						return
					}
					s.Log.Debug("Got sample for discovered schema", "id", schema.Id, "size", len(schema.Sample))
				}

			}(schemas[i])
		}

		// wait until all concurrent shape details have been loaded
		wait.Wait()

	}()

	return out, nil
}

func (s SchemaDiscoverer) DiscoverSchemasSync(session *internal.OpSession, req *pub.DiscoverSchemasRequest) ([]*pub.Schema, error) {
	discovered, err := s.DiscoverSchemas(session, req)
	if err != nil {
		return nil, err
	}

	ctx := session.Ctx

	var schemas []*pub.Schema

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()

		case schema, more := <-discovered:
			if !more {
				sort.Sort(pub.SortableShapes(schemas))
				return schemas, nil
			}

			schemas = append(schemas, schema)
		}
	}
}

func getAllSchemas(session *internal.OpSession) ([]*pub.Schema, error) {
	var schemas []*pub.Schema

	for _, metaSchema := range session.SchemaInfo {
		schema := new(pub.Schema)

		var (
			schemaName string
			tableName  string
		)
		schemaName, tableName = decomposeSafeName(metaSchema.ID)

		if schemaName == "dbo" {
			schema.Id = getSchemaID(schemaName, tableName)
			schema.Name = tableName
		} else {
			schema.Id = getSchemaID(schemaName, tableName)
			schema.Name = fmt.Sprintf("%s.%s", schemaName, tableName)
		}

		schema.DataFlowDirection = pub.Schema_READ

		var properties []*pub.Property
		for _, column := range metaSchema.Columns() {
			property := &pub.Property{
				Id:                   column.ID,
				Name:                 column.OpaqueName(),
				Description:          "",
				Type:                 ConvertSQLTypeToPluginType(column.SQLType, 0),
				IsKey:                column.IsKey,
				IsCreateCounter:      false,
				IsUpdateCounter:      false,
				PublisherMetaJson:    "",
				TypeAtSource:         column.SQLType,
				IsNullable:           column.IsKey,
			}

			properties = append(properties, property)
		}

		schema.Properties = properties

		schemas = append(schemas, schema)
	}

	return schemas, nil
}

var schemaIDParseRE = regexp.MustCompile(`(?:\[([^\]]+)\].)?(?:)(?:\[([^\]]+)\])`)

func getCount(session *internal.OpSession, schema *pub.Schema) (*pub.Count, error) {

	var query string
	var err error

	schemaInfo := session.SchemaInfo[schema.Id]

	if schema.Query != "" {
		query = fmt.Sprintf("SELECT COUNT(1) FROM (%s) as Q", schema.Query)
	} else if schemaInfo == nil || !schemaInfo.IsTable {
		return &pub.Count{Kind: pub.Count_UNAVAILABLE}, nil
	} else {
		segs := schemaIDParseRE.FindStringSubmatch(schema.Id)
		if segs == nil {
			return nil, fmt.Errorf("malformed schema ID %q", schema.Id)
		}

		schema, table := segs[1], segs[2]
		if schema == "" {
			schema = "dbo"
		}

		query = fmt.Sprintf(`
			SELECT SUM(p.rows) FROM sys.partitions AS p
			INNER JOIN sys.tables AS t
			ON p.[object_id] = t.[object_id]
			INNER JOIN sys.schemas AS s
			ON s.[schema_id] = t.[schema_id]
			WHERE t.name = N'%s'
			AND s.name = N'%s'
			AND p.index_id IN (0,1);`, table, schema)
	}

	ctx, cancel := context.WithTimeout(session.Ctx, time.Second)
	defer cancel()
	row := session.DB.QueryRowContext(ctx, query)
	var count int
	err = row.Scan(&count)
	if err != nil {
		return nil, fmt.Errorf("error from query %q: %s", query, err)
	}

	return &pub.Count{
		Kind:  pub.Count_EXACT,
		Value: int32(count),
	}, nil
}

var _ adapters.SchemaDiscoverer = &SchemaDiscoverer{}
