package internal

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/LK4D4/joincontext"
	"github.com/hashicorp/go-hclog"
	jsonschema "github.com/naveego/go-json-schema"
	"github.com/naveego/plugin-pub-mssql/internal/adapters/mssql"
	"github.com/naveego/plugin-pub-mssql/internal/meta"
	"github.com/naveego/plugin-pub-mssql/internal/pub"
	"github.com/pkg/errors"
	"io"
	"io/ioutil"
	"os"
	"sync"
)

// Server type to describe a server
type Server struct {
	mu      *sync.Mutex
	log     hclog.Logger
	session *Session
	config  *Config
}


type Config struct {
	LogLevel hclog.Level
	// Directory where log files should be stored.
	LogDirectory string
	// Directory where the plugin can store data permanently.
	PermanentDirectory string
	// Directory where the plugin can store temporary information which may be deleted.
	TemporaryDirectory string
}

type Session struct {
	Ctx        context.Context
	Cancel     func()
	Publishing bool
	Log        hclog.Logger
	MetadataSource MetadataSource
	Settings   Settings
	Writer     Writer
	// tables that were discovered during connect
	SchemaInfo       map[string]*meta.Schema
	StoredProcedures []string
	RealTimeHelper   RealTimeHelper
	Config           Config
	DB               *sql.DB
	SchemaDiscoverer SchemaDiscoverer
}

type OpSession struct {
	Session
	// Cancel cancels the context in this operation.
	Cancel func()
	// Ctx is the context for this operation. It will
	// be done when the context from gRPC call is done,
	// the overall session context is cancelled (by a disconnect)
	// or Cancel is called on this OpSession instance.
	Ctx context.Context
}

func (s *Session) OpSession(ctx context.Context) *OpSession {
	ctx, cancel := joincontext.Join(s.Ctx, ctx)
	return &OpSession{
		Session: *s,
		Cancel:  cancel,
		Ctx:     ctx,
	}
}

// NewServer creates a new publisher Server.
func NewServer(logger hclog.Logger) pub.PublisherServer {

	manifestBytes, err := ioutil.ReadFile("manifest.json")
	if err != nil {
		panic(errors.Wrap(err, "manifest.json must be in plugin directory"))
	}
	var manifest map[string]interface{}
	err = json.Unmarshal(manifestBytes, &manifest)
	if err != nil {
		panic(errors.Wrap(err, "manifest.json was invalid"))
	}

	configSchema := manifest["configSchema"].(map[string]interface{})
	configSchemaSchema = configSchema["schema"].(map[string]interface{})
	configSchemaUI = configSchema["ui"].(map[string]interface{})
	b, _ := json.Marshal(configSchemaSchema)
	configSchemaSchemaJSON = string(b)
	b, _ = json.Marshal(configSchemaUI)
	configSchemaUIJSON = string(b)

	return &Server{
		mu:  &sync.Mutex{},
		log: logger,
	}
}

func (s *Server) Configure(ctx context.Context, req *pub.ConfigureRequest) (*pub.ConfigureResponse, error) {

	config := &Config{
		LogLevel:           hclog.LevelFromString(req.LogLevel.String()),
		TemporaryDirectory: req.TemporaryDirectory,
		PermanentDirectory: req.PermanentDirectory,
		LogDirectory:       req.LogDirectory,
	}

	s.log.SetLevel(config.LogLevel)

	err := os.MkdirAll(config.PermanentDirectory, 0700)
	if err != nil {
		return nil, errors.Wrap(err, "ensure permanent directory available")
	}

	err = os.MkdirAll(config.TemporaryDirectory, 0700)
	if err != nil {
		return nil, errors.Wrap(err, "ensure temporary directory available")
	}

	s.config = config

	return new(pub.ConfigureResponse), nil
}

func (s *Server) getConfig() (Config, error) {
	if s.config == nil {
		_, err := s.Configure(context.Background(), &pub.ConfigureRequest{
			LogDirectory:       "",
			LogLevel:           pub.LogLevel_Info,
			PermanentDirectory: "./data",
			TemporaryDirectory: "./temp",
		})
		if err != nil {
			return Config{}, err
		}
	}

	return *s.config, nil
}

var configSchemaUI map[string]interface{}
var configSchemaUIJSON string
var configSchemaSchema map[string]interface{}
var configSchemaSchemaJSON string

// Connect connects to the data base and validates the connections
func (s *Server) Connect(ctx context.Context, req *pub.ConnectRequest) (*pub.ConnectResponse, error) {
	var err error
	s.log.Debug("Connecting...")
	s.disconnect()

	s.mu.Lock()
	defer s.mu.Unlock()

	session := &Session{
		Log:        s.log,
		SchemaInfo: map[string]*meta.Schema{},
	}

	session.Config, err = s.getConfig()
	if err != nil {
		return nil, err
	}

	session.Ctx, session.Cancel = context.WithCancel(context.Background())

	switch Driver {
	case MSSQLDriver:
		metadataSource, err := mssql.NewMetadataSource()
		if err != nil {

		}

		session.MetadataSource = metadataSource
		break
	case SnowflakeDriver:
		//metadataSource, err := snowflake.NewMetadataSource()
		//if err != nil {
		//
		//}
		//
		//session.MetadataSource = metadataSource
		break
	}

	session.Settings, err = session.MetadataSource.GetSettings([]byte(req.SettingsJson))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if err := session.Settings.Validate(); err != nil {
		return nil, errors.WithStack(err)
	}

	connectionString, err := session.Settings.GetConnectionString()
	if err != nil {
		return nil, err
	}

	session.DB, err = sql.Open(Driver, connectionString)
	if err != nil {
		return nil, errors.Errorf("could not open connection: %s", err)
	}

	session.SchemaInfo , err = session.MetadataSource.GetSchemaInfoMap(session.DB)
	if err != nil {
		return nil, errors.Wrap(err, "could not read table schemas")
	}

	session.StoredProcedures , err = session.MetadataSource.GetStoredProcedures(session.DB)
	if err != nil {
		return nil, errors.Wrap(err, "could not read stored procedures")
	}

	session.SchemaDiscoverer, err = session.MetadataSource.GetSchemaDiscoverer(s.log.With("cmp", "SchemaDiscoverer"))
	if err != nil {
		return nil, errors.Wrap(err, "could not create schema discoverer")
	}

	s.session = session

	s.log.Debug("Connect completed successfully.")

	return new(pub.ConnectResponse), err
}

func (s *Server) ConnectSession(*pub.ConnectRequest, pub.Publisher_ConnectSessionServer) error {
	return errors.New("Not supported.")
}

func (s *Server) ConfigureConnection(ctx context.Context, req *pub.ConfigureConnectionRequest) (*pub.ConfigureConnectionResponse, error) {
	return &pub.ConfigureConnectionResponse{
		Form: &pub.ConfigurationFormResponse{
			DataJson:   req.Form.DataJson,
			StateJson:  req.Form.StateJson,
			SchemaJson: configSchemaSchemaJSON,
			UiJson:     configSchemaUIJSON,
		},
	}, nil
}

func (s *Server) ConfigureQuery(ctx context.Context, req *pub.ConfigureQueryRequest) (*pub.ConfigureQueryResponse, error) {
	return nil, errors.New("Not implemented.")
}

func (s *Server) ConfigureRealTime(ctx context.Context, req *pub.ConfigureRealTimeRequest) (*pub.ConfigureRealTimeResponse, error) {

	session, err := s.getOpSession(ctx)
	if err != nil {
		return nil, err
	}

	if req.Form == nil {
		req.Form = &pub.ConfigurationFormRequest{}
	}

	if session.RealTimeHelper == nil {
		session.RealTimeHelper, err = session.MetadataSource.GetRealTimeHelper()
	}

	ctx, cancel := context.WithCancel(session.Ctx)
	defer cancel()
	resp, err := session.RealTimeHelper.ConfigureRealTime(session, req)

	return resp, err
}

func (s *Server) BeginOAuthFlow(ctx context.Context, req *pub.BeginOAuthFlowRequest) (*pub.BeginOAuthFlowResponse, error) {
	return nil, errors.New("Not supported.")
}

func (s *Server) CompleteOAuthFlow(ctx context.Context, req *pub.CompleteOAuthFlowRequest) (*pub.CompleteOAuthFlowResponse, error) {
	return nil, errors.New("Not supported.")
}

func (s *Server) DiscoverSchemasStream(req *pub.DiscoverSchemasRequest, stream pub.Publisher_DiscoverSchemasStreamServer) error {
	s.log.Debug("Handling DiscoverSchemasStream...")

	session, err := s.getOpSession(stream.Context())
	if err != nil {
		return err
	}

	discovered, err := session.SchemaDiscoverer.DiscoverSchemas(session, req)
	if err != nil {
		return err
	}

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()

		case schema, more := <-discovered:
			if !more {
				s.log.Info("Reached end of schema stream.")
				return nil
			}

			s.log.Debug("Discovered schema.", "schema", schema.Name)

			err = stream.Send(schema)
			if err != nil {
				return err
			}
		}
	}
}

func (s *Server) DiscoverSchemas(ctx context.Context, req *pub.DiscoverSchemasRequest) (*pub.DiscoverSchemasResponse, error) {

	s.log.Debug("Handling DiscoverSchemasRequest...")

	session, err := s.getOpSession(ctx)
	if err != nil {
		return nil, err
	}

	schemas, err := session.SchemaDiscoverer.DiscoverSchemasSync(session, req)

	return &pub.DiscoverSchemasResponse{
		Schemas: schemas,
	}, err
}

func (s *Server) ReadStream(req *pub.ReadRequest, stream pub.Publisher_ReadStreamServer) error {

	session, err := s.getOpSession(context.Background())
	if err != nil {
		return err
	}

	defer session.Cancel()

	jsonReq, _ := json.Marshal(req)

	s.log.Debug("Got PublishStream request.", "req", string(jsonReq))

	if session.Settings.GetPrePublishQuery() != "" {
		_, err := session.DB.Exec(session.Settings.GetPrePublishQuery())
		if err != nil {
			return errors.Errorf("error running pre-publish query: %s", err)
		}
	}

	handler, innerRequest, err := BuildHandlerAndRequest(session, req, PublishToStreamHandler(session.Ctx, stream))
	if err != nil {
		return errors.Wrap(err, "create handler")
	}

	err = handler.Handle(innerRequest)

	if session.Settings.GetPostPublishQuery() != "" {
		_, postPublishErr := session.DB.Exec(session.Settings.GetPostPublishQuery())
		if postPublishErr != nil {
			if err != nil {
				postPublishErr = errors.Errorf("%s (publish had already stopped with error: %s)", postPublishErr, err)
			}

			return errors.Errorf("error running post-publish query: %s", postPublishErr)
		}
	}

	if err != nil && session.Ctx.Err() != nil {
		s.log.Error("Handler returned error, but context was cancelled so error will not be returned to caller. Error was: %s", err.Error())
		return nil
	}

	return err
}

// ConfigureWrite
func (s *Server) ConfigureWrite(ctx context.Context, req *pub.ConfigureWriteRequest) (*pub.ConfigureWriteResponse, error) {
	session, err := s.getOpSession(ctx)
	if err != nil {
		return nil, err
	}

	var errArray []string

	storedProcedures, _ := json.Marshal(session.StoredProcedures)
	schemaJSON := fmt.Sprintf(`{
	"type": "object",
	"properties": {
		"storedProcedure": {
			"type": "string",
			"title": "Stored Procedure Name",
			"description": "The name of the stored procedure",
			"enum": %s
		}
	},
	"required": [
		"storedProcedure"
	]
}`, storedProcedures)

	// first request return ui json schema form
	if req.Form == nil || req.Form.DataJson == "" {
		return &pub.ConfigureWriteResponse{
			Form: &pub.ConfigurationFormResponse{
				DataJson:       `{"storedProcedure":""}`,
				DataErrorsJson: "",
				Errors:         nil,
				SchemaJson:     schemaJSON,
				StateJson:      "",
			},
			Schema: nil,
		}, nil
	}

	var properties []*pub.Property

	// get form data
	var formData ConfigureWriteFormData
	if err := json.Unmarshal([]byte(req.Form.DataJson), &formData); err != nil {
		errArray = append(errArray, fmt.Sprintf("error reading form data: %s", err))
		goto Done
	}

	properties, err = session.MetadataSource.GetStoredProcedureProperties(session.DB, formData)
	if err != nil {
		return nil, errors.Wrap(err, "could not read stored procedure properties")
	}

Done:
	// return write back schema
	return &pub.ConfigureWriteResponse{
		Form: &pub.ConfigurationFormResponse{
			DataJson:   req.Form.DataJson,
			Errors:     errArray,
			StateJson:  req.Form.StateJson,
			SchemaJson: schemaJSON,
		},
		Schema: &pub.Schema{
			Id:                formData.StoredProcedure,
			Query:             formData.StoredProcedure,
			DataFlowDirection: pub.Schema_WRITE,
			Properties:        properties,
		},
	}, nil
}

type ConfigureWriteFormData struct {
	StoredProcedure string `json:"storedProcedure,omitempty"`
}


func (s *Server) ConfigureReplication(ctx context.Context, req *pub.ConfigureReplicationRequest) (*pub.ConfigureReplicationResponse, error) {
	builder := pub.NewConfigurationFormResponseBuilder(req.Form)

	s.log.Debug("Handling configure replication request.")

	var settings ReplicationSettings
	if req.Form.DataJson != "" {
		if err := json.Unmarshal([]byte(req.Form.DataJson), &settings); err != nil {
			return nil, errors.Wrapf(err, "invalid data json %q", req.Form.DataJson)
		}

		s.log.Debug("Configure replication request had data.", "data", req.Form.DataJson)


		if req.Schema != nil {
			s.log.Debug("Configure replication request had a schema.", "schema", req.Schema)
		}
		if req.Form.IsSave {
			s.log.Debug("Configure replication request was a save.")
		}

		if settings.SQLSchema != "" &&
			settings.VersionRecordTable != "" &&
			settings.GoldenRecordTable != "" &&
			req.Schema != nil &&
			req.Form.IsSave {

			s.log.Info("Configure replication request had IsSave=true, committing replication settings to database.")

			// The settings have been filled in, let's make sure it's ready to go.

			session, err := s.getOpSession(ctx)
			if err != nil {
				return nil, err
			}

			_, err = session.MetadataSource.GetWriter(session, &pub.PrepareWriteRequest{
				Schema:req.Schema,
				Replication: &pub.ReplicationWriteRequest{
				SettingsJson:req.Form.DataJson,
				Versions: req.Versions,
				},
			})
			if err != nil {
				s.log.Error("Configuring replication failed.", "req", string(req.Form.DataJson), "err", err)
				builder.Response.Errors = []string{err.Error()}
			}
		}
	}

	builder.UISchema = map[string]interface{}{
		"ui:order":[]string{"sqlSchema","goldenRecordTable", "versionRecordTable"},
	}
	builder.FormSchema = jsonschema.NewGenerator().WithRoot(ReplicationSettings{}).MustGenerate()

	return &pub.ConfigureReplicationResponse{
		Form:builder.Build(),
	}, nil
}

// PrepareWrite sets up the plugin to be able to write back
func (s *Server) PrepareWrite(ctx context.Context, req *pub.PrepareWriteRequest) (*pub.PrepareWriteResponse, error) {
	session, err := s.getOpSession(ctx)
	if err != nil {
		return nil, err
	}

	s.session.Writer, err = session.MetadataSource.GetWriter(session, req)
	if err != nil {
		return nil, err
	}

	schemaJSON, _ := json.MarshalIndent(req.Schema, "", "  ")
	s.log.Debug("Prepared to write.", "commitSLA", req.CommitSlaSeconds, "schema", string(schemaJSON))

	return &pub.PrepareWriteResponse{}, nil
}

// WriteStream writes a stream of records back to the source system
func (s *Server) WriteStream(stream pub.Publisher_WriteStreamServer) error {

	session, err := s.getOpSession(stream.Context())
	if err != nil {
		return err
	}

	if session.Writer == nil {
		return errors.New("session.Writer was nil, PrepareWrite should have been called before WriteStream")
	}

	defer session.Cancel()

	// get and process each record
	for {

		if session.Ctx.Err() != nil {
			return nil
		}

		// get record and exit if no more records or error
		record, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		unmarshalledRecord, err := record.AsUnmarshalled()

		// if the record unmarshalled correctly,
		// we send it to the writer
		if err == nil {
			err = session.Writer.Write(session, unmarshalledRecord)
		}

		if err != nil {
			// send failure ack to agent
			ack := &pub.RecordAck{
				CorrelationId: record.CorrelationId,
				Error:         err.Error(),
			}
			err := stream.Send(ack)
			if err != nil {
				s.log.Error("Error writing error ack to agent.", "err", err, "ack", ack)
				return err
			}
		} else {
			// send success ack to agent
			err := stream.Send(&pub.RecordAck{
				CorrelationId: record.CorrelationId,
			})
			if err != nil {
				s.log.Error("Error writing success ack to agent.", "err", err)
				return err
			}
		}
	}
}

// DiscoverShapes discovers shapes present in the database
func (s *Server) DiscoverShapes(ctx context.Context, req *pub.DiscoverSchemasRequest) (*pub.DiscoverSchemasResponse, error) {
	return s.DiscoverSchemas(ctx, req)
}

// PublishStream sends records read in request to the agent
func (s *Server) PublishStream(req *pub.ReadRequest, stream pub.Publisher_PublishStreamServer) error {
	return s.ReadStream(req, stream)
}

// Disconnect disconnects from the server
func (s *Server) Disconnect(ctx context.Context, req *pub.DisconnectRequest) (*pub.DisconnectResponse, error) {

	s.disconnect()

	return new(pub.DisconnectResponse), nil
}

func (s *Server) disconnect() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.session != nil {
		s.session.Cancel()
		if s.session.DB != nil {
			err := s.session.DB.Close()
			if err != nil {
				s.log.Error("Error closing connection", "err", err)
			}
		}
	}

	s.session = nil
}

func (s *Server) getOpSession(ctx context.Context) (*OpSession, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.session == nil {
		return nil, errors.New("not connected")
	}

	if s.session.Ctx != nil && s.session.Ctx.Err() != nil {
		return nil, s.session.Ctx.Err()
	}

	return s.session.OpSession(ctx), nil
}

//var errNotConnected = errors.New("not connected")
//
//func formatTypeAtSource(t string, maxLength, precision, scale int) string {
//	var maxLengthString string
//	if maxLength < 0 {
//		maxLengthString = "MAX"
//	} else {
//		maxLengthString = fmt.Sprintf("%d", maxLength)
//	}
//
//	switch t {
//	case "char", "varchar", "nvarchar", "nchar", "binary", "varbinary", "text", "ntext":
//		return fmt.Sprintf("%s(%s)", t, maxLengthString)
//	case "decimal", "numeric":
//		return fmt.Sprintf("%s(%d,%d)", t, precision, scale)
//	case "float", "real":
//		return fmt.Sprintf("%s(%d)", t, precision)
//	case "datetime2":
//		return fmt.Sprintf("%s(%d)", t, scale)
//	default:
//		return t
//	}
//}
