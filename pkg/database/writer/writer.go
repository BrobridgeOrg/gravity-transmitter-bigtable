package writer

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strconv"

	"cloud.google.com/go/bigtable"
	transmitter "github.com/BrobridgeOrg/gravity-api/service/transmitter"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	columnFamilyName = "gravity-cf"
)

type DatabaseInfo struct {
	ProjectID  string `json:"projectid"`
	InstanceID string `json:"instanceid"`
}

type RecordDef struct {
	HasPrimary    bool
	PrimaryColumn string
	Values        map[string]interface{}
	ColumnDefs    []*ColumnDef
}

type ColumnDef struct {
	ColumnName  string
	BindingName string
	Value       interface{}
}

type DBCommand struct {
	QueryStr string
	Args     map[string]interface{}
}

type Writer struct {
	dbInfo      *DatabaseInfo
	adminclient *bigtable.AdminClient
	db          *bigtable.Client
	commands    chan *DBCommand
}

func NewWriter() *Writer {
	return &Writer{
		dbInfo:   &DatabaseInfo{},
		commands: make(chan *DBCommand, 2048),
	}
}

func (writer *Writer) Init() error {

	// Read configuration file
	writer.dbInfo.ProjectID = viper.GetString("gcp.project_id")
	writer.dbInfo.InstanceID = viper.GetString("gcp.instance_id")

	// Bigtable connection
	ctx := context.Background()

	// Create Bigtable Admin
	adminClient, err := bigtable.NewAdminClient(ctx, writer.dbInfo.ProjectID, writer.dbInfo.InstanceID)
	if err != nil {
		log.Fatalf("Could not create admin client: %v", err)
	}

	// Create BigTable Operator Instance
	db, err := bigtable.NewClient(ctx, writer.dbInfo.ProjectID, writer.dbInfo.InstanceID)
	if err != nil {
		log.Fatalf("Could not create db client: %v", err)
	}

	log.WithFields(log.Fields{
		"instance-id": writer.dbInfo.InstanceID,
		"project-id":  writer.dbInfo.ProjectID,
	}).Info("Connecting to GCP bigtable database")

	writer.adminclient = adminClient
	writer.db = db
	return nil
}

func (writer *Writer) run() {

	// Dagin Note
	// Notice that in BigTable we using GCP-SDK don't need to run SQL query

	// for {
	// 	select {
	// 	case cmd := <-writer.commands:
	// 		_, err := writer.db.NamedExec(cmd.QueryStr, cmd.Args)
	// 		if err != nil {
	// 			log.Error(err)
	// 		}
	// 	}
	// }

}

func (writer *Writer) ProcessData(record *transmitter.Record) error {

	log.WithFields(log.Fields{
		"method": record.Method,
		"event":  record.EventName,
		"table":  record.Table,
	}).Info("Write record")

	switch record.Method {
	case transmitter.Method_DELETE:
		return writer.DeleteRecord(record)
	case transmitter.Method_UPDATE:
		return writer.UpdateRecord(record)
	case transmitter.Method_INSERT:
		return writer.InsertRecord(record)
	}

	return nil
}

// Method you can choose

func (writer *Writer) InsertRecord(record *transmitter.Record) error {

	// Get record define
	recordDef := writer.GetDefinition(record)

	// Keep recordDef but not using beacause SDK implement that
	return writer.insertBigTable(record, recordDef)
}

func (writer *Writer) UpdateRecord(record *transmitter.Record) error {

	// Get record define
	recordDef := writer.GetDefinition(record)

	// Keep recordDef but not using beacause SDK implement that
	return writer.updateBigTable(record, recordDef)
}

func (writer *Writer) DeleteRecord(record *transmitter.Record) error {

	// Create ctx for bigtable writer
	ctx := context.Background()

	// Get bigtable client
	tbl := writer.db.Open(record.Table)

	// Allocate BigTable data structure
	var rowKey interface{}
	mut := bigtable.NewMutation()

	for _, field := range record.Fields {
		// Primary key
		if field.IsPrimary {
			rowKey = writer.GetValue(field.Value)
			break
		}
	}

	// Set delete
	mut.DeleteRow()

	// Assert for rowKey and write to BigTable
	err := tbl.Apply(ctx, rowKey.(string), mut)

	// BigTable write error
	if err != nil {
		return err
	}

	return nil
}

func (writer *Writer) updateBigTable(record *transmitter.Record, recordDef *RecordDef) error {

	// Create ctx for bigtable writer
	ctx := context.Background()

	// Get bigtable client
	tbl := writer.db.Open(record.Table)

	// Allocate BigTable data structure
	var rowKey interface{}
	mut := bigtable.NewMutation()
	buf := new(bytes.Buffer)

	// Indexing from gRPC
	for _, field := range record.Fields {
		if field.IsPrimary {
			rowKey = writer.GetValue(field.Value)
			continue
		}

		// Transfer data to byte
		err := binary.Write(buf, binary.BigEndian, writer.GetValue(field.Value))

		if err != nil {
			return err
		}

		mut.Set(columnFamilyName, field.Name, bigtable.Now(), buf.Bytes())
	}

	// Assert for rowKey and write to BigTable
	err := tbl.Apply(ctx, rowKey.(string), mut)

	// BigTable write error
	if err != nil {
		return err
	}

	return nil
}

func (writer *Writer) insertBigTable(record *transmitter.Record, recordDef *RecordDef) error {

	// Create ctx for bigtable writer
	ctx := context.Background()

	// Get bigtable client
	tbl := writer.db.Open(record.Table)

	// Allocate BigTable data structure
	var rowKey interface{}
	mut := bigtable.NewMutation()
	buf := new(bytes.Buffer)

	// Indexing from gRPC
	for _, field := range record.Fields {
		if field.IsPrimary {
			rowKey = writer.GetValue(field.Value)
			continue
		}

		// Transfer data to byte
		err := binary.Write(buf, binary.BigEndian, writer.GetValue(field.Value))

		if err != nil {
			return err
		}

		mut.Set(columnFamilyName, field.Name, bigtable.Now(), buf.Bytes())
	}

	// Assert for rowKey and write to BigTable
	err := tbl.Apply(ctx, rowKey.(string), mut)

	// BigTable write error
	if err != nil {
		return err
	}

	return nil
}

func (writer *Writer) GetValue(value *transmitter.Value) interface{} {

	switch value.Type {
	case transmitter.DataType_FLOAT64:
		return float64(binary.LittleEndian.Uint64(value.Value))
	case transmitter.DataType_INT64:
		return int64(binary.LittleEndian.Uint64(value.Value))
	case transmitter.DataType_UINT64:
		return uint64(binary.LittleEndian.Uint64(value.Value))
	case transmitter.DataType_BOOLEAN:
		return int8(value.Value[0]) & 1
	case transmitter.DataType_STRING:
		return string(value.Value)
	}

	// binary
	return value.Value
}

func (writer *Writer) GetDefinition(record *transmitter.Record) *RecordDef {

	recordDef := &RecordDef{
		HasPrimary: false,
		Values:     make(map[string]interface{}),
		ColumnDefs: make([]*ColumnDef, 0, len(record.Fields)),
	}

	// Scanning fields
	for n, field := range record.Fields {

		value := writer.GetValue(field.Value)

		// Primary key
		if field.IsPrimary == true {
			recordDef.Values["primary_val"] = value
			recordDef.HasPrimary = true
			recordDef.PrimaryColumn = field.Name
			continue
		}

		// Generate binding name
		bindingName := fmt.Sprintf("val_%s", strconv.Itoa(n))
		recordDef.Values[bindingName] = value

		// Store definition
		recordDef.ColumnDefs = append(recordDef.ColumnDefs, &ColumnDef{
			ColumnName:  field.Name,
			Value:       field.Name,
			BindingName: bindingName,
		})
	}

	return recordDef
}
