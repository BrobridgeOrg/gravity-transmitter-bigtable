package writer

import (
	"context"
	"encoding/binary"
	"fmt"
	"strconv"

	"cloud.google.com/go/bigtable"
	transmitter "github.com/BrobridgeOrg/gravity-api/service/transmitter"
	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	tableName        = "Gravity-demo"
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
	writer.dbInfo.ProjectID = viper.GetString("gcp.instance_id")
	writer.dbInfo.InstanceID = viper.GetString("gcp.project_id")

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

// WTFF is this ??

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

// Method you can choose

func (writer *Writer) InsertRecord(record *transmitter.Record) error {

	// Create ctx for bigtable writer
	ctx := context.Background()

	// Get bigtable client
	tbl := writer.db.Open(record.Table)

	// Allocate muts and row
	muts := make([]*bigtable.Mutation, len(record.Fields))
	rowKeys := make([]string, len(record.Fields))

	fmt.Println("Start sending data to BigTable ...")

	// Read Table data from gRPC
	for i, field := range record.Fields {

		muts[i] = bigtable.NewMutation()
		muts[i].Set(columnFamilyName, field.Name, bigtable.Now(), []byte(fmt.Sprintf("%v", writer.GetValue(field.Value))))

		if field.IsPrimary {
			rowKeys[i] = field.Name
		}

	}

	// Writing ...
	rowErrs, err := tbl.ApplyBulk(ctx, rowKeys, muts)

	if err != nil {
		log.Fatalf("Could not apply bulk row mutation: %v", err)
	}

	if rowErrs != nil {
		for _, rowErr := range rowErrs {
			log.Printf("Error writing row: %v", rowErr)
		}
		log.Fatalf("Could not write some rows")
	}

	// Fred under this line
	recordDef := writer.GetDefinition(record)

	return writer.insert(record.Table, recordDef)
}

func (writer *Writer) UpdateRecord(record *transmitter.Record) error {

	return nil
}

func (writer *Writer) DeleteRecord(record *transmitter.Record) error {

	return nil
}

func (writer *Writer) update(table string, recordDef *RecordDef) (bool, error) {

	return false, nil
}

func (writer *Writer) insert(table string, recordDef *RecordDef) error {

	paramLength := len(recordDef.ColumnDefs) + 1

	// Allocation
	colNames := make([]string, 0, paramLength)
	colNames = append(colNames, recordDef.PrimaryColumn)
	valNames := make([]string, 0, paramLength)
	valNames = append(valNames, ":primary_val")

	// Preparing columns and bindings
	for _, def := range recordDef.ColumnDefs {
		colNames = append(colNames, `"`+def.ColumnName+`"`)
		valNames = append(valNames, `:`+def.BindingName)
	}

	return nil
}

func insertBigTable() {

}
