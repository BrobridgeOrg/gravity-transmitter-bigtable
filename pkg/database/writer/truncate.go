package writer

import (
	"context"
)

func (writer *Writer) Truncate(table string) error {

	// Create ctx for bigtable writer
	ctx := context.Background()

	// Get bigtable admin client
	err := writer.adminclient.DropAllRows(ctx, table)

	// If Drop is error
	if err != nil {
		return err
	}

	return nil
}
