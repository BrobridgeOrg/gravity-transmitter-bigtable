package writer

import "fmt"

func (writer *Writer) Truncate(table string) error {

	// sqlStr := fmt.Sprintf(`TRUNCATE TABLE "%s"`, table)
	// _, err := writer.db.Exec(sqlStr)
	// if err != nil {
	// 	return err
	// }

	fmt.Println("Bigtable Truncate" + table)

	return nil
}
