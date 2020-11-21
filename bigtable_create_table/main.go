package main

// [START bigtable_hw_imports]
import (
	"context"
	"log"

	"cloud.google.com/go/bigtable"
)

const (
	tableName        = "gravity-demo"
	columnFamilyName = "gravity-cf"
)

// sliceContains reports whether the provided string is present in the given slice of strings.
func sliceContains(list []string, target string) bool {
	for _, s := range list {
		if s == target {
			return true
		}
	}
	return false
}

func main() {

	// This area provide you to change your setting
	/***********************************************************************************************/

	// project := flag.String("project", "", "The Google Cloud Platform project ID. Required.")
	// instance := flag.String("instance", "", "The Google Cloud Bigtable instance ID. Required.")
	// flag.Parse()

	// for _, f := range []string{"project", "instance"} {
	// 	if flag.Lookup(f).Value.String() == "" {
	// 		log.Fatalf("The %s flag is required.", f)
	// 	}
	// }

	/***********************************************************************************************/

	ctx := context.Background()

	// Set up admin client, tables, and column families.
	// NewAdminClient uses Application Default Credentials to authenticate.
	// [START bigtable_hw_connect]
	adminClient, err := bigtable.NewAdminClient(ctx, "gravity-bigtable", "gravity")
	if err != nil {
		log.Fatalf("Could not create admin client: %v", err)
	}
	// [END bigtable_hw_connect]

	// [START bigtable_hw_create_table]
	tables, err := adminClient.Tables(ctx)
	if err != nil {
		log.Fatalf("Could not fetch table list: %v", err)
	}

	if !sliceContains(tables, tableName) {
		log.Printf("Creating table %s", tableName)
		if err := adminClient.CreateTable(ctx, tableName); err != nil {
			log.Fatalf("Could not create table %s: %v", tableName, err)
		}
	}

	tblInfo, err := adminClient.TableInfo(ctx, tableName)
	if err != nil {
		log.Fatalf("Could not read info for table %s: %v", tableName, err)
	}

	if !sliceContains(tblInfo.Families, columnFamilyName) {
		if err := adminClient.CreateColumnFamily(ctx, tableName, columnFamilyName); err != nil {
			log.Fatalf("Could not create column family %s: %v", columnFamilyName, err)
		}
	}
}
