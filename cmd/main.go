// create csv file for inserting RDB
package main

import (
	// "github.com/gensan0223/create-test-csv/internal"
	"github.com/gensan0223/create-test-csv/open_search"
)

func main() {
	// create csv file
	// internal.CreateCsvFile()

	open_search.Bulk_insert_csv()
}
