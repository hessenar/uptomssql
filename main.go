package main

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
	_ "github.com/microsoft/go-mssqldb"
)

type ColumnSchema struct {
	ColumnName    string         `db:"COLUMN_NAME"`
	IsNullable    string         `db:"IS_NULLABLE"`
	ColumnDefault sql.NullString `db:"COLUMN_DEFAULT"`
	DataType      string         `db:"DATA_TYPE"`
}

type Format = int
type AppExitCode = int

const (
	Json Format = iota
	Csv
)

const (
	SuccessCode AppExitCode = iota

	ConnectErrorCode
	TableInfoErrorCode
	InsertDataErrorCode
	UnmarshalErrorCode

	ReadDirErrorCode
	ReadFileErrorCode
	OpenFileErrorCode
)

var exitCodeDescription = map[AppExitCode]string{
	SuccessCode:         "success",
	ConnectErrorCode:    "error on connect to db",
	TableInfoErrorCode:  "error on get table info",
	InsertDataErrorCode: "error on data insert in table",
	UnmarshalErrorCode:  "error on unmarshal inserted data",
	ReadDirErrorCode:    "error on read dir",
	ReadFileErrorCode:   "error on read file",
	OpenFileErrorCode:   "error on open file",
}

func handleError(err error, errorCode AppExitCode) {
	if err != nil {
		fmt.Println(fmt.Errorf("%s: %w", exitCodeDescription[errorCode], err).Error())
		os.Exit(errorCode)
	}
}

func try(err error) {
	if err != nil {
		panic(err)
	}
}

func getFileFormat(strFormat string) Format {
	if strFormat == "json" {
		return Json
	} else if strFormat == "csv" {
		return Csv
	} else {
		panic("incorrect format")
	}
}

func getTableSchema(db *sqlx.DB, tableName string) (map[string]ColumnSchema, error) {
	query := `
SELECT COLUMN_NAME, IS_NULLABLE, COLUMN_DEFAULT, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = @p1`

	var cols []ColumnSchema
	if err := db.Select(&cols, query, tableName); err != nil {
		return nil, err
	}

	schema := make(map[string]ColumnSchema)
	for _, col := range cols {
		schema[col.ColumnName] = col
	}
	return schema, nil
}

func isTableHasIdentity(db *sqlx.DB, tableName string) (bool, error) {
	query := `
SELECT Count(*)
FROM sys.identity_columns
where OBJECT_NAME(object_id ) = @p1`
	var res []int
	if err := db.Select(&res, query, tableName); err != nil {
		return false, err
	}
	return res[0] > 0, nil
}

func getComputeColumns(db *sqlx.DB, tableName string) ([]string, error) {
	query := `
SELECT name
FROM sys.computed_columns
WHERE OBJECT_NAME(object_id) = @p1`
	var res []string
	if err := db.Select(&res, query, tableName); err != nil {
		return nil, err
	}
	return res, nil
}

func main() {
	var dataSource, initialCatalog, userId, password, dirPath string
	flag.StringVar(&dataSource, "s", "localhost,1433", "db data source")
	flag.StringVar(&initialCatalog, "c", "master", "initial catalog")
	flag.StringVar(&userId, "u", "test", "user id")
	flag.StringVar(&password, "p", "test", "user password")
	flag.StringVar(&dirPath, "d", "test_data", "path to dir with data to upload")

	flag.Usage = func() {
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nReturn codes:\n")
		for i := range len(exitCodeDescription) {
			fmt.Fprintf(os.Stderr, "  %d => %s\n", i, exitCodeDescription[i])
		}
	}
	flag.Parse()

	connectionString := fmt.Sprintf("Data Source=%s; Initial Catalog=%s;User ID=%s;Password=%s;", dataSource, initialCatalog, userId, password)
	db, err := sqlx.Open("sqlserver", connectionString)
	handleError(err, ConnectErrorCode)
	defer db.Close()

	files, err := os.ReadDir(dirPath)
	handleError(err, ReadDirErrorCode)

	for _, file := range files {
		fileName := file.Name()
		filePath := fmt.Sprintf("%s/%s", dirPath, fileName)
		tableName, ext := func(fn string) (string, Format) {
			nameAndExt := strings.Split(strings.SplitN(fn, "_", 2)[1], ".")
			if len(nameAndExt) > 2 {
				li := len(nameAndExt) - 1
				return strings.Join(nameAndExt[:li], ""), getFileFormat(nameAndExt[li])
			}
			return nameAndExt[0], getFileFormat(nameAndExt[1])
		}(fileName)

		schema, err := getTableSchema(db, tableName)
		handleError(err, TableInfoErrorCode)

		isTableIdentity, err := isTableHasIdentity(db, tableName)
		handleError(err, TableInfoErrorCode)

		computeColumns, err := getComputeColumns(db, tableName)
		handleError(err, TableInfoErrorCode)

		var allRecords []map[string]any
		switch ext {
		case Json:
			data, err := os.ReadFile(filePath)
			handleError(err, ReadFileErrorCode)

			try(json.Unmarshal(data, &allRecords))
			handleError(err, UnmarshalErrorCode)
		case Csv:
			file, err := os.Open(filePath)
			handleError(err, OpenFileErrorCode)

			r := csv.NewReader(file)
			r.Comma = ';'
			headers, err := r.Read()
			handleError(err, UnmarshalErrorCode)
			for {
				record, err := r.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					panic(err)
				}
				row := make(map[string]any, len(headers))
				for i, header := range headers {
					if num, err := strconv.Atoi(record[i]); err == nil {
						row[header] = num
					} else {
						row[header] = record[i]
					}
				}
				allRecords = append(allRecords, row)
			}

		}

		for _, records := range allRecords {
			var columns []string
			var values []any
			for col, colSchema := range schema {
				if val, ok := records[col]; ok {
					if colSchema.DataType == "timestamp" || slices.Contains(computeColumns, col) {
						continue
					}
					if ext == Csv && val == "NULL" {
						if colSchema.IsNullable != "YES" && !colSchema.ColumnDefault.Valid {
							log.Fatalf("required field %s missing from csv", col)
						}
					} else {
						col = "[" + col + "]"
						columns = append(columns, col)
						values = append(values, val)
					}
				} else {
					if colSchema.IsNullable != "YES" && !colSchema.ColumnDefault.Valid {
						log.Fatalf("required field %s missing from json", col)
					}
				}
			}
			if len(columns) == 0 {
				fmt.Println("No data to insert.")
				return
			}
			placeholders := ""
			for i := range columns {
				if i > 0 {
					placeholders += ", "
				}
				placeholders += fmt.Sprintf("@p%d", i+1)
			}

			columnsStr := ""
			for i, col := range columns {
				if i > 0 {
					columnsStr += ", "
				}
				columnsStr += col
			}
			query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", tableName, columnsStr, placeholders)
			if isTableIdentity {
				identityON := fmt.Sprintf("SET IDENTITY_INSERT %s ON;", tableName)
				identityOFF := fmt.Sprintf("SET IDENTITY_INSERT %s OFF;", tableName)
				query = identityON + query + identityOFF
			}
			fmt.Println("query ", query)
			_, err := db.Exec(query, values...)
			handleError(err, InsertDataErrorCode)
		}
	}
	fmt.Println("Upload done")
	os.Exit(SuccessCode)
}
