package main

import (
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"unicode/utf8"

	csv "github.com/JensRantil/go-csv"
	"github.com/cheggaaa/pb/v3"
)

func connect(host string, port int, dbname string, username string, password string) (*sql.DB, error) {

	var connectionString = fmt.Sprintf("user=%s dbname=%s password='%s' host=%s port=%d sslmode=disable connect_timeout=5",
		username,
		dbname,
		password,
		host,
		port,
	)

	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return db, err
	}

	err = db.Ping()
	if err != nil {
		return db, err
	}

	return db, nil
}

func lineCounter(r io.Reader) (int, error) {

	var count int

	buf := make([]byte, 1024)

	for {
		bufferSize, err := r.Read(buf)
		if err != nil && err != io.EOF {
			return 0, err
		}

		var buffPosition int
		for {
			i := bytes.IndexByte(buf[buffPosition:], '\n')
			if i == -1 || bufferSize == buffPosition {
				break
			}
			buffPosition += i + 1
			count++
		}
		if err == io.EOF {
			break
		}
	}

	return count, nil
}

// NewProgressBar initializes new progress bar based on size of file
func NewProgressBar(file *os.File) *pb.ProgressBar {
	fi, err := file.Stat()

	total := int64(0)
	if err == nil {
		total = fi.Size()
	}

	bar := pb.New64(total)
	bar.Set(pb.Bytes, true)
	return bar
}

func importCSV(db *sql.DB, filename string, tableName string, fields string, delimiter string) (int, error) {

	dialect := csv.Dialect{}
	dialect.Delimiter, _ = utf8.DecodeRuneInString(delimiter)

	dialect.LineTerminator = "\n"

	var reader *csv.Reader
	var bar *pb.ProgressBar
	var recordsErrors int = 0
	if filename != "" {
		file, err := os.Open(filename)
		if err != nil {
			fmt.Println("Error when open csv file.")
			return recordsErrors, err
		}
		defer file.Close()

		bar = NewProgressBar(file)
		reader = csv.NewDialectReader(bar.NewProxyReader(file), dialect)
	} else {
		reader = csv.NewDialectReader(os.Stdin, dialect)
	}

	var columns []string = strings.Split(fields, ",")

	var recordsCount int64 = 0
	imp, err := newImport(db, "public", tableName, columns)
	if err != nil {
		fmt.Printf("Error create import object. tableName: %s\n", tableName)
		fmt.Println(err)
		return recordsErrors, err
	}

	numberColumns := len(columns)
	bar.Start()
	for {

		values := make([]interface{}, numberColumns)
		record, err := reader.Read()
		if err == io.EOF {
			break
		}

		// skip csv errors
		if len(record) < numberColumns {
			//			fmt.Println("specified columns are not fit with read record:")
			//			fmt.Println(columns)
			//			fmt.Println(record)
			recordsErrors++
			continue
		}
		//Loop ensures we don't insert too many values and that
		//values are properly converted into empty interfaces
		for i, col := range record {
			if i >= numberColumns {
				break
			}
			values[i] = strings.Replace(col, "\x00", "", -1)
		}

		imp.AddRow("\\N", values...)
		recordsCount++
	}

	imp.Commit()
	bar.Finish()
	return recordsErrors, err
}

var Usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s [OPTIONS] csv_file :\n", os.Args[0])

	flag.PrintDefaults()
}

// --pass 1 --user postgres --host localhost --dbname fias --table AddressObjectType csv --fields "LEVEL,SOCRNAME,SCNAME,KOD_T_ST" AddressObjectTypes.csv

func main() {
	pgHost := flag.String("host", "localhost", "database name in pg.")
	pgDbname := flag.String("dbname", "fias", "database name in pg")
	pgPort := flag.Int("port", 5432, "pg port.")
	pgUser := flag.String("user", "postgres", "username to login to pg.")
	pgPassword := flag.String("pass", "1", "password to login to pg.")
	pgTable := flag.String("table", "", "table name where to import csv")
	pgTableFields := flag.String("fields", "", "table name where to import csv")
	delimiter := flag.String("delimiter", ";", "delimiter in csv")
	flag.Parse()
	var csvFile string = flag.Arg(0)

	if len(csvFile) == 0 {
		fmt.Println("No csv file specified to import.")
		Usage()
		return
	}

	fmt.Println("connect to postgresql...")
	fmt.Println("pgHost: ", *pgHost)
	fmt.Println("pgPort: ", *pgPort)
	fmt.Println("pgDbname:", *pgDbname)
	fmt.Println("Delimiter:", *delimiter)
	fmt.Println("pgPassword:", *pgPassword)
	fmt.Println("table name:", *pgTable)
	fmt.Println("fields to import:", *pgTableFields)
	fmt.Println("file to import from:", csvFile)

	db, err := connect(*pgHost, *pgPort, *pgDbname, *pgUser, *pgPassword)
	if err != nil {
		fmt.Println("Unable to connect to postgresql.")
		fmt.Println(err)
		return
	}
	defer db.Close()

	errRecords, err := importCSV(db, csvFile, *pgTable, *pgTableFields, *delimiter)
	if err != nil {
		fmt.Println("Error while import csv file.")
		fmt.Println(err)
		return
	}

	fmt.Printf("%d errors records skiped.", errRecords)
}
