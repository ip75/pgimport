package main

import (
	"database/sql"
	"fmt"

	"github.com/lib/pq"
)

type Import struct {
	txn  *sql.Tx
	stmt *sql.Stmt
}

func newImport(db *sql.DB, schema string, tableName string, columns []string) (*Import, error) {

	txn, err := db.Begin()
	if err != nil {
		return nil, err
	}

	stmt, err := txn.Prepare(pq.CopyInSchema(schema, tableName, columns...))
	if err != nil {
		fmt.Println(pq.CopyInSchema(schema, tableName, columns...))
		return nil, err
	}

	//	fmt.Print("Statement to execute: ")
	//	fmt.Println(stmt)

	return &Import{txn, stmt}, nil
}

func (i *Import) AddRow(nullDelimiter string, columns ...interface{}) error {
	for index := range columns {
		column := columns[index]

		if column == nullDelimiter {
			columns[index] = nil
		}
	}

	_, err := i.stmt.Exec(columns...)
	return err
}

func (i *Import) Commit() error {

	_, err := i.stmt.Exec()
	if err != nil {
		return err
	}

	// Statement might already be closed
	// therefore ignore errors
	_ = i.stmt.Close()

	return i.txn.Commit()

}
