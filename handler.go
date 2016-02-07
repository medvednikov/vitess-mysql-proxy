package main

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/siddontang/go-mysql/mysql"
)

var execStatements = []string{"insert", "update", "delete", "create", "alter"}
var selectStatements = []string{"select", "show"}

type VitessHandler struct{}

//handle COM_QUERY comamnd, like SELECT, INSERT, UPDATE, etc...
//If Result has a Resultset (SELECT, SHOW, etc...), we will send this as the
//repsonse, otherwise, we will send Result
func (h VitessHandler) HandleQuery(query string) (*mysql.Result, error) {
	fmt.Println("query = ", query)
	// Get the statement (first word of the query)
	statement := strings.ToLower(strings.Split(query, " ")[0])
	if contains(execStatements, statement) {
		return executeQuery(query)
	} else if contains(selectStatements, statement) {
		return selectQuery(query)
	}
	return nil, errors.New("Unsupported statement")
}

func executeQuery(query string) (*mysql.Result, error) {
	fmt.Println("Updating/deleting/inserting into master...")
	tx, err := vitessdb.Begin()
	if err != nil {
		fmt.Printf("begin failed: %v\n", err)
		return nil, err
	}
	res, err := tx.Exec(query)
	if err != nil {
		fmt.Printf("exec failed: %v\n", err)
		return nil, err
	}
	err = tx.Commit()
	if err != nil {
		fmt.Printf("commit failed: %v\n", err)
		return nil, err
	}
	nrRows, _ := res.RowsAffected()
	return &mysql.Result{0, 0, uint64(nrRows), nil}, nil
}

func selectQuery(query string) (*mysql.Result, error) {
	rows, err := vitessdb.Query(query)
	if err != nil {
		fmt.Printf("query failed: %v\n", err)
		return &mysql.Result{0, 0, 0, nil}, err
	}
	columns, _ := rows.Columns()
	count := len(columns)
	// Read rows and convert them to [][]interface{}
	res := make([][]interface{}, 0)
	for rows.Next() {
		values := make([]interface{}, count)
		valuePtrs := make([]interface{}, count)
		for i, _ := range columns {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			fmt.Printf("scan failed: %v\n", err)
			os.Exit(1)
		}
		res = append(res, values)
	}
	if err := rows.Err(); err != nil {
		fmt.Printf("row iteration failed: %v\n", err)
		os.Exit(1)
	}
	r, err := mysql.BuildSimpleResultset(columns, res, false)
	return &mysql.Result{0, 0, 0, r}, err
}

func contains(slice []string, s string) bool {
	for i := 0; i < len(slice); i++ {
		if slice[i] == s {
			return true
		}
	}
	return false
}

//handle COM_FILED_LIST command
func (h VitessHandler) HandleFieldList(table string, fieldWildcard string) ([]*mysql.Field, error) {
	return nil, nil
}

//handle COM_STMT_PREPARE, params is the param number for this statement,
//columns is the column number context will be used later for statement execute
func (h VitessHandler) HandleStmtPrepare(query string) (params int, columns int, context interface{}, err error) {
	return 0, 0, nil, nil
}

//handle COM_STMT_EXECUTE, context is the previous one set in prepare query is
//the statement prepare query, and args is the params for this statement
func (h VitessHandler) HandleStmtExecute(context interface{}, query string, args []interface{}) (*mysql.Result, error) {
	return nil, nil
}

//handle COM_INIT_DB command, you can check whether the dbName is valid, or other.
func (h VitessHandler) UseDB(dbName string) error {
	return nil
}
