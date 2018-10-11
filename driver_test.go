package ocsql_test

import (
	"database/sql"
	"database/sql/driver"
	"reflect"
	"testing"

	"github.com/opencensus-integrations/ocsql"
)

type stubRows struct{}

func (stubRows) Columns() []string              { return []string{} }
func (stubRows) Close() error                   { return nil }
func (stubRows) Next(dest []driver.Value) error { return nil }

type stubScanType struct {
	toReturn reflect.Type
}

func (s stubScanType) ColumnTypeScanType(index int) reflect.Type { return s.toReturn }

type stubDriver struct {
	rows driver.Rows
}

func (d stubDriver) Open(name string) (driver.Conn, error) {
	return stubConnection{rows: d.rows}, nil
}

type stubConnection struct {
	rows driver.Rows
}

func (c stubConnection) Prepare(query string) (driver.Stmt, error) {
	return stubStmt{rows: c.rows}, nil
}

func (stubConnection) Close() error              { return nil }
func (stubConnection) Begin() (driver.Tx, error) { return &sql.Tx{}, nil }

type stubStmt struct {
	rows driver.Rows
}

func (stubStmt) Close() error                                    { return nil }
func (stubStmt) NumInput() int                                   { return 0 }
func (stubStmt) Exec(args []driver.Value) (driver.Result, error) { return stubResult{}, nil }

func (s stubStmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.rows, nil
}

type stubResult struct{}

func (stubResult) LastInsertId() (int64, error) { return 0, nil }
func (stubResult) RowsAffected() (int64, error) { return 0, nil }

type testFunc func(t *testing.T, rows driver.Rows)

var testNotAssignableToScanTypeInterface testFunc = func(t *testing.T, rows driver.Rows) {
	if _, ok := rows.(driver.RowsColumnTypeScanType); ok {
		t.Error("expected output to not be assignable to type: RowsColumnTypeLength")
	}
}

var testAssignableToScanTypeInterface testFunc = func(t *testing.T, rows driver.Rows) {
	if _, ok := rows.(driver.RowsColumnTypeScanType); !ok {
		t.Error("expected output to be assignable to type: RowsColumnTypeLength")
	}
}

func TestRowsAreWrappedWithCorrectInterfaceType(t *testing.T) {
	type test struct {
		name      string
		input     driver.Rows
		testFunc testFunc
	}

	tests := []test{
		{
			input: stubRows{},
			name:  "test non scan type parent is not wrapped with scan type interface",
			testFunc: testNotAssignableToScanTypeInterface,
		},
		{
			input: struct {
				driver.Rows
				ocsql.RowsColumnTypeScanType
			}{ stubRows{}, stubScanType{}},
			name:  "test wraps rows with scan type interface",
			testFunc: testAssignableToScanTypeInterface,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var d = ocsql.Wrap(stubDriver{
				rows: tt.input,
			}, ocsql.WithAllTraceOptions())
			var c, _ = d.Open("fake-connection")

			s, err := c.Prepare("SELECT * FROM test;")
			if err != nil {
				t.Errorf("connection.Prepare returned unexpected err: %v", err)
				return
			}

			rows, err := s.Query([]driver.Value{})
			if err != nil {
				t.Errorf("stmt.Query returned unexpected err: %v", err)
				return
			}

			tt.testFunc(t, rows)
		})
	}
}
