package sqls

import (
	"database/sql"
	"fmt"
)

type SqlWrap struct {
	db *sql.DB
}

// set real sql DB
func (s *SqlWrap) SetDB(db *sql.DB) {
	s.db = db
}

func (s *SqlWrap) Begin() (tx *sql.Tx, err error) {
	tx, err = s.db.Begin()
	return
}

func (s *SqlWrap) Rollback(tx *sql.Tx) {
	tx.Rollback()
}

func (s *SqlWrap) Commit(tx *sql.Tx) {
	tx.Commit()
}

func (s *SqlWrap) Exec(sql string, params ...interface{}) (res sql.Result, err error) {
	if s.db == nil {
		return res, fmt.Errorf("db connection is nil")
	}

	stmtIns, err := s.db.Prepare(sql)
	if err != nil {
		return res, err
	}
	defer stmtIns.Close()

	res, err = stmtIns.Exec(
		params...,
	)
	if err != nil {
		return res, err
	}

	return res, nil
}

func (s *SqlWrap) ExecTx(tx *sql.Tx, sql string, params ...interface{}) (res sql.Result, err error) {
	if tx == nil {
		return res, fmt.Errorf("db tx is nil")
	}

	stmtIns, err := tx.Prepare(sql)
	if err != nil {
		return res, err
	}
	defer stmtIns.Close()

	res, err = stmtIns.Exec(
		params...,
	)
	if err != nil {
		return res, err
	}

	return res, nil
}

func (s *SqlWrap) Query(sql string, params ...interface{}) (rows *sql.Rows, err error) {
    if s.db == nil {
		return rows, fmt.Errorf("db connection is nil")
	}

    stmtOuts, err := s.db.Prepare(sql)
    if err != nil {
        return rows, err
    }
    defer stmtOuts.Close()

    rows, err = stmtOuts.Query(
        params...,
    )
    if err != nil {
        return rows, err
    }
    defer rows.Close()
    
    return rows, nil
}
