package sqls

import (
	_ "github.com/go-sql-driver/mysql"

	"fmt"
	"os"
	"testing"
)

var (
	db        *SqlWrap
	username  string = "root"   // your mysql username for test
	passwd    string = "123456" // your mysql password for test
	insertNum int64  = 100
)

func init() {
	var err error
	db, err = Open("mysql", fmt.Sprintf("%s:%s@tcp(127.0.0.1:3306)/golib_test", username, passwd))
	if err != nil {
		fmt.Printf("connect to mysql error\n")
		os.Exit(1)
	}

	// you can also use sql.Open and then assign to SqlWrap
	/*
	    mysqlConn, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(127.0.0.1:3306)/golib_test", username, passwd))
		if err != nil {
			fmt.Printf("connect to mysql error\n")
			os.Exit(1)
		}
	    db = &SqlWrap{db: mysqlConn}
	*/
}

func TestCreateTable(t *testing.T) {
	sql := "CREATE TABLE IF NOT EXISTS `users` (`id` bigint unsigned NOT NULL AUTO_INCREMENT, `name` varchar(255) NOT NULL, PRIMARY KEY (`id`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8"
	_, err := db.Exec(sql)
	if err != nil {
		t.Fatalf("create table error, %v", err)
	}
}

func TestInsertOne(t *testing.T) {
	sql := "INSERT INTO users (name) VALUES ('test')"
	_, err := db.Exec(sql)
	if err != nil {
		t.Fatalf("insert into db error, %v", err)
	}
}

func TestInsertTx(t *testing.T) {
	sql := "INSERT INTO users (name) VALUES (?)"

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("start tx error: %v", err)
	}

	for i := int64(0); i < insertNum-1; i++ {
		_, err := db.ExecTx(tx, sql, "test")
		if err != nil {
			db.Rollback(tx)
			t.Fatalf("tx insert into db error, %v", err)
		}
	}

	db.Commit(tx)
}

func TestQuery(t *testing.T) {
	sql := "SELECT id, name FROM users WHERE name = ?"

	rows, err := db.Query(sql, "test")
	if err != nil {
		t.Fatalf("query db error, %v", err)
	}
	defer rows.Close()

	var id int64
	var name string
	var count int64 = 0
	for rows.Next() {
		count++
		rows.Scan(
			&id,
			&name,
		)
	}
	err = rows.Err()
	if err != nil {
		t.Fatalf("query db error, %v", err)
	}

	if count != insertNum {
		t.Fatalf("query db error, %d rows inserted and %d get", insertNum, count)
	}
}

func TestDropTable(t *testing.T) {
	sql := "DROP TABLE users"
	_, err := db.Exec(sql)
	if err != nil {
		t.Fatalf("drop table error, %v", err)
	}
}
