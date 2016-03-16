package sqls

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"

	"fmt"
	"os"
	"testing"
)

var (
	mysqlConn *sql.DB
	username  string = "root"
	passwd    string = ""
)

func init() {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(127.0.0.1:3306)/golib_test", username, passwd))
	if err != nil {
		fmt.Printf("connect to mysql error\n")
		os.Exit(1)
	}

	mysqlConn = db
}

func TestCreateTable(t *testing.T) {
	db := &SqlWrap{db: mysqlConn}
	sql := "CREATE TABLE IF NOT EXISTS `users` (`id` bigint unsigned NOT NULL AUTO_INCREMENT, `name` varchar(255) NOT NULL, PRIMARY KEY (`id`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8"
	_, err := db.Exec(sql)
	if err != nil {
		t.Fatalf("create table error, %v", err)
	}
}

func TestInsertOne(t *testing.T) {
	db := &SqlWrap{db: mysqlConn}
	sql := "INSERT INTO users (name) VALUES ('test')"
	_, err := db.Exec(sql)
	if err != nil {
		t.Fatalf("insert into db error, %v", err)
	}
}

func TestInsertTx(t *testing.T) {
	db := &SqlWrap{db: mysqlConn}
	sql := "INSERT INTO users (name) VALUES (?)"

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("start tx error: %v", err)
	}

	for i := 0; i < 5; i++ {
		_, err := db.ExecTx(tx, sql, "test")
		if err != nil {
			db.Rollback(tx)
			t.Fatalf("tx insert into db error, %v", err)
		}
	}

	db.Commit(tx)
}

func TestQuery(t *testing.T) {
	db := &SqlWrap{db: mysqlConn}
	sql := "SELECT id, name FROM users WHERE name = ?"

	rows, err := db.Query(sql, "test")
	if err != nil {
		t.Fatalf("query db error, %v", err)
	}
	defer rows.Close()

	var id int64
	var name string
	for rows.Next() {
		rows.Scan(
			&id,
			&name,
		)
		t.Logf("query db, id: %d, name: %s", id, name)
	}
	err = rows.Err()
	if err != nil {
		t.Fatalf("query db error, %v", err)
	}
}

func TestDropTable(t *testing.T) {
	db := &SqlWrap{db: mysqlConn}
	sql := "DROP TABLE users"
	_, err := db.Exec(sql)
	if err != nil {
		t.Fatalf("drop table error, %v", err)
	}
}
