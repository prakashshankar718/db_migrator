package main

import (

	// "github.com/jackc/pgx/v4/pgxpool"

	"context"
	"fmt"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"
	consumerMySQL "github.com/prakashshankar718/db_migrator/src/db/mysql/consumer"
	producerPG "github.com/prakashshankar718/db_migrator/src/db/postgres/producer"
)

var sourceDB = "migrationPG"
var destinationDB = "migrationMYSQL"

func main() {
	dsn := "postgres://postgres:admin@localhost:5432/" + sourceDB
	mysqlDSN := "root:mysql@/" + destinationDB + "?charset=utf8"
	pgSchema := producerPG.NewSchemaMig(dsn)
	err := pgSchema.NewConnection()
	if err != nil {
		fmt.Println(err)
		return
	}

	// check wal in sourceDB
	err = pgSchema.CheckCreateWAL(sourceDB)
	if err != nil {
		fmt.Println(err)
		return
	}
	// tables, err := pgSchema.GetAllTables()
	// if err != nil {
	// 	fmt.Println(err)
	// 	return
	// }
	tables, err := pgSchema.GetAllTablesSchema()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("total tables: ", len(tables))
	for i, _ := range tables {
		fmt.Println("   creating table:", tables[i].Name)
		// fmt.Println(tables[i].Columns())
		// for j, _ := range tables[i].Columns() {
		// 	fmt.Println("   ", tables[i].Columns()[j].Name, tables[i].Columns()[j].SQLType.Name)
		// }
	}
	// err = pgSchema.CloseConnection()
	// if err != nil {
	// 	fmt.Println(err)
	// 	return
	// }
	defer pgSchema.CloseConnection()

	mySqlSchema := consumerMySQL.NewSchemaMig(mysqlDSN)
	mySqlSchema.NewConnection()

	defer mySqlSchema.CloseConnection()

	err = mySqlSchema.CreateTable(tables)
	if err != nil {
		fmt.Println("mySqlSchema.CreateTable()", err)
		return
	}

	ChangeDataStream := make(chan string, 4096)
	pgCDC := producerPG.NewPgWALConfig(sourceDB, strings.ToLower(sourceDB), pgSchema.GetConnectionForCDC())
	go pgCDC.StreamWAL(context.Background(), ChangeDataStream)

	mySqlDataConsumer := consumerMySQL.NewWALConsumer(mysqlDSN)

	err = mySqlDataConsumer.NewConnection()
	if err != nil {
		fmt.Println(70, err)
		return
	}

	mySqlDataConsumer.ProcessWALData(ChangeDataStream)

	// time.Sleep(30 * time.Second)
}
