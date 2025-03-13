package consumerMySQL

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"xorm.io/xorm"
	"xorm.io/xorm/schemas"
)

type MySQLSchema interface {
	NewConnection() (err error)
	// GetAllTables() (tables []*models.Table, err error)
	CloseConnection() (err error)
	CreateTable(tables []*schemas.Table) (err error)
}

func NewSchemaMig(dsn string) MySQLSchema {
	return &mySQLSchemaImpl{dsn: dsn}
}

type mySQLSchemaImpl struct {
	dsn    string
	engine *xorm.Engine
}

func (mys *mySQLSchemaImpl) NewConnection() (err error) {
	mys.engine, err = xorm.NewEngine("mysql", mys.dsn)
	if err != nil {
		log.Fatalf("MySQL: Failed to create XORM engine: %v", err)
		return
	}
	// defer mys.engine.Close()

	// Test the connection
	if err = mys.engine.Ping(); err != nil {
		log.Fatalf("MySQL: Failed to connect to the database: %v", err)
		return
	}

	fmt.Println("Connected to MySQL database successfully!")
	return
}

func (mys *mySQLSchemaImpl) CloseConnection() (err error) {
	err = mys.engine.Close()
	if err != nil {
		log.Fatalf("MySQL: Failed to close XORM connection: %v", err)
		return
	}
	fmt.Println("Disonnected to MySQL database!")
	return
}

func (mys *mySQLSchemaImpl) CreateTable(tables []*schemas.Table) (err error) {
	for _, table := range tables {
		tableInf := createStructFromTable(table)
		err = mys.createTableFromSQL(tableInf, table)
		if err != nil {
			return
		}
	}
	return
}

func (mys *mySQLSchemaImpl) createTableFromSQL(tableInf interface{}, table *schemas.Table) (err error) {
	err = mys.engine.CreateTables(tableInf)
	if err != nil {
		// fmt.Println(err)
		sql, bl, err1 := mys.engine.Dialect().CreateTableSQL(context.Background(), mys.engine.DB(), table, table.Name)
		if err1 != nil {
			log.Fatal("Error generating SQL:", err1)
			return err1
		}
		// fmt.Println(bl, sql)

		if bl {
			_, err1 := mys.engine.Exec(sql)
			if err1 != nil {
				return err1
			}
			// fmt.Println(result)
		}
		return err1

	}
	return
}

// Function to create a struct dynamically from an xorm.schemas.Table
func createStructFromTable(table *schemas.Table) interface{} {
	table.StoreEngine = "InnoDB"           // Default engine
	table.Charset = "utf8mb4"              // Use UTF8MB4
	table.Collation = "utf8mb4_unicode_ci" // Set collation

	fields := make([]reflect.StructField, 0)
	titleCaser := cases.Title(language.English) // Proper Unicode case handling
	// fmt.Println("table.Name", table.Name)
	// Iterate through table columns and define struct fields
	for _, col := range table.Columns() {
		// Convert database column name to an exported Go struct field
		if col.Name == "year" {
			col.Name = "v_year"
		}

		fieldName := titleCaser.String(col.Name)           // Properly capitalizes first letter
		fieldName = strings.ReplaceAll(fieldName, " ", "") // Remove spaces (just in case)
		// fmt.Println("fieldName", fieldName)

		// Convert database column types to Go types
		var goType reflect.Type
		switch col.SQLType.Name {
		case schemas.Int, schemas.Integer:
			goType = reflect.TypeOf(int(0))
		case schemas.BigInt:
			goType = reflect.TypeOf(int64(0))
		case schemas.Varchar, schemas.Char, schemas.Text:
			goType = reflect.TypeOf("")
		case schemas.Bool:
			goType = reflect.TypeOf(true)
		default:
			goType = reflect.TypeOf("") // Default to string
		}

		// Define struct field with XORM tag
		field := reflect.StructField{
			Name: fieldName,                                                // Exported field name
			Type: goType,                                                   // Go data type
			Tag:  reflect.StructTag(fmt.Sprintf(`xorm:"'%s'"`, fieldName)), // XORM tag
		}
		fields = append(fields, field)
	}

	// Create struct type dynamically
	structType := reflect.StructOf(fields)

	// Return an instance of the struct
	return reflect.New(structType).Interface()
}
