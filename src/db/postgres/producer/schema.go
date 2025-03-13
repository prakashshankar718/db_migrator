package producerPG

import (
	"fmt"
	"log"
	"strings"

	"github.com/prakashshankar718/db_migrator/src/db/models"
	"xorm.io/xorm"
	"xorm.io/xorm/schemas"
)

type PgSchema interface {
	NewConnection() (err error)
	GetAllTablesCommon() (tables []*models.Table, err error)
	GetAllTablesSchema() (tables []*schemas.Table, err error)
	CloseConnection() (err error)
	GetConnectionForCDC() *xorm.Engine
	CheckCreateWAL(dbname string) error
}

func NewSchemaMig(dsn string) PgSchema {
	return &pgSchemaImpl{dsn: dsn}
}

type pgSchemaImpl struct {
	dsn    string
	Engine *xorm.Engine
}

func (pgs *pgSchemaImpl) NewConnection() (err error) {
	pgs.Engine, err = xorm.NewEngine("pgx", pgs.dsn)
	if err != nil {
		log.Fatalf("PostgreSQL: Failed to create XORM engine: %v", err)
		return
	}
	// defer pgs.engine.Close()

	// Test the connection
	if err = pgs.Engine.Ping(); err != nil {
		log.Fatalf("PostgreSQL: Failed to connect to the database: %v", err)
		return
	}

	// pgs.engine.ShowSQL(true)

	fmt.Println("Connected to PostgreSQL database successfully!")
	return
}

func (pgs *pgSchemaImpl) CheckCreateWAL(dbname string) (err error) {
	_, err = pgs.Engine.Exec("SELECT pg_create_logical_replication_slot(?, 'wal2json');", strings.ToLower(dbname))
	if err.Error() == "ERROR: replication slot \"migrationpg\" already exists (SQLSTATE 42710)" {
		return nil
	}
	return
}

func (pgs *pgSchemaImpl) GetConnectionForCDC() *xorm.Engine {
	return pgs.Engine
}

func (pgs *pgSchemaImpl) CloseConnection() (err error) {
	err = pgs.Engine.Close()
	if err != nil {
		log.Fatalf("PostgreSQL: Failed to close XORM connection: %v", err)
		return
	}
	fmt.Println("Disonnected to PostgreSQL database!")
	return
}

// ([]*schemas.Table, error)
func (pgs *pgSchemaImpl) GetAllTablesCommon() (tables []*models.Table, err error) {
	pgTables, err := pgs.Engine.DBMetas()
	if err != nil {
		return
		// return nil, err
	}
	// fmt.Println(pgTables)
	for i := range pgTables {
		tables = append(tables, convertToCommonTable(pgTables[i]))
	}
	return
}

func (pgs *pgSchemaImpl) GetAllTablesSchema() (tables []*schemas.Table, err error) {
	tables, err = pgs.Engine.DBMetas()
	if err != nil {
		return
		// return nil, err
	}
	return
}

func convertToCommonTable(pgTable *schemas.Table) (table *models.Table) {
	return &models.Table{
		Name:       pgTable.Name,
		Type:       pgTable.Type,
		ColumnsSeq: pgTable.ColumnsSeq(),
		// ColumnsMap: pgTable.ColumnsMap,
		Columns:       getColumns(pgTable.Columns()),
		Indexes:       getIndexValues(pgTable.Indexes),
		PrimaryKeys:   pgTable.PrimaryKeys,
		AutoIncrement: pgTable.AutoIncrement,
		Created:       pgTable.Created,
		Updated:       pgTable.Updated,
		Deleted:       pgTable.Deleted,
		Version:       pgTable.Version,
		StoreEngine:   pgTable.StoreEngine,
		Charset:       pgTable.Charset,
		Comment:       pgTable.Comment,
		Collation:     pgTable.Collation,
	}
}

func getColumns(pgColumns []*schemas.Column) (columns []*models.Column) {
	for i := range pgColumns {
		columns = append(columns, &models.Column{
			Name:            pgColumns[i].Name,
			TableName:       pgColumns[i].TableName,
			FieldName:       pgColumns[i].FieldName,
			FieldIndex:      pgColumns[i].FieldIndex,
			SQLType:         models.SQLType(pgColumns[i].SQLType),
			IsJSON:          pgColumns[i].IsJSON,
			Length:          pgColumns[i].Length,
			Length2:         pgColumns[i].Length2,
			Nullable:        pgColumns[i].Nullable,
			Default:         pgColumns[i].Default,
			Indexes:         pgColumns[i].Indexes,
			IsPrimaryKey:    pgColumns[i].IsPrimaryKey,
			IsAutoIncrement: pgColumns[i].IsAutoIncrement,
			MapType:         pgColumns[i].MapType,
			IsCreated:       pgColumns[i].IsCreated,
			IsUpdated:       pgColumns[i].IsUpdated,
			IsDeleted:       pgColumns[i].IsDeleted,
			IsCascade:       pgColumns[i].IsCascade,
			IsVersion:       pgColumns[i].IsVersion,
			DefaultIsEmpty:  pgColumns[i].DefaultIsEmpty,
			EnumOptions:     pgColumns[i].EnumOptions,
			SetOptions:      pgColumns[i].SetOptions,
			DisableTimeZone: pgColumns[i].DisableTimeZone,
			TimeZone:        pgColumns[i].TimeZone,
			Comment:         pgColumns[i].Comment,
			Collation:       pgColumns[i].Collation,
		})
	}
	return
}

func getIndexValues(pgIndexes map[string]*schemas.Index) (indexes map[string]*models.Index) {
	indexes = make(map[string]*models.Index)
	for k, _ := range pgIndexes {
		indexes[k] = getIndex(pgIndexes[k])
	}
	return
}

func getIndex(pgindex *schemas.Index) (index *models.Index) {
	return &models.Index{
		IsRegular: pgindex.IsRegular,
		Name:      pgindex.Name,
		Type:      pgindex.Type,
		Cols:      pgindex.Cols,
	}
}
