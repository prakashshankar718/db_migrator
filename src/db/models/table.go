package models

import (
	"reflect"
	"time"
)

// Table represents a database table
type Table struct {
	Name          string
	Type          reflect.Type
	ColumnsSeq    []string
	ColumnsMap    map[string][]*Column
	Columns       []*Column
	Indexes       map[string]*Index
	PrimaryKeys   []string
	AutoIncrement string
	Created       map[string]bool
	Updated       string
	Deleted       string
	Version       string
	StoreEngine   string
	Charset       string
	Comment       string
	Collation     string
}

// Column defines database column
type Column struct {
	Name            string
	TableName       string
	FieldName       string // Available only when parsed from a struct
	FieldIndex      []int  // Available only when parsed from a struct
	SQLType         SQLType
	IsJSON          bool
	Length          int64
	Length2         int64
	Nullable        bool
	Default         string
	Indexes         map[string]int
	IsPrimaryKey    bool
	IsAutoIncrement bool
	MapType         int
	IsCreated       bool
	IsUpdated       bool
	IsDeleted       bool
	IsCascade       bool
	IsVersion       bool
	DefaultIsEmpty  bool // false means column has no default set, but not default value is empty
	EnumOptions     map[string]int
	SetOptions      map[string]int
	DisableTimeZone bool
	TimeZone        *time.Location // column specified time zone
	Comment         string
	Collation       string
}

// Index represents a database index
type Index struct {
	IsRegular bool
	Name      string
	Type      int
	Cols      []string
}

// SQLType represents SQL types
type SQLType struct {
	Name           string
	DefaultLength  int64
	DefaultLength2 int64
}
