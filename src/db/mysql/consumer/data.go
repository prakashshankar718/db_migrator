package consumerMySQL

import (
	"encoding/json"
	"log"

	"github.com/prakashshankar718/db_migrator/src/db/models"
	"github.com/tidwall/gjson"
	"xorm.io/xorm"
)

type walConsumer interface {
	NewConnection() error
	ProcessWALData(changeDataStream <-chan string)
}

type WALConsumerImpl struct {
	dsn    string
	engine *xorm.Engine
}

func NewWALConsumer(dsn string) walConsumer {
	return &WALConsumerImpl{
		dsn: dsn,
	}
}

func (wci *WALConsumerImpl) NewConnection() error {
	var err error
	wci.engine, err = connectMySQL(wci.dsn)
	if err != nil {
		log.Fatal("Failed to connect to MySQL:", err)
	}
	// defer engine.Close()

	// log.Println("Starting WAL log streaming...")
	// streamWAL()
	return err
}

func connectMySQL(dsn string) (*xorm.Engine, error) {
	// dsn := "user:password@tcp(mysql_host:3306)/your_db"
	engine, err := xorm.NewEngine("mysql", dsn)
	if err != nil {
		return nil, err
	}
	return engine, nil
}

func (wci *WALConsumerImpl) ProcessWALData(changeDataStream <-chan string) {
	for jsonData := range changeDataStream { // Receive from channel
		// var jsonData = <-changeDataStream
		changes := gjson.Get(jsonData, "change").Array()
		for _, change := range changes {
			var c models.ChangeData
			err := json.Unmarshal([]byte(change.Raw), &c)
			if err != nil {
				log.Println("Error parsing change:", err)
				continue
			}

			// Handle different event types
			switch c.Kind {
			case "insert":
				wci.insertIntoMySQL(c)
			case "update":
				wci.updateMySQL(c)
			case "delete":
				wci.deleteFromMySQL(c)
			}
		}
	}
}

var predefinedNames = map[string]string{
	"year": "v_year",
}

func replaceNames(name string) (newName string) {
	if v, ok := predefinedNames[name]; ok {
		return v
	}
	return name
}

// Insert data into MySQL using XORM
func (wci *WALConsumerImpl) insertIntoMySQL(change models.ChangeData) {
	data := make(map[string]interface{})
	for i, col := range change.ColumnNames {
		col = replaceNames(col)
		data[col] = change.ColumnValues[i]
	}

	_, err := wci.engine.Table(change.Table).Insert(data)
	if err != nil {
		log.Println("Error inserting into MySQL:", err)
	} else {
		log.Println("Inserted into MySQL:", data)
	}
}

// Update data in MySQL using XORM
func (wci *WALConsumerImpl) updateMySQL(change models.ChangeData) {
	data := make(map[string]interface{})
	for i, col := range change.ColumnNames {
		col = replaceNames(col)
		data[col] = change.ColumnValues[i]
	}

	// Create a where condition using old key values
	where := make(map[string]interface{})
	for i, key := range change.OldKeys.KeyNames {
		where[key] = change.OldKeys.KeyValues[i]
	}

	_, err := wci.engine.Table(change.Table).Where(where).Update(data)
	if err != nil {
		log.Println("Error updating MySQL:", err)
	} else {
		log.Println("Updated MySQL:", data)
	}
}

// Delete data from MySQL using XORM
func (wci *WALConsumerImpl) deleteFromMySQL(change models.ChangeData) {
	where := make(map[string]interface{})
	for i, key := range change.OldKeys.KeyNames {
		where[key] = change.OldKeys.KeyValues[i]
	}

	_, err := wci.engine.Table(change.Table).Where(where).Delete()
	if err != nil {
		log.Println("Error deleting from MySQL:", err)
	} else {
		log.Println("Deleted from MySQL:", where)
	}
}
