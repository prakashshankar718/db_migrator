package producerPG

import (
	"bufio"
	"context"
	"log"
	"os/exec"

	"xorm.io/xorm"
)

type PgWAL interface {
	StreamWAL(ctx context.Context, ChangeDataStream chan<- string)

	// StartChangeDataCapture()
}

type pgWALImpl struct {
	dbName string
	slot   string
	engine *xorm.Engine
}

// Change struct for JSON parsing
// type Change struct {
// 	Kind         string        `json:"kind"`
// 	Schema       string        `json:"schema"`
// 	Table        string        `json:"table"`
// 	ColumnNames  []string      `json:"columnnames"`
// 	ColumnValues []interface{} `json:"columnvalues"`
// 	OldKeys      struct {
// 		KeyNames  []string      `json:"keynames"`
// 		KeyValues []interface{} `json:"keyvalues"`
// 	} `json:"oldkeys"`
// }

func NewPgWALConfig(dbname, slot string, engine *xorm.Engine) PgWAL {
	return &pgWALImpl{
		dbName: dbname,
		slot:   slot,
		engine: engine,
	}
}

// Start WAL log streaming
func (pgw *pgWALImpl) StreamWAL(ctx context.Context, ChangeDataStream chan<- string) {
	// Ensure pg_recvlogical command is correctly formatted
	cmd := exec.Command("pg_recvlogical",
		"-U", "postgres",
		"-h", "localhost",
		"-p", "5432",
		"-d", pgw.dbName,
		"--slot", pgw.slot,
		"--start",
		"-f", "-",
	)

	// Get output pipe
	out, err := cmd.StdoutPipe()
	if err != nil {
		log.Println("❌ Error creating pipe:", err)
		return
	}

	// Start the command execution
	err = cmd.Start()
	if err != nil {
		log.Println("❌ Error starting pg_recvlogical:", err)
		return
	}

	log.Println("✅ Streaming started...")

	// Read output in a non-blocking way
	scanner := bufio.NewScanner(out)
	go func() {
		defer close(ChangeDataStream)
		for scanner.Scan() {
			jsonData := scanner.Text()
			if jsonData != "" {
				log.Println("📥 Received WAL change:", jsonData)
				ChangeDataStream <- jsonData
			}
		}

		if err := scanner.Err(); err != nil {
			log.Println("❌ Error reading WAL stream:", err)
			close(ChangeDataStream)
		}
	}()

	// Handle shutdown using context
	<-ctx.Done()
	log.Println("🛑 Stopping WAL streaming...")

	// Kill pg_recvlogical process
	if err := cmd.Process.Kill(); err != nil {
		log.Println("❌ Error killing process:", err)
	} else {
		log.Println("✅ WAL streaming stopped gracefully.")
	}
}
