package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"maps"
	"slices"
	"strings"

	"cloud.google.com/go/firestore"
	_ "github.com/lib/pq"
	"google.golang.org/api/option"
)

// Configuration struct to hold database and Firestore configurations
type Config struct {
	PostgresHost          string
	PostgresPort          string
	PostgresUser          string
	PostgresPassword      string
	PostgresDB            string
	FirestoreProject      string
	FirestoreCred         string // Path to Firestore credentials JSON
	TableName             string
	SearchCollectionName  string
	ContentCollectionName string
}

type SearchRecord struct {
	Title     string   `firestore:"title"`
	SubTitle  string   `firestore:"subtitle"`
	Keywords  []string `firestore:"keywords"`
	Published bool     `firestore:"published"`
}

type ContentRecord struct {
	Chunks   []Chunk  `firestore:"chunks"`
	Keywords []string `firestore:"keywords"`
}

type Chunk struct {
	Type string `firestore:"type"`
	Data string `firestore:"data"`
}

func main() {
	// Load configuration (You can enhance this to load from env or config files)
	config := Config{
		PostgresHost:          "localhost",
		PostgresPort:          "5432",
		PostgresUser:          "postgres",
		PostgresPassword:      "password",
		PostgresDB:            "cw-prod-content-maths",
		FirestoreProject:      "clayway-maths",
		FirestoreCred:         "C:\\Users\\Peter\\Downloads\\clayway-maths-b776845f672e.json",
		TableName:             "public.content",
		SearchCollectionName:  "search",
		ContentCollectionName: "content",
	}

	// Initialize PostgreSQL connection
	pgConnStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		config.PostgresHost,
		config.PostgresPort,
		config.PostgresUser,
		config.PostgresPassword,
		config.PostgresDB,
	)

	pgDB, err := sql.Open("postgres", pgConnStr)
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer pgDB.Close()

	// Verify PostgreSQL connection
	if err := pgDB.Ping(); err != nil {
		log.Fatalf("PostgreSQL ping failed: %v", err)
	}
	log.Println("Connected to PostgreSQL successfully.")

	// Initialize Firestore client
	ctx := context.Background()
	firestoreClient, err := firestore.NewClient(ctx, config.FirestoreProject, option.WithCredentialsFile(config.FirestoreCred))
	if err != nil {
		log.Fatalf("Failed to create Firestore client: %v", err)
	}
	defer firestoreClient.Close()
	log.Println("Connected to Firestore successfully.")

	// Query data from PostgreSQL
	query := fmt.Sprintf("SELECT * FROM %s WHERE not(question) and not(archived)", config.TableName)
	rows, err := pgDB.Query(query)
	if err != nil {
		log.Fatalf("Failed to execute query: %v", err)
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		log.Fatalf("Failed to get columns: %v", err)
	}

	// Prepare a slice of interfaces to scan the row data
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	// Iterate through the rows
	count := 0
	for rows.Next() {
		err := rows.Scan(valuePtrs...)
		if err != nil {
			log.Printf("Failed to scan row: %v", err)
			continue
		}

		// Create a map for Firestore document
		docData := make(map[string]interface{})
		keywords := make([]string, 0)
		var docID string // Assuming you have an 'id' column for document ID

		searchRow := SearchRecord{}
		contentRow := ContentRecord{}
		for i, col := range columns {
			val := values[i]

			// Type assertions based on PostgreSQL types
			switch v := val.(type) {
			case int64:
				docData[col] = v
			case float64:
				docData[col] = v
			case bool:
				docData[col] = v
			case []byte:
				// Assuming this is a string
				s := string(v)
				docData[col] = s
			case string:
				docData[col] = v
			default:
				docData[col] = v
			}

			// 	// If the column is 'id', use it as document ID
			if col == "id" {
				docID = fmt.Sprintf("%v", docData[col])
			}
			if col == "title" {
				searchRow.Title = docData[col].(string)
			}
			if col == "subtitle" {
				searchRow.SubTitle = docData[col].(string)
			}

			if col == "published" {
				searchRow.Published = docData[col].(bool)
			}

			if len(searchRow.Title) > 0 {
				keywords = append(keywords, strings.Split(searchRow.Title, " ")...)
			}

			if col == "tags" {
				if docData[col] != nil {
					foo := string(docData[col].(string))
					foo = foo[1:]
					foo = foo[:len(foo)-1]
					bar := strings.Split(foo, ",")
					keywords = append(keywords, bar...)
				}
			}
		}

		for i, w := range keywords {
			keywords[i] = strings.ToLower(w)
		}
		keys := make(map[string]int64)
		for _, word := range keywords {
			for _, w := range strings.Split(word, " ") {
				keys[w] = 0
			}
		}
		contentRow.Keywords = append(contentRow.Keywords, slices.Sorted(maps.Keys(keys))...)

		extraKeys := make(map[string]int64)
		for _, word := range keywords {
			for _, w := range strings.Split(word, " ") {
				max := len(w) - 1
				if len(w) >= 6 {
					max = 6
				}
				if len(w) > 2 {
					for i := 2; i <= max; i++ {
						extraKeys[w[:i]] = 0
					}
				}
				extraKeys[w] = 0
			}

		}
		searchRow.Keywords = append(searchRow.Keywords, slices.Sorted(maps.Keys(extraKeys))...)
		contentRow.Chunks = makeContent(docID, pgDB)

		// If no 'id' column, Firestore will auto-generate a document ID
		var docRef *firestore.DocumentRef
		docRef = firestoreClient.Collection(config.SearchCollectionName).Doc(docID)
		// Write to Firestore
		_, err = docRef.Set(ctx, searchRow)
		if err != nil {
			log.Printf("Failed to write document to Firestore: %v", err)
			continue
		}

		docRef = firestoreClient.Collection(config.ContentCollectionName).Doc(docID)
		// Write to Firestore
		_, err = docRef.Set(ctx, contentRow)

		count++
		if count%100 == 0 {
			log.Printf("%d documents written to Firestore.", count)
		}
	}

	if err := rows.Err(); err != nil {
		log.Fatalf("Error iterating rows: %v", err)
	}

	log.Printf("Data migration completed. Total documents written: %d", count)
}

func makeContent(id string, pgDB *sql.DB) []Chunk {

	// Query data from PostgreSQL
	query := "SELECT chunk_type, data FROM content_chunks WHERE not(archived) and content_id = '" + id + "' ORDER BY position"
	rows, err := pgDB.Query(query)
	if err != nil {
		log.Fatalf("Failed to execute query: %v", err)
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		log.Fatalf("Failed to get columns: %v", err)
	}

	// Prepare a slice of interfaces to scan the row data
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	// Iterate through the rows
	//count := 0
	result := make([]Chunk, 0)
	for rows.Next() {
		err := rows.Scan(valuePtrs...)
		if err != nil {
			log.Printf("Failed to scan row: %v", err)
			continue
		}

		c := Chunk{}
		docData := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]

			switch v := val.(type) {
			case int64:
				docData[col] = v
			case float64:
				docData[col] = v
			case bool:
				docData[col] = v
			case []byte:
				// Assuming this is a string
				s := string(v)
				docData[col] = s
			case string:
				docData[col] = v
			default:
				docData[col] = v
			}

			if col == "chunk_type" {
				c.Type = docData[col].(string)
			}
			if col == "data" {
				c.Data = cleanData(docData[col].(string))
			}

		}
		result = append(result, c)
	}
	return result
}

func cleanData(data string) string {
	//1. add to any img: style="max-width: 90% !important;"  where it has: <img src="data:image/png;base64,
	data = strings.ReplaceAll(data, `<img src="data:image/png;base64,`, `<img style="max-width: 90% !important;" src="data:image/png;base64,`)
	return data
}
