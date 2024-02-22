package main

import (
	"archive/tar"
	"compress/gzip"
	"database/sql"
	"encoding/csv"
	"fmt"
	"flag"
	"io"
	"os"
	_ "github.com/lib/pq"
)

var (
	version bool
)

func getEnv(key, fallback string) string {
    if value, ok := os.LookupEnv(key); ok {
	return value
    }
    return fallback
}

func init() {
	flag.BoolVar(&version, "version", false, "Print version")
	flag.Parse()
}


func main() {

	if version {
		fmt.Println("unlock-pg 0.0.1")
		os.Exit(0)
	}


	// Read PostgreSQL connection parameters from command line arguments or environment variables
	var username, password, dbname, host, port, appname string

	if len(os.Args) >= 6 {
		host = os.Args[1]
		username = os.Args[2]
		password = os.Args[3]
		dbname = os.Args[4]
		port = os.Args[5]
	} else {
		// If not provided as command line arguments, read from environment variables
		host = os.Getenv("PGHOST")
		username = os.Getenv("PGUSER")
		password = os.Getenv("PGPASSWORD")
		dbname = os.Getenv("PGDATABASE")
		port = os.Getenv("PGPORT")
		appname = getEnv("PGAPPNAME", "unlock-postgres")
	}

	// Construct connection string
	connectionString := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s application_name=%s sslmode=disable", host, port, username, password, dbname, appname)

	fmt.Println("Connect on :", connectionString)


	export_table(	connectionString, "class")
	export_table(	connectionString, "database")
	export_table(	connectionString, "index")
	export_table(	connectionString, "namespace")
	export_table(	connectionString, "settings")
	export_table(	connectionString, "tables")

	export	(connectionString, "database_size", "SELECT datname, pg_database_size(datname) as datsize from pg_database")

	export	(connectionString, "tables_size", "with tt as (SELECT schemaname,tablename,schemaname::text ||'.'||tablename::text as tn FROM pg_tables WHERE schemaname::text NOT IN ('pg_catalog') ) SELECT schemaname,tablename,pg_relation_size(tn),pg_relation_size(tn, 'fsm') as fsm, pg_relation_size(tn, 'main') as main, pg_relation_size(tn, 'vm') as vm from tt;")

	compress( []string{"up_class.csv","up_index.csv"})
}

func export_table(connectionString string, tableName string) {
	export	(connectionString, tableName, "SELECT * FROM pg_" + tableName)
}

func compress(files []string) {
	// Create a new .tar file
	tarFile, err := os.Create("unlock.tar")
	if err != nil {
		fmt.Println("Error creating tar file:", err)
		return
	}
	defer tarFile.Close()

	// Create a tar writer
	tarWriter := tar.NewWriter(tarFile)
	defer tarWriter.Close()


	// Iterate over each file and add it to the .tar archive
	for _, filename := range files {
		// Open the file
		file, err := os.Open(filename)
		if err != nil {
			fmt.Printf("Error opening file %s: %s\n", filename, err)
			continue
		}
		defer file.Close()

		// Get file info
		info, err := file.Stat()
		if err != nil {
			fmt.Printf("Error getting file info for %s: %s\n", filename, err)
			continue
		}

		// Create a tar header
		header := &tar.Header{
			Name: filename,
			Size: info.Size(),
			Mode:    int64(info.Mode()), // Set file mode from file info
			ModTime: info.ModTime(),
		}

		// Write the header to the tar writer
		if err := tarWriter.WriteHeader(header); err != nil {
			fmt.Printf("Error writing header for %s: %s\n", filename, err)
			continue
		}

		// Copy file contents to the tar writer
		if _, err := io.Copy(tarWriter, file); err != nil {
			fmt.Printf("Error writing file %s contents to tar: %s\n", filename, err)
			continue
		}
	}

	fmt.Println("Tar file created successfully")

    // Open the original file
    originalFile, err := os.Open("unlock.tar")
    if err != nil {
	panic(err)
    }
    defer originalFile.Close()

    // Create a new gzipped file
    gzippedFile, err := os.Create("example.txt.gz")
    if err != nil {
	panic(err)
    }
    defer gzippedFile.Close()

    // Create a new gzip writer
    gzipWriter := gzip.NewWriter(gzippedFile)
    defer gzipWriter.Close()

    // Copy the contents of the original file to the gzip writer
    _, err = io.Copy(gzipWriter, originalFile)
    if err != nil {
	panic(err)
    }

    // Flush the gzip writer to ensure all data is written
    gzipWriter.Flush()


}

func export(connectionString string, filename string, query string) {
	// Connect to the database
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		fmt.Println("Error connecting to the database:", err)
		return
	}
	defer db.Close()


	// Perform a query
	rows, err := db.Query(query)
	if err != nil {
		fmt.Println("Error executing query:", err)
		os.Exit(1)
		return
	}
	defer rows.Close()


	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		fmt.Println("Error getting column names:", err)
		return
	}

	// Create a CSV file
	file, err := os.Create("up_" + filename + ".csv")
	if err != nil {
		fmt.Println("Error creating CSV file:", err)
		return
	}
	defer file.Close()

	// Create a CSV writer
	csvWriter := csv.NewWriter(file)
	defer csvWriter.Flush()

	// Write header to CSV file
	err = csvWriter.Write(columns)
	if err != nil {
		fmt.Println("Error writing header to CSV:", err)
		return
	}


	data := make(map[string]string)
	for rows.Next() {
		columns := make([]string, len(columns))

		columnPointers := make([]interface{}, len(columns))
		for i, _ := range columns {
			columnPointers[i] = &columns[i]
		}

		rows.Scan(columnPointers...)

		for i, colName := range columns {
			data[colName] = columns[i]
		}


		// Convert slice of interface{} to slice of string with double quotes
		stringValues := make([]string, len(columns))

		for i, value := range columns {

		// Enclose each value in double quotes and escape existing double quotes
			stringValues[i] = fmt.Sprintf("%v", value)
		}

		// Write values to CSV
		if err := csvWriter.Write(stringValues); err != nil {
			fmt.Println("Error writing row to CSV:", err)
			return
		}

	}
	fmt.Println("Data exported for : " + filename)

}
