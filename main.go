package main

import (
	"crypto/tls"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/BurntSushi/toml"
	sqlite "github.com/gwenn/gosqlite"
	"github.com/jackc/pgx"
	"github.com/mitchellh/go-homedir"
)

// Configuration file
type TomlConfig struct {
	Geo GeoInfo
	Pg  PGInfo
}
type GeoInfo struct {
	Path string // Path to the Geo-IP.sqlite file
}
type PGInfo struct {
	Database       string
	NumConnections int `toml:"num_connections"`
	Port           int
	Password       string
	Server         string
	SSL            bool
	Username       string
}

type oneRow struct {
	ipFrom   int
	ipTo     int
	registry string
	assigned int
	ctry     string
	cntry    string
	country  string
}

var (
	// Application config
	Conf TomlConfig

	// Display debugging messages?
	debug = true

	// PostgreSQL Connection pool
	pg *pgx.ConnPool

	// SQLite pieces
	sdb *sqlite.Conn
)

func main() {
	// Override config file location via environment variables
	var err error
	configFile := os.Getenv("CONFIG_FILE")
	if configFile == "" {
		userHome, err := homedir.Dir()
		if err != nil {
			log.Fatalf("User home directory couldn't be determined: %s", "\n")
		}
		configFile = filepath.Join(userHome, ".db4s", "status_updater.toml")
	}

	// Read our configuration settings
	if _, err = toml.DecodeFile(configFile, &Conf); err != nil {
		log.Fatal(err)
	}

	// Open the Geo-IP database, for country lookups
	sdb, err = sqlite.Open(Conf.Geo.Path)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err = sdb.Close()
		if err != nil {
			log.Println(err)
		}
	}()

	// Log successful connection
	if debug {
		fmt.Printf("Connected to Geo-IP database: %v\n", Conf.Geo.Path)
	}

	// Setup the PostgreSQL config
	pgConfig := new(pgx.ConnConfig)
	pgConfig.Host = Conf.Pg.Server
	pgConfig.Port = uint16(Conf.Pg.Port)
	pgConfig.User = Conf.Pg.Username
	pgConfig.Password = Conf.Pg.Password
	pgConfig.Database = Conf.Pg.Database
	clientTLSConfig := tls.Config{InsecureSkipVerify: true}
	if Conf.Pg.SSL {
		pgConfig.TLSConfig = &clientTLSConfig
	} else {
		pgConfig.TLSConfig = nil
	}

	// Connect to PG
	pgPoolConfig := pgx.ConnPoolConfig{*pgConfig, Conf.Pg.NumConnections, nil, 5 * time.Second}
	pg, err = pgx.NewConnPool(pgPoolConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer pg.Close()

	// Log successful connection
	if debug {
		fmt.Printf("Connected to PostgreSQL server: %v\n", Conf.Pg.Server)
	}

	// Begin PostgreSQL transaction
	tx, err := pg.Begin()
	if err != nil {
		log.Fatal(err)
	}
	// Set up an automatic transaction roll back if the function exits without committing
	defer func() {
		err = tx.Rollback()
		if err != nil {
			log.Println(err)
		}
	}()

	// Drop existing PG tables holding the IP country lookup data
	fmt.Print("Dropping existing IPv4 data table from PG")
	dbQuery := `DROP TABLE IF EXISTS country_code_lookups`
	_, err = tx.Exec(dbQuery)
	if err != nil {
		log.Fatal(err)
	}

	// Create the PG tables to hold the country lookup data
	fmt.Print("Creating new IPv4 data table in PG")
	dbQuery = `
		CREATE TABLE country_code_lookups (
			ipfrom bigint constraint country_code_lookups_pk primary key,
			ipto bigint,
			registry text,
			assigned bigint,
			ctry text,
			cntry text,
			country text
		)`
	_, err = tx.Exec(dbQuery)
	if err != nil {
		log.Fatal(err)
	}

	// Import the IP country lookup data from SQLite to PG
	fmt.Print("Importing IPv4 data table from SQLite to PG")
	sQuery := `
		SELECT IPFROM, IPTO, REGISTRY, ASSIGNED, CTRY, CNTRY, COUNTRY
		FROM ipv4
		ORDER BY IPFROM ASC`
	err = sdb.Select(sQuery, func(s *sqlite.Stmt) (innerErr error) {
		var row oneRow
		innerErr = s.Scan(&row.ipFrom, &row.ipTo, &row.registry, &row.assigned, &row.ctry, &row.cntry, &row.country)
		if innerErr != nil {
			return
		}

		// Insert the row into PG
		innerErr = insertIPv4PGData(tx, row)
		return
	})
	if err != nil {
		log.Fatal(err)
	}

	// TODO: Import the IPv6 data from SQLite to PG

	// Create appropriate indexes on the new PG country lookup data
	fmt.Print("Creating indexes in PG")
	dbQuery = `
		CREATE INDEX country_code_lookups_ipto_index
		ON country_code_lookups (ipto)`
	_, err = tx.Exec(dbQuery)
	if err != nil {
		log.Fatal(err)
	}

	// Verify the same number of entries in both the SQLite and PG tables
	var pgRowCount, sRowCount int
	dbQuery = `SELECT count(*) FROM country_code_lookups`
	err = tx.QueryRow(dbQuery).Scan(&pgRowCount)
	if err != nil {
		log.Fatalf("error when counting rows in the pg table: %v\n", err)
	}

	sQuery = `SELECT count(*) FROM ipv4`
	err = sdb.Select(sQuery, func(s *sqlite.Stmt) (innerErr error) {
		innerErr = s.Scan(&sRowCount)
		return
	})
	if err != nil {
		log.Fatal(err)
	}
	if pgRowCount != sRowCount {
		log.Fatalf("Mismatching IPv4 row counts after import.  SQLite: %d, PostgreSQL: %d\n", sRowCount, pgRowCount)
	}

	// TODO: Figure out why the transaction is already complete here
	//// Commit PostgreSQL transaction
	//err = tx.Commit()
	//if err != nil {
	//	log.Fatal(err)
	//}

	// Let the user know the import is complete
	fmt.Println("Import of SQLite country lookup data is complete")
}

// Inserts a single data record into the PostgreSQL database
func insertIPv4PGData(tx *pgx.Tx, row oneRow) (err error) {
	var tag pgx.CommandTag
	dbQuery := `
		INSERT INTO country_code_lookups (ipfrom, ipto, registry, assigned, ctry, cntry, country)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`
	tag, err = tx.Exec(dbQuery, row.ipFrom, row.ipTo, row.registry, row.assigned, row.ctry, row.cntry, row.country)
	if err != nil {
		log.Fatal(err)
	}
	if numRows := tag.RowsAffected(); numRows != 1 {
		log.Printf("Wrong number of rows affected (%d) when insert ip lookup data. ipfrom: %v, \n", numRows, row.ipFrom)
	}
	return
}
