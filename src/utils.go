package main

import (
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/joho/godotenv"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// GetDBConnection returns a database connection handle
func GetDBConnection(dsn string, logLevel logger.LogLevel) *gorm.DB {
	db, err := gorm.Open(
		mysql.Open(dsn),
		&gorm.Config{Logger: logger.Default.LogMode(logLevel)},
	)
	if err != nil {
		log.Fatal(err)
	}
	return db
}

// DropTables drops all database tables;
func DropTables(db *gorm.DB) {
	gen, err := db.DB()
	if err != nil {
		log.Fatal(err)
	}
	rows, err := gen.Query(
		"SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA=(?)",
		db.Migrator().CurrentDatabase(),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	var name string
	tx := db.Exec("SET FOREIGN_KEY_CHECKS = 0;") // Disable checks for foreign key relationships
	if tx.Error != nil {
		log.Fatal(tx.Error)
	}
	for rows.Next() {
		rows.Scan(&name)
		tx := db.Exec(fmt.Sprintf("DROP TABLE %s", name))
		if tx.Error != nil {
			log.Fatal(tx.Error)
		}
	}
	_ = db.Exec("SET FOREIGN_KEY_CHECKS = 1;") // Re-enable checks for foreign key relationships
}

func deleteTableRecords(db *gorm.DB) {
	gen, err := db.DB()
	if db.Migrator().HasTable(&CardInfo{}) {
		_, err = gen.Exec("DELETE FROM card_infos;")
		if err != nil {
			log.Println(err)
		}
	}
	if db.Migrator().HasTable(&SetInfo{}) {
		_, err = gen.Exec("DELETE FROM set_infos;")
		if err != nil {
			log.Println(err)
		}
	}
	if db.Migrator().HasTable(&ProductLine{}) {
		_, err = gen.Exec("DELETE FROM product_lines;")
		if err != nil {
			log.Println(err)
		}
	}
}

// GetTestDataSource returns a DataSourceName object that specifies
// the data source used during testing.
func GetTestDataSource() DataSourceName {
	godotenv.Load("/home/gurbos/Projects/golang/tcgplayer_scraper/.env")
	dataSource := DataSourceName{
		Host:     os.Getenv("TEST_HOST"),
		Port:     os.Getenv("TEST_PORT"),
		User:     os.Getenv("TEST_USER"),
		Password: os.Getenv("TEST_PASSWD"),
		Database: os.Getenv("TEST_DB"),
	}
	return dataSource
}

// GetDataSource returns a DataSourceName object with that specifies
// the data source used during production.
func GetDataSource() DataSourceName {
	godotenv.Load("/home/gurbos/Projects/golang/tcgplayer_scraper/.env")
	dataSource := DataSourceName{
		Host:     os.Getenv("DB_HOST"),
		Port:     os.Getenv("DB_PORT"),
		User:     os.Getenv("DB_USER"),
		Password: os.Getenv("DB_PASSWD"),
		Database: os.Getenv("DB_NAME"),
	}
	return dataSource
}

// DatabaseConnConfig Set the max number of open database and idle database connections
func DatabaseConnConfig(db *gorm.DB, numConn int, numIdleConn int) error {
	gen, err := db.DB()
	if err != nil {
		return err
	}
	gen.SetMaxOpenConns(numConn)
	gen.SetMaxIdleConns(numIdleConn)
	return nil
}

func TerminateCardInfoGoroutines(wg *sync.WaitGroup, dataChan chan *RequestPayload, numRoutines int) {
	for i := 0; i < numRoutines; i++ {
		dataChan <- nil
	}
	wg.Wait()
}

func TerminateCardImageGoroutines(wg *sync.WaitGroup, dataChan chan []CardImageID, numRoutines int) {
	for i := 0; i < numRoutines; i++ {
		dataChan <- nil
	}
	wg.Wait()
}
