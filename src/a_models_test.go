package main

import (
	"testing"

	"gorm.io/gorm/logger"
)

// TEST: DataSourceName.DSNString
func TestDSNString(t *testing.T) {
	var dsn string = "USER:PASSWORD@tcp(127.0.0.1:3306)/DATABASE?charset=utf8mb4&parseTime=True&loc=Local"
	dataSource := DataSourceName{
		Host:     "127.0.0.1",
		Port:     "3306",
		User:     "USER",
		Password: "PASSWORD",
		Database: "DATABASE",
	}

	res := dataSource.DSNString()
	if res != dsn {
		t.Fatal("Expected", dsn, "\nGot", res)
	}
}

// TEST: dropTables
func TestDropTables(t *testing.T) {
	ds := GetTestDataSource()
	db := GetDBConnection(ds.DSNString(), logger.Silent)
	DropTables(db)
}

// TEST: Migrate
func TestMigrate(t *testing.T) {
	ds := GetTestDataSource()
	dsn := ds.DSNString()
	Migrate(dsn, &ProductLine{}, &SetInfo{}, &CardInfo{})
	db := GetDBConnection(dsn, logger.Silent)
	if !db.Migrator().HasTable(&ProductLine{}) {
		t.Fatal("Failed to create table for ProductLine model")
	}
	if !db.Migrator().HasTable(&SetInfo{}) {
		t.Fatal("Failed to create table for SetInfo model")
	}
	if !db.Migrator().HasTable(&CardInfo{}) {
		t.Fatal("Failed to create table for CardInfo model")
	}
	// dropTables(db)
}
