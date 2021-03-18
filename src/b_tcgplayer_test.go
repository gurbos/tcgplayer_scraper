package main

import (
	"log"
	"testing"

	tcm "github.com/gurbos/tcmodels"
	"gorm.io/gorm/logger"
)

// TEST: WriteProductLine
func TestWriteProductLine(t *testing.T) {
	dataSource := GetDataSource()
	db := GetDBConnection(dataSource.DSNString(), logger.Silent)

	err := db.AutoMigrate(&tcm.ProductLine{}) // Create database table for ProductLine model
	if err != nil {
		log.Fatal(err)
	}

	requestData := GetRequestPayload("YuGiOh", "Cards", "", 0)
	var responseData *ResponsePayload
	for true {
		responseData, err = MakeTcgPlayerRequest(requestData.ToJSON(), 20)
		if err != nil {
			continue
		} else {
			break
		}
	}
	WriteProductLineInfo(db, responseData.Results[0])
	var entryCount int64
	db.Model(&tcm.ProductLine{}).Count(&entryCount)
	if entryCount != 1 {
		t.Fatal("Expected database entries: ", 1, "Got: ", entryCount)
	}
}

// TEST: MakeSetMap
func TestMakeSetMap(t *testing.T) {
	ds := GetDataSource()
	db := GetDBConnection(ds.DSNString(), logger.Silent)

	productLineName := "YuGiOh"
	var setCount int64
	db.Model(&tcm.SetInfo{}).Count(&setCount)

	setMap, err := MakeSetMap(db, productLineName)

	if err != nil {
		t.Fatal(err)
	}
	if int64(len(setMap)) != setCount {
		t.Fatal("Expected length:", setCount, "  Got:", len(setMap))
	}
}

// Clean up after testing
func TestCleanUp(t *testing.T) {
	dataSource := GetDataSource()
	db := GetDBConnection(dataSource.DSNString(), logger.Silent)
	DropTables(db)
}

func TestMigrate(t *testing.T) {
	ds := GetDataSource()
	Migrate(ds.DSNString(), tcm.ProductLine{}, tcm.SetInfo{}, tcm.YuGiOhCardInfo{})
}
