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

// func TestGetImages(t *testing.T) {
// 	dsn := GetDataSource()
// 	db := GetDBConnection(dsn.DSNString(), logger.Silent)

// 	// Get generic database connection handle and configure max connections and idle connections
// 	gen, err := db.DB()
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	gen.SetMaxOpenConns(10)
// 	gen.SetMaxIdleConns(10)

// 	var cardTotal int64
// 	tx := db.Model(&tcm.CardInfo{}).Count(&cardTotal)
// 	if tx.Error != nil {
// 		log.Fatal(tx.Error)
// 	}

// 	numCPUThread := runtime.NumCPU() * 2
// 	runtime.GOMAXPROCS(numCPUThread)

// 	var wg sync.WaitGroup
// 	wg.Add(numCPUThread) // Specify the number of goroutines to wait on
// 	var imgListSize int64 = 50
// 	imgInfoChan := make(chan []CardImageID, numCPUThread*2)

// 	// Create goroutines to get card images
// 	for i := 0; i < numCPUThread; i++ {
// 		go GetImages(&wg, imgInfoChan)
// 	}

// 	var count int64 = 0
// 	for count < 1000 {
// 		imgIDList := make([]CardImageID, int(imgListSize), int(imgListSize))
// 		tx = db.Model(&tcm.CardInfo{}).Where("ID > ?", count).Limit(50).Find(&imgIDList)
// 		count += tx.RowsAffected
// 		imgInfoChan <- imgIDList
// 		fmt.Printf("Images Downloaded: %5d\n", count)
// 	}

// 	for i := 0; i < numCPUThread; i++ {
// 		imgInfoChan <- nil
// 	}
// 	wg.Wait()
// }
