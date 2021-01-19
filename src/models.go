package main

import (
	"fmt"
	"log"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// DataSourceName attributes hold informaition about a specific database.
// The information is used to connect to said database.
type DataSourceName struct {
	Host     string
	Port     string
	User     string
	Password string
	Database string
}

// DSNString returns a data source identifier string
func (dsn *DataSourceName) DSNString() string {
	format := "%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local"
	return fmt.Sprintf(format, dsn.User, dsn.Password, dsn.Host, dsn.Port, dsn.Database)
}

// ProductLine represents the type of information available for a given product line.
// It maps to a record in a database table.
type ProductLine struct {
	ID    uint   `gorm:";primaryKey;autoIncrement"`
	Title string `gorm:"type:varchar(25);unique;not null"`
	Name  string `gorm:"type:varchar(25);unique;not null"`
}

// ProductLineID used when only requesting ProductLine.ID field from corresponding database table.
type ProductLineID struct {
	ID uint
}

// SetInfo represents the type of information available for a given card set.
// It maps to a record in a database table.
type SetInfo struct {
	// Model
	ID            uint   `gorm:"primaryKey;autoIncrement"`
	Name          string `gorm:"type:varchar(70);unique;not null;"`
	Count         uint16 `gorm:"not null"`
	ProductLineID uint
	ProductLine   ProductLine `gorm:"foreignKey:ProductLineID"` // Defines a "Belongs To" relationship with ProductLine model
}

// SetInfoID used when only requesting SetInfo.ID field from corresponding database table.
type SetInfoID struct {
	ID uint
}

// CardInfo represents the type of information available for a given card.
// It maps to a record in a database table.
type CardInfo struct {
	ID            uint    `gorm:";unique;autoIncrement"`
	Number        string  `gorm:"type:varchar(15);primaryKey"`
	Attack        string  `gorm:"type:varchar(5)"`
	Attribute     string  `gorm:"type:varchar(15)"` // (e.g. Earth, Fire, Water, etc..)
	CardType      string  `gorm:"type:varchar(40)"`
	CardTypeB     string  `gorm:"type:varchar(40);"` // Specific type of monster, spell, or trap (e.g. Effect Monster, Ritual Monster)
	Defense       string  `gorm:"type:varchar(5)"`
	LinkRating    string  `gorm:"type:varchar(1)"`
	LinkArrows    string  `gorm:"type:varchar(70)"` // Contains one or more comma separated string values
	Name          string  `gorm:"type:varchar(100);primaryKey"`
	Level         string  `gorm:"type:varchar(4)"`
	MonsterType   string  `gorm:"type:varchar(50)"`
	Rarity        string  `gorm:"type:varchar(25);primaryKey"`
	MarketPrice   float32 `gorm:"type:float(2) unsigned;not null"`
	Description   string  `gorm:"type:text"`
	SetID         uint    `gorm:"primaryKey"`
	ProductLineID uint
	SetInfo       SetInfo     `gorm:"foreignKey:SetID"` // Defines a "Belongs To" relationship with SetInfo model
	ProductLine   ProductLine `gorm:"foreignKey:ProductLineID"`
	ProductID     uint        // Identifes corresponding image
}

// CardImageID is used when only the ID and ProductID fields need to be retrieved
// from the CardInfo database table. Used in the GetImages goroutine.
type CardImageID struct {
	ID        uint // Local image ID
	ProductID uint // Remote image ID
}

// Migrate creates the CardInfo database table and returns
func Migrate(dsn string, args ...interface{}) {
	db, err := gorm.Open(
		mysql.Open(dsn),
		&gorm.Config{Logger: logger.Default.LogMode(logger.Error)},
	)
	if err != nil {
		log.Fatal(err)
	}
	for _, m := range args {
		err = db.AutoMigrate(m)
		if err != nil {
			log.Fatal(err)
		}
	}
}
