package main

// ProductLine represents the type of information available for a given product line.
// It maps to a record in a database table.
type ProductLine struct {
	ID        uint   `gorm:";primaryKey;autoIncrement"`
	Title     string `gorm:"type:varchar(25);unique;not null"`
	Name      string `gorm:"type:varchar(25);unique;not null"`
	URLName   string `gorm:"type:varchar(32);unique;not null"`
	SetCount  uint
	CardCount uint
}

// SetInfo represents the type of information available for a given card set.
// It maps to a record in a database table.
type SetInfo struct {
	// Model
	ID            uint   `gorm:"primaryKey;autoIncrement"`
	Title         string `gorm:"type:varchar(70);unique;not null;"`
	Name          string `gorm:"type:varchar(75);unique;not null"`
	Count         uint16 `gorm:"not null"`
	ProductLineID uint
	ProductLine   ProductLine `gorm:"foreignKey:ProductLineID"` // Defines a "Belongs To" relationship with ProductLine model
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
