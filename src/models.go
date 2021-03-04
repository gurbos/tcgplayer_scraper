package main

// CardImageID temporary table to hold tcg product id
// which is used to identify product image.
type CardImageID struct {
	OldID uint `gorm:"primaryKey"` // Product ID assigned by tcgplayer
	NewID uint `gorm:"not null"`   // ID of the corresponding card info
}
