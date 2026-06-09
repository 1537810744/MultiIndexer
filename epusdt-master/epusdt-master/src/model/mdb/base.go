package mdb

import (
	"github.com/dromara/carbon/v2"
	"gorm.io/gorm"
)

type BaseModel struct {
	ID        uint64         `gorm:"column:id;primary_key;autoIncrement" json:"id" example:"1"`
	CreatedAt carbon.Time    `gorm:"column:created_at" json:"created_at" example:"2026-04-16 12:00:00"`
	UpdatedAt carbon.Time    `gorm:"column:updated_at" json:"updated_at" example:"2026-04-16 12:00:00"`
	DeletedAt gorm.DeletedAt `gorm:"index" json:"deleted_at"`
}
