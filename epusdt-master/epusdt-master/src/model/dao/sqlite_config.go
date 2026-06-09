package dao

import (
	"database/sql"

	"gorm.io/gorm"
)

func configureSQLite(db *gorm.DB, maxOpenConns int) (*sql.DB, error) {
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}

	if maxOpenConns <= 0 {
		maxOpenConns = 1
	}
	sqlDB.SetMaxOpenConns(maxOpenConns)
	sqlDB.SetMaxIdleConns(1)

	if err := db.Exec("PRAGMA journal_mode=WAL;").Error; err != nil {
		return nil, err
	}
	if err := db.Exec("PRAGMA synchronous=NORMAL;").Error; err != nil {
		return nil, err
	}
	if err := db.Exec("PRAGMA busy_timeout=5000;").Error; err != nil {
		return nil, err
	}
	if err := sqlDB.Ping(); err != nil {
		return nil, err
	}

	return sqlDB, nil
}
