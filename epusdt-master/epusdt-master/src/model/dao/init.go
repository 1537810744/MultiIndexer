package dao

import (
	"log"
)

func Init() {
	if err := DBInit(); err != nil {
		log.Fatalf("DBInit err: %v", err)
	}

	if err := RuntimeInit(); err != nil {
		log.Fatalf("RuntimeInit err: %v", err)
	}
}
