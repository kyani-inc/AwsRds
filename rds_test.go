package AwsRds

import (
	"fmt"
	"testing"
)

func TestRegisterCluster(t *testing.T) {
	err := RegisterCluster("au-bi-151012-cluster", "Interrupt", "root", "kyaniadmin")

	if err != nil {
		t.Error(err)
	}

	rows, err := Query("au-bi-151012-cluster", "Interrupt", "Select * from Interrupt")

	fmt.Println(rows.Columns())

	if err != nil {
		t.Error(err)
	}

	db := DB("au-bi-151012-cluster", "Interrupt", false)

	if db != nil {
		rows, err = db.Query("Select * from Interrupt")
	}

	if err != nil {
		t.Error(err)
	}

	fmt.Println(rows.Columns())

	fmt.Println(RegisteredDbMap)
}
