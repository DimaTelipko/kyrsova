package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"

	pgx "github.com/jackc/pgx/v5"
)

type Unit struct {
	Id      int `json:"id" db:"id"`
	Signal1 int `json:"signal1" db:"signal1"`
	Signal2 int `json:"signal2" db:"signal2"`
	Signal3 int `json:"signal3" db:"signal3"`
	Signal4 int `json:"signal4" db:"signal4"`
	UnitN   int `json:"unit_number"`
}

func establsihDBConn() (*pgx.Conn, error) {
	conn, err := pgx.Connect(context.Background(), "postgres://postgres:12345@BD:5432/units?sslmode=disable")

	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		return nil, err

	}

	return conn, nil
}

func mainFetchData(w http.ResponseWriter, r *http.Request) {
	dpConnection, err := establsihDBConn()
	if err != nil {
		fmt.Println("Unable to connect to database", err)
		return
	}

	defer dpConnection.Close(context.Background())

	var unit1Week []Unit
	var unit2Week []Unit
	var unit3Week []Unit

	rows1, _ := dpConnection.Query(context.Background(), "SELECT * FROM unit1 ORDER BY id ASC")

	for rows1.Next() {
		var unit Unit
		err := rows1.Scan(&unit.Id, &unit.Signal1, &unit.Signal2, &unit.Signal3)

		if err != nil {
			fmt.Fprintf(w, "The error during db fetch accured:%v /n", err)
			return
		}
		unit1Week = append(unit1Week, unit)
	}

	fmt.Fprintf(w, "\nUnit #1")
	for _, unit := range unit1Week {
		fmt.Fprintf(w, "\nDay: %v, Signal1:  %v, Signal2: %v, Signal3: %v", unit.Id, unit.Signal1, unit.Signal2, unit.Signal3)
	}

	rows2, _ := dpConnection.Query(context.Background(), "SELECT * FROM unit2 ORDER BY id ASC")

	for rows2.Next() {
		var unit Unit
		err := rows2.Scan(&unit.Id, &unit.Signal1, &unit.Signal2, &unit.Signal3)

		if err != nil {
			fmt.Fprintf(w, "The error during db fetch accured:%v/n", err)
			return
		}
		unit2Week = append(unit2Week, unit)
	}

	fmt.Fprintf(w, "\nUnit #2")
	for _, unit := range unit2Week {
		fmt.Fprintf(w, "\nDay: %v, Signal1:  %v, Signal2: %v, Signal3: %v", unit.Id, unit.Signal1, unit.Signal2, unit.Signal3)
	}

	rows3, _ := dpConnection.Query(context.Background(), "SELECT * FROM unit3 ORDER BY id ASC")

	for rows3.Next() {
		var unit Unit
		err := rows3.Scan(&unit.Id, &unit.Signal1, &unit.Signal2, &unit.Signal3, &unit.Signal4)

		if err != nil {
			fmt.Fprintf(w, "The error during db fetch accured:%v /n", err)
			return
		}
		unit3Week = append(unit3Week, unit)
	}

	fmt.Fprintf(w, "\nUnit #3")
	for _, unit := range unit3Week {
		fmt.Fprintf(w, "\nDay: %v, Signal1:  %v, Signal2: %v, Signal3: %v, Signal4: %v", unit.Id, unit.Signal1, unit.Signal2, unit.Signal3, unit.Signal4)
	}

}

func mainInsertData(w http.ResponseWriter, r *http.Request) {

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		fmt.Fprintf(w, "The error during requests body reading occured %v", err)
		return
	}
	if len(bodyBytes) == 0 {
		fmt.Fprintf(w, "The client sent POST request with an empty body")
		return
	}

	var unit1 Unit

	err = json.Unmarshal(bodyBytes, &unit1)

	if err != nil {
		fmt.Fprintf(w, "The error during body nymarshalling occured %v", err)
		return
	}

	dbConnection, err := establsihDBConn()
	if err != nil {
		fmt.Fprintln(w, "The error duribg body unmarhalling occureg", err)
		return
	}
	defer dbConnection.Close(context.Background())

	if unit1.UnitN == 1 {
		_, err := dbConnection.Exec(context.Background(), "INSERT INTO unit1 (signal1, signal2, signal3) VALUES ($1, $2, $3)", unit1.Signal1, unit1.Signal2, unit1.Signal3)
		fmt.Fprintf(w, "INSERT operations status %v", err)
	}

	if unit1.UnitN == 2 {
		_, err := dbConnection.Exec(context.Background(), "INSERT INTO unit2 (signal1, signal2, signal3) VALUES ($1, $2, $3)", unit1.Signal1, unit1.Signal2, unit1.Signal3)
		fmt.Fprintf(w, "INSERT operations status %v", err)
	}

	if unit1.UnitN == 3 {
		_, err := dbConnection.Exec(context.Background(), "INSERT INTO unit3 (signal1, signal2, signal3, signal4) VALUES ($1, $2, $3, $4 )", unit1.Signal1, unit1.Signal2, unit1.Signal3, unit1.Signal4)
		fmt.Fprintf(w, "INSERT operations status %v", err)
	}

}

func Average(i int, Meaning int, Signal string, w http.ResponseWriter) {
	
	fmt.Println("Level: ", Meaning)
	fmt.Fprintf(w, "\nLevel: %v", Meaning)

	if i <= 4 {
		fmt.Println(Signal, " Code grenn")
		fmt.Fprintf(w, " Code grenn %v", Signal)
		return
	}

	if i > 4 && i <= 7 {
		fmt.Println(Signal, " Code orange")
		fmt.Fprintf(w, " Code orange %v", Signal)
		return
	}

	if i > 7 {
		fmt.Println(Signal, " Code red")
		fmt.Fprintf(w, " CCode red %v", Signal)
		return
	}
}

func statsHandler(w http.ResponseWriter, r *http.Request) {
	dpConnection, err := establsihDBConn()
	if err != nil {
		fmt.Println("Unable to connect to database", err)
		return
	}

	defer dpConnection.Close(context.Background())

	var startUnit1Week []Unit
	var startUnit2Week []Unit
	var startUnit3Week []Unit

	//var unit1 Unit

	rows1, _ := dpConnection.Query(context.Background(), "SELECT * FROM unit1 ORDER BY id ASC")

	for rows1.Next() {
		var unit Unit
		err := rows1.Scan(&unit.Id, &unit.Signal1, &unit.Signal2, &unit.Signal3)

		if err != nil {
			fmt.Println(w, "The error during db fetch accured:/n", err)
			return
		}
		startUnit1Week = append(startUnit1Week, unit)
	}

	fmt.Fprintf(w, "\nUnit #1")
	u1sig1Sum := 0
	u1sig2Sum := 0
	u1sig3Sum := 0

	for _, unit := range startUnit1Week {
		u1sig1Sum += unit.Signal1
		u1sig2Sum += unit.Signal2
		u1sig3Sum += unit.Signal3
	}

	Average(u1sig1Sum/5, u1sig1Sum, "Signal1", w)
	Average(u1sig2Sum/5, u1sig2Sum, "Signal2", w)
	Average(u1sig3Sum/5, u1sig3Sum, "Signal3", w)

	rows2, _ := dpConnection.Query(context.Background(), "SELECT * FROM unit2 ORDER BY id ASC")

	for rows2.Next() {
		var unit Unit
		err := rows2.Scan(&unit.Id, &unit.Signal1, &unit.Signal2, &unit.Signal3)

		if err != nil {
			fmt.Fprintf(w, "The error during db fetch accured:%v/n", err)
			return
		}
		startUnit2Week = append(startUnit2Week, unit)
	}

	fmt.Fprintf(w, "\nUnit #2")
	u2sig1Sum := 0
	u2sig2Sum := 0
	u2sig3Sum := 0

	for _, unit := range startUnit2Week {
		u2sig1Sum += unit.Signal1
		u2sig2Sum += unit.Signal2
		u2sig3Sum += unit.Signal3
	}

	Average(u2sig1Sum/5, u2sig1Sum, "Signal1", w)
	Average(u2sig2Sum/5, u2sig2Sum, "Signal2", w)
	Average(u2sig3Sum/5, u2sig3Sum, "Signal3", w)

	rows3, _ := dpConnection.Query(context.Background(), "SELECT * FROM unit3 ORDER BY id ASC")

	for rows3.Next() {
		var unit Unit
		err := rows3.Scan(&unit.Id, &unit.Signal1, &unit.Signal2, &unit.Signal3, &unit.Signal4)

		if err != nil {
			fmt.Fprintf(w, "The error during db fetch accured:%v/n", err)
			return
		}
		startUnit3Week = append(startUnit3Week, unit)
	}

	fmt.Fprintf(w, "\nUnit #3")
	u3sig1Sum := 0
	u3sig2Sum := 0
	u3sig3Sum := 0
	u3sig4Sum := 0

	for _, unit := range startUnit3Week {
		u3sig1Sum += unit.Signal1
		u3sig2Sum += unit.Signal2
		u3sig3Sum += unit.Signal3
		u3sig4Sum += unit.Signal4
	}

	Average(u3sig1Sum/5, u3sig1Sum, "Signal1", w)
	Average(u3sig2Sum/5, u3sig2Sum, "Signal2", w)
	Average(u3sig3Sum/5, u3sig3Sum, "Signal3", w)
	Average(u3sig4Sum/5, u3sig4Sum, "Signal4", w)
}

func mainHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		mainFetchData(w, r)
	}

	if r.Method == http.MethodPost {
		mainInsertData(w, r)
	}

}

func main() {
	http.HandleFunc("/", mainHandler)
	http.HandleFunc("/run_stats", statsHandler)
	fmt.Println("HTTP server is running on :8080")
	http.ListenAndServe(":8080", nil)
}
