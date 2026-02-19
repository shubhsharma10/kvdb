package main

import (
	"flag"
	"fmt"
	kvdb "kvdb/src"
	"log"
	"strings"
)

func main() {
	cmdPtr := flag.String("cmd", "", "Command to execute")
	keyPtr := flag.String("key", "", "Key to execute")
	valuePtr := flag.String("value", "", "Value to execute")

	flag.Parse()
	cmdString := strings.ToUpper(*cmdPtr)
	keyString := *keyPtr
	valueString := *valuePtr
	fileName := "logFile.txt"
	var observedValue string
	db, err := kvdb.NewDB(fileName)

	if err != nil {
		fmt.Printf("Error opening database: %v\n", err)
		log.Fatal(err)
	}

	if strings.Contains(keyString, kvdb.DELIMITER) || strings.Contains(valueString, kvdb.DELIMITER) {
		fmt.Println("'|' is not allowed in Key/Value")
		return
	}

	defer db.Close()

	switch cmdString {
	case kvdb.SET_COMMAND:

		err = db.Put(keyString, valueString)

	case kvdb.GET_COMMAND:

		observedValue, err = db.Get(keyString)
	case kvdb.DELETE_COMMAND:

		err = db.Delete(keyString)
	default:

		fmt.Printf("Unknown command: %s\n", *cmdPtr)
	}

	if err != nil {
		fmt.Printf("Error executing command %v, error: %v\n", cmdString, err)
	} else {
		fmt.Printf("Command executed successfully: %s,key:%s\n", cmdString, keyString)
		if cmdString == kvdb.GET_COMMAND {
			fmt.Printf("Observed value: %s\n", observedValue)
		}
	}
}
