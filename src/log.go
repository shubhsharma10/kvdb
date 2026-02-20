package kvdb

import (
	"fmt"
	"os"
	"strings"
)

type Log struct {
	file *os.File
	path string
}

func NewLog(filePath string) (*Log, error) {
	logFile, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %v", err)
	}

	log := &Log{
		file: logFile,
		path: filePath,
	}
	return log, nil
}

func (log *Log) Append(e Entry) error {
	var valueToWrite string
	if e.Command == DELETE_COMMAND {
		valueToWrite = fmt.Sprintf("%s%s%s\n", e.Command, DELIMITER, e.Key)
	} else {
		valueToWrite = fmt.Sprintf("%s%s%s%s%s\n", e.Command, DELIMITER, e.Key, DELIMITER, e.Value)
	}
	valueInBytes := []byte(valueToWrite)
	if _, err := log.file.Write(valueInBytes); err != nil {
		return fmt.Errorf("error writing to file: %v \n", err)
	}
	return nil
}

func (log *Log) ReadAll() ([]Entry, error) {
	var entries []Entry
	content, err := os.ReadFile(log.path)
	if err != nil {
		return entries, fmt.Errorf("error reading file: %v \n", err)
	}
	text := string(content)
	lines := strings.Split(text, "\n")
	for _, line := range lines {
		parsedLine := strings.Split(line, DELIMITER)
		if len(parsedLine) < 2 {
			continue
		}
		cmd := parsedLine[0]
		if cmd != DELETE_COMMAND && cmd != SET_COMMAND && cmd != GET_COMMAND {
			continue
		}
		key := parsedLine[1]
		value := ""
		if len(parsedLine) == 3 {
			value = parsedLine[2]
		}
		entry := Entry{
			Command: cmd,
			Key:     key,
			Value:   value,
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func (log *Log) Close() error {
	return log.file.Close()
}
