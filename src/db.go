package kvdb

type DB struct {
	log *Log
}

func NewDB(filePath string) (*DB, error) {
	log, err := NewLog(filePath)
	if err != nil {
		return nil, err
	}

	db := &DB{log: log}
	return db, nil
}

func (db *DB) Put(key string, value string) error {
	entry := Entry{Command: SET_COMMAND, Key: key, Value: value}
	if err := db.log.Append(entry); err != nil {
		return err
	}
	return nil
}

func (db *DB) Get(key string) (string, error) {
	var observedValue string
	allEntries, err := db.log.ReadAll()
	if err != nil {
		return observedValue, err
	}
	for _, entry := range allEntries {
		if entry.Key == key {
			if entry.Command == SET_COMMAND {
				observedValue = entry.Value
			} else if entry.Command == DELETE_COMMAND {
				observedValue = ""
			}
		}
	}
	return observedValue, nil
}

func (db *DB) Delete(key string) error {
	var entry Entry
	entry.Command = DELETE_COMMAND
	entry.Key = key
	if err := db.log.Append(entry); err != nil {
		return err
	}
	return nil
}

func (db *DB) Close() {
	db.log.Close()
}
