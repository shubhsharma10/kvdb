package kvdb

import (
	"fmt"
	"path/filepath"
	"testing"
)

func newDB(t *testing.T) *DB {
	t.Helper()
	db, err := NewDB(filepath.Join(t.TempDir(), "dbtest.txt"))
	if err != nil {
		t.Fatalf("NewDB: unexpected error: %v", err)
	}

	t.Cleanup(func() { db.Close() })
	return db
}

func TestDB_PutAndGet(t *testing.T) {
	db := newDB(t)
	key := "user1"
	value := "raj"
	if err := db.Put(key, value); err != nil {
		t.Fatalf("Error calling PUT %v", err)
	}
	readValue, err := db.Get(key)
	if err != nil {
		t.Fatalf("Error calling GET %v", err)
	}
	if readValue != value {
		t.Errorf("Error calling GET expected %v, got %v", value, readValue)
	}
}

func TestDB_Get_NonexistentKey(t *testing.T) {
	db := newDB(t)
	key := "user1"
	readValue, err := db.Get(key)
	if err != nil {
		t.Fatalf("Error calling GET %v", err)
	}
	if readValue != "" {
		t.Errorf("Error calling GET expected empty, got %v", readValue)
	}
}

func TestDB_Put_Overwrite(t *testing.T) {
	db := newDB(t)
	key := "user1"
	value1 := "raj"
	value2 := "manoj"
	value3 := "ravi"

	if err := db.Put(key, value1); err != nil {
		t.Fatalf("Error calling PUT %v", err)
	}
	if err := db.Put(key, value2); err != nil {
		t.Fatalf("Error calling PUT %v", err)
	}
	if err := db.Put(key, value3); err != nil {
		t.Fatalf("Error calling PUT %v", err)
	}
	readValue, err := db.Get(key)
	if err != nil {
		t.Fatalf("Error calling GET %v", err)
	}
	if readValue != value3 {
		t.Errorf("Error calling GET expected %v, got %v", value3, readValue)
	}
}

func TestDB_Delete_ExistingKey(t *testing.T) {
	db := newDB(t)
	key := "user1"
	value1 := "raj"

	if err := db.Put(key, value1); err != nil {
		t.Fatalf("Error calling PUT %v", err)
	}
	if err := db.Delete(key); err != nil {
		t.Fatalf("Error calling DEL %v", err)
	}
	readValue, err := db.Get(key)
	if err != nil {
		t.Fatalf("Error calling GET %v", err)
	}
	if readValue != "" {
		t.Errorf("Error calling GET expected empty, got %v", readValue)
	}
}

func TestDB_Delete_ThenPut(t *testing.T) {
	db := newDB(t)
	key := "user1"
	value1 := "raj"
	value2 := "manoj"

	if err := db.Put(key, value1); err != nil {
		t.Fatalf("Error calling PUT %v", err)
	}
	if err := db.Delete(key); err != nil {
		t.Fatalf("Error calling DEL %v", err)
	}
	if err := db.Put(key, value2); err != nil {
		t.Fatalf("Error calling PUT %v", err)
	}
	readValue, err := db.Get(key)
	if err != nil {
		t.Fatalf("Error calling GET %v", err)
	}
	if readValue != value2 {
		t.Errorf("Error calling GET expected %v, got %v", value2, readValue)
	}
}

func TestDB_MultipleKeys(t *testing.T) {
	db := newDB(t)
	key1 := "user1"
	value1 := "raj"
	key2 := "user2"
	value2 := "manoj"
	key3 := "user3"
	value3 := "ravi"

	if err := db.Put(key1, value1); err != nil {
		t.Fatalf("Error calling PUT %v", err)
	}
	if err := db.Put(key2, value2); err != nil {
		t.Fatalf("Error calling PUT %v", err)
	}
	if err := db.Put(key3, value3); err != nil {
		t.Fatalf("Error calling PUT %v", err)
	}
	readValue2, err := db.Get(key2)
	if err != nil {
		t.Fatalf("Error calling GET %v", err)
	}
	if readValue2 != value2 {
		t.Errorf("Error calling GET expected %v, got %v", value2, readValue2)
	}

	readValue1, err := db.Get(key1)
	if err != nil {
		t.Fatalf("Error calling GET %v", err)
	}
	if readValue1 != value1 {
		t.Errorf("Error calling GET expected %v, got %v", value1, readValue1)
	}

	readValue3, err := db.Get(key3)
	if err != nil {
		t.Fatalf("Error calling GET %v", err)
	}
	if readValue3 != value3 {
		t.Errorf("Error calling GET expected %v, got %v", value3, readValue3)
	}
}

func TestDB_EmptyKey(t *testing.T) {
	db := newDB(t)
	key := ""
	value := "raj"

	if err := db.Put(key, value); err != nil {
		t.Fatalf("Error calling PUT %v", err)
	}

	readValue, err := db.Get(key)
	if err != nil {
		t.Fatalf("Error calling GET %v", err)
	}
	if readValue != value {
		t.Errorf("Error calling GET expected %v, got %v", value, readValue)
	}
}

func TestDB_EmptyValue(t *testing.T) {
	db := newDB(t)
	key := "user1"
	value := ""

	if err := db.Put(key, value); err != nil {
		t.Fatalf("Error calling PUT %v", err)
	}

	readValue, err := db.Get(key)
	if err != nil {
		t.Fatalf("Error calling GET %v", err)
	}
	if readValue != value {
		t.Errorf("Error calling GET expected %v, got %v", value, readValue)
	}
}

func TestDB_PersistanceAcrossInstances(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dbtest.txt")
	db1, err := NewDB(path)
	if err != nil {
		t.Fatalf("NewDB: unexpected error: %v", err)
	}

	key := "user1"
	value := "raj"

	if err := db1.Put(key, value); err != nil {
		t.Fatalf("Error calling PUT %v", err)
	}

	db1.Close()

	db2, err := NewDB(path)
	defer db2.Close()
	if err != nil {
		t.Fatalf("NewDB: unexpected error: %v", err)
	}
	readValue, err := db2.Get(key)
	if err != nil {
		t.Fatalf("Error calling GET %v", err)
	}
	if readValue != value {
		t.Errorf("Error calling GET expected %v, got %v", value, readValue)
	}
}

func BenchmarkDB_Put(b *testing.B) {
	path := filepath.Join(b.TempDir(), "benchtest.txt")
	db, err := NewDB(path)
	if err != nil {
		b.Fatalf("NewDB: unexpected error: %v", err)
	}
	defer db.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Put("user1", "raj"); err != nil {
			b.Fatalf("Error calling PUT %v", err)
		}
	}
}

func benchmarkDBGet(b *testing.B, n int) {
	b.Helper()
	path := filepath.Join(b.TempDir(), "benchtest.txt")
	db, err := NewDB(path)
	if err != nil {
		b.Fatalf("NewDB: unexpected error: %v", err)
	}
	defer db.Close()
	for i := 0; i < n; i++ {
		if err := db.Put(fmt.Sprintf("user%d", i), fmt.Sprintf("value%d", i)); err != nil {
			b.Fatalf("Error calling PUT %v", err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Get(fmt.Sprintf("user%d", i%n)); err != nil {
			b.Fatalf("Error calling GET %v", err)
		}
	}
}

func BenchmarkDB_Get_10(b *testing.B)    { benchmarkDBGet(b, 10) }
func BenchmarkDB_Get_1000(b *testing.B)  { benchmarkDBGet(b, 1000) }
func BenchmarkDB_Get_10000(b *testing.B) { benchmarkDBGet(b, 10000) }
