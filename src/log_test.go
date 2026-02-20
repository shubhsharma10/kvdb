package kvdb

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func newTestLog(t *testing.T) *Log {
	t.Helper()
	l, err := NewLog(filepath.Join(t.TempDir(), "testFile.txt"))
	if err != nil {
		t.Fatalf("NewLog: unexpected error: %v", err)
	}

	t.Cleanup(func() { l.Close() })
	return l
}

// TestNewLog_CreatesFile
// Checks whether log file is created or not
func TestNewLog_CreatesFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "testFile.txt")
	l, err := NewLog(path)
	if err != nil {
		t.Fatalf("NewLog(%q): unexpected error: %v", path, err)
	}

	defer l.Close()
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Errorf("expected file %q to exist but it doesn't.", path)
	}
}

// TestLog_AppendAndReadSET
// verifies correctness of SET operation
func TestLog_AppendAndReadSET(t *testing.T) {
	l := newTestLog(t)

	e := Entry{Command: SET_COMMAND, Key: "user1", Value: "raj"}
	if err := l.Append(e); err != nil {
		t.Fatalf("Append: unexpected error: %v", err)
	}
	readEntries, err := l.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: unexpected error: %v", err)
	}

	if len(readEntries) != 1 {
		t.Errorf("ReadAll: expected %d entries, got %d", 1, len(readEntries))
	}

	if readEntries[0].Command != e.Command || readEntries[0].Key != e.Key || readEntries[0].Value != e.Value {
		t.Errorf("ReadAll: expected %+v, got %+v", e, readEntries[0])
	}
}

// TestLog_AppendAndReadDELETE
// verifies correctness of DELETE operation
func TestLog_AppendAndReadDELETE(t *testing.T) {
	l := newTestLog(t)

	e := Entry{Command: DELETE_COMMAND, Key: "user1"}
	if err := l.Append(e); err != nil {
		t.Fatalf("Append: unexpected error: %v", err)
	}
	readEntries, err := l.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: unexpected error: %v", err)
	}

	if len(readEntries) != 1 {
		t.Errorf("ReadAll: expected %d entries, got %d", 1, len(readEntries))
	}

	if readEntries[0].Command != e.Command || readEntries[0].Key != e.Key || readEntries[0].Value != e.Value {
		t.Errorf("ReadAll: expected %+v, got %+v", e, readEntries[0])
	}
}

// TestLog_AppendMultipleEntries
// verifies order of multiple operation
func TestLog_AppendMultipleEntries(t *testing.T) {
	l := newTestLog(t)

	e1 := Entry{Command: SET_COMMAND, Key: "user1", Value: "raj"}
	e2 := Entry{Command: GET_COMMAND, Key: "user1"}
	e3 := Entry{Command: SET_COMMAND, Key: "user2", Value: "manoj"}
	e4 := Entry{Command: DELETE_COMMAND, Key: "user1"}
	e5 := Entry{Command: GET_COMMAND, Key: "user1"}
	op := []Entry{e1, e2, e3, e4, e5}
	for _, e := range op {
		if err := l.Append(e); err != nil {
			t.Fatalf("Append: unexpected error: %v", err)
		}
	}
	readEntries, err := l.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: unexpected error: %v", err)
	}

	if len(readEntries) != len(op) {
		t.Errorf("ReadAll: expected %d entries, got %d", len(op), len(readEntries))
	}

	for i, entry := range op {
		if readEntries[i].Command != entry.Command || readEntries[i].Key != entry.Key || readEntries[i].Value != entry.Value {
			t.Errorf("ReadAll: expected %+v, got %+v", entry, readEntries[i])
		}
	}
}

// TestLog_ReadAll_EmptyFile
// verifies empty file
func TestLog_ReadAll_EmptyFile(t *testing.T) {
	l := newTestLog(t)

	readEntries, err := l.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: unexpected error: %v", err)
	}

	if len(readEntries) != 0 {
		t.Errorf("ReadAll: expected %d entries, got %d", 0, len(readEntries))
	}
}

// TestLog_ReadAll_MalformedLines
// Checks for bad lines while reading
func TestLog_ReadAll_MalformedLines(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "testFile.txt")
	l, err := NewLog(path)
	if err != nil {
		t.Fatalf("NewLog(%q): unexpected error: %v", path, err)
	}
	defer l.Close()

	// Write one valid entry
	e := Entry{Command: SET_COMMAND, Key: "user1", Value: "raj"}
	if err := l.Append(e); err != nil {
		t.Fatalf("Append: unexpected error: %v", err)
	}

	// Manually inject malformed lines into the file
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("OpenFile: unexpected error: %v", err)
	}
	f.WriteString("badline-no-delimiter\n")
	f.WriteString("ONLY_COMMAND\n")
	f.WriteString("\n")
	f.Close()

	readEntries, err := l.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: unexpected error: %v", err)
	}

	if len(readEntries) != 1 {
		t.Errorf("ReadAll: expected 1 valid entry, got %d", len(readEntries))
	}

	if readEntries[0].Command != e.Command || readEntries[0].Key != e.Key || readEntries[0].Value != e.Value {
		t.Errorf("ReadAll: expected %+v, got %+v", e, readEntries[0])
	}
}

// TestLog_PersistenceAcrossInstances
// Checks persistence of log file
func TestLog_PersistenceAcrossInstances(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "testFile.txt")

	l1, err := NewLog(path)
	if err != nil {
		t.Fatalf("NewLog(%q): unexpected error: %v", path, err)
	}

	entries := []Entry{
		{Command: SET_COMMAND, Key: "user1", Value: "raj"},
		{Command: SET_COMMAND, Key: "user2", Value: "manoj"},
		{Command: DELETE_COMMAND, Key: "user1"},
	}
	for _, e := range entries {
		if err := l1.Append(e); err != nil {
			t.Fatalf("Append: unexpected error: %v", err)
		}
	}
	l1.Close()

	l2, err := NewLog(path)
	if err != nil {
		t.Fatalf("NewLog(%q) reopen: unexpected error: %v", path, err)
	}
	defer l2.Close()

	readEntries, err := l2.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: unexpected error: %v", err)
	}

	if len(readEntries) != len(entries) {
		t.Fatalf("ReadAll: expected %d entries, got %d", len(entries), len(readEntries))
	}

	for i, e := range entries {
		if readEntries[i].Command != e.Command || readEntries[i].Key != e.Key || readEntries[i].Value != e.Value {
			t.Errorf("ReadAll[%d]: expected %+v, got %+v", i, e, readEntries[i])
		}
	}
}

// TestLog_AppendLargeValue
// Checks for large value on SET
func TestLog_AppendLargeValue(t *testing.T) {
	l := newTestLog(t)

	largeValue := strings.Repeat("a", 10*1024)
	e := Entry{Command: SET_COMMAND, Key: "bigkey", Value: largeValue}
	if err := l.Append(e); err != nil {
		t.Fatalf("Append: unexpected error: %v", err)
	}

	readEntries, err := l.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: unexpected error: %v", err)
	}

	if len(readEntries) != 1 {
		t.Fatalf("ReadAll: expected 1 entry, got %d", len(readEntries))
	}

	if readEntries[0].Value != largeValue {
		t.Errorf("ReadAll: value length expected %d, got %d", len(largeValue), len(readEntries[0].Value))
	}
}

func BenchmarkLog_Append(b *testing.B) {
	path := filepath.Join(b.TempDir(), "benchmarkFile.txt")
	l, err := NewLog(path)
	if err != nil {
		b.Fatalf("NewLog(%q): unexpected error: %v", path, err)
	}
	defer l.Close()

	e := Entry{Command: SET_COMMAND, Key: "user1", Value: "raj"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := l.Append(e); err != nil {
			b.Fatalf("Append: unexpected error: %v", err)
		}
	}
}

func benchmarkReadAll(b *testing.B, n int) {
	b.Helper()
	path := filepath.Join(b.TempDir(), "benchmarkFile.txt")
	l, err := NewLog(path)
	if err != nil {
		b.Fatalf("NewLog(%q): unexpected error: %v", path, err)
	}
	defer l.Close()
	e := Entry{Command: SET_COMMAND, Key: "user1", Value: "raj"}
	for i := 0; i < n; i++ {
		if err := l.Append(e); err != nil {
			b.Fatalf("Append: unexpected error: %v", err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := l.ReadAll(); err != nil {
			b.Fatalf("ReadAll: unexpected error: %v", err)
		}
	}
}

func BenchmarkLog_ReadAll_10(b *testing.B) {
	benchmarkReadAll(b, 10)
}

func BenchmarkLog_ReadAll_1000(b *testing.B) {
	benchmarkReadAll(b, 1000)
}

func BenchmarkLog_ReadAll_10000(b *testing.B) {
	benchmarkReadAll(b, 10000)
}
