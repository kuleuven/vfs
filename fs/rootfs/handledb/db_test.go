package handledb

import "testing"

func TestDB(t *testing.T) {
	db, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	err = db.Put([]byte{1}, "test")
	if err != nil {
		t.Fatal(err)
	}

	path, err := db.Get([]byte{1})
	if err != nil {
		t.Fatal(err)
	}

	if path != "test" {
		t.Errorf("Expected path 'test', got '%s'", path)
	}

	handle, err := db.Generate("test2")
	if err != nil {
		t.Fatal(err)
	}

	path, err = db.Get(handle)
	if err != nil {
		t.Fatal(err)
	}

	if path != "test2" {
		t.Errorf("Expected path 'test2', got '%s'", path)
	}
}
