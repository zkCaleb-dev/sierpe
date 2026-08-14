package store

import (
	"sort"
	"strings"
	"testing"
)

// Migrations are applied in filename order forever; this test locks the
// naming discipline in before the list grows.
func TestMigrationNames(t *testing.T) {
	names, err := MigrationNames()
	if err != nil {
		t.Fatalf("MigrationNames() error = %v", err)
	}
	if len(names) == 0 {
		t.Fatal("no embedded migrations found")
	}
	if !sort.StringsAreSorted(names) {
		t.Errorf("migrations not in sorted order: %v", names)
	}
	seen := map[string]bool{}
	for _, n := range names {
		if !strings.HasSuffix(n, ".sql") {
			t.Errorf("migration %q must end in .sql", n)
		}
		prefix := strings.SplitN(n, "_", 2)[0]
		if len(prefix) != 4 {
			t.Errorf("migration %q must start with a 4-digit ordinal", n)
		}
		if seen[prefix] {
			t.Errorf("duplicate migration ordinal %q", prefix)
		}
		seen[prefix] = true
	}
}
