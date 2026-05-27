// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package apicheck

import (
	"os"
	"strings"
	"testing"
)

func TestPublicAPIInventoryMatchesSnapshots(t *testing.T) {
	if os.Getenv("UPDATE_API_SNAPSHOTS") == "1" {
		t.Skip("snapshot update mode")
	}
	for _, pkg := range PublicPackages {
		pkg := pkg
		t.Run(pkg, func(t *testing.T) {
			got, err := ListExports(pkg)
			if err != nil {
				t.Fatal(err)
			}
			path, err := TestdataPath(SnapshotFileName(pkg))
			if err != nil {
				t.Fatal(err)
			}
			want, err := ReadSnapshot(path)
			if err != nil {
				t.Fatalf("read snapshot %s: %v (run UPDATE_API_SNAPSHOTS=1 go test ./internal/apicheck/...)", path, err)
			}
			if diff := diffLines(want, got); diff != "" {
				t.Fatalf("export list drift for %s:\n%s\nUpdate: UPDATE_API_SNAPSHOTS=1 go test ./internal/apicheck/... -run TestUpdatePublicAPISnapshots", pkg, diff)
			}
		})
	}
}

func TestUpdatePublicAPISnapshots(t *testing.T) {
	if os.Getenv("UPDATE_API_SNAPSHOTS") != "1" {
		t.Skip("set UPDATE_API_SNAPSHOTS=1 to regenerate testdata")
	}
	for _, pkg := range PublicPackages {
		pkg := pkg
		t.Run(pkg, func(t *testing.T) {
			names, err := ListExports(pkg)
			if err != nil {
				t.Fatal(err)
			}
			path, err := TestdataPath(SnapshotFileName(pkg))
			if err != nil {
				t.Fatal(err)
			}
			if err := WriteSnapshot(path, names); err != nil {
				t.Fatal(err)
			}
			t.Logf("wrote %d symbols to %s", len(names), path)
		})
	}
}

func diffLines(want, got []string) string {
	wm := make(map[string]int, len(want))
	for _, w := range want {
		wm[w]++
	}
	gm := make(map[string]int, len(got))
	for _, g := range got {
		gm[g]++
	}
	var added, removed []string
	for g, n := range gm {
		if wm[g] < n {
			for i := 0; i < n-wm[g]; i++ {
				added = append(added, g)
			}
		}
	}
	for w, n := range wm {
		if gm[w] < n {
			for i := 0; i < n-gm[w]; i++ {
				removed = append(removed, w)
			}
		}
	}
	sortStrings(added)
	sortStrings(removed)
	if len(added) == 0 && len(removed) == 0 {
		return ""
	}
	var b strings.Builder
	if len(removed) > 0 {
		b.WriteString("removed:\n  - ")
		b.WriteString(strings.Join(removed, "\n  - "))
		b.WriteByte('\n')
	}
	if len(added) > 0 {
		b.WriteString("added:\n  - ")
		b.WriteString(strings.Join(added, "\n  - "))
		b.WriteByte('\n')
	}
	return b.String()
}

func sortStrings(s []string) {
	for i := 0; i < len(s); i++ {
		for j := i + 1; j < len(s); j++ {
			if s[j] < s[i] {
				s[i], s[j] = s[j], s[i]
			}
		}
	}
}
