// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

// Package apicheck provides lightweight guards against accidental public API growth.
package apicheck

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

// PublicPackages are user-facing import paths reviewed for KL-1801.
var PublicPackages = []string{
	"github.com/haluan/go-keylane",
	"github.com/haluan/go-keylane/httpkeylane",
	"github.com/haluan/go-keylane/metrics/prometheus",
	"github.com/haluan/go-keylane/tracing/otel",
}

// ListExports returns sorted exported symbol names for an import path (types, funcs, consts, vars).
// Method names are included as Type.Method when declared on exported types.
func ListExports(importPath string) ([]string, error) {
	dir, err := packageDir(importPath)
	if err != nil {
		return nil, err
	}
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, dir, func(fi os.FileInfo) bool {
		name := fi.Name()
		return !strings.HasSuffix(name, "_test.go") && strings.HasSuffix(name, ".go")
	}, parser.ParseComments)
	if err != nil {
		return nil, err
	}
	if len(pkgs) == 0 {
		return nil, fmt.Errorf("no packages in %s", dir)
	}
	var pkg *ast.Package
	for _, p := range pkgs {
		pkg = p
		break
	}
	names := make(map[string]struct{})
	for _, f := range pkg.Files {
		collectExports(f, names)
	}
	out := make([]string, 0, len(names))
	for n := range names {
		out = append(out, n)
	}
	sort.Strings(out)
	return out, nil
}

const rootModulePath = "github.com/haluan/go-keylane"

func packageDir(importPath string) (string, error) {
	root, err := repoRoot()
	if err != nil {
		return "", fmt.Errorf("finding repo root: %w", err)
	}
	cmd := exec.Command("go", "list", "-f", "{{.Dir}}", importPath)
	cmd.Dir = moduleRootForPackage(root, importPath)
	out, err := cmd.Output()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			return "", fmt.Errorf("go list %s: %w: %s", importPath, err, string(ee.Stderr))
		}
		return "", fmt.Errorf("go list %s: %w", importPath, err)
	}
	return strings.TrimSpace(string(out)), nil
}

func repoRoot() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	dir := wd
	for i := 0; i < 8; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		dir = filepath.Dir(dir)
	}
	return wd, nil
}

func moduleRootForPackage(root, importPath string) string {
	if importPath == rootModulePath {
		return root
	}
	rel := filepath.FromSlash(strings.TrimPrefix(importPath, rootModulePath+"/"))
	dir := filepath.Join(root, rel)
	for dir != root {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		dir = filepath.Dir(dir)
	}
	return root
}

func collectExports(f *ast.File, names map[string]struct{}) {
	for _, decl := range f.Decls {
		switch d := decl.(type) {
		case *ast.GenDecl:
			if d.Tok != token.TYPE && d.Tok != token.CONST && d.Tok != token.VAR {
				continue
			}
			for _, spec := range d.Specs {
				switch s := spec.(type) {
				case *ast.TypeSpec:
					if s.Name != nil && s.Name.IsExported() {
						names[s.Name.Name] = struct{}{}
					}
				case *ast.ValueSpec:
					for _, id := range s.Names {
						if id.IsExported() {
							names[id.Name] = struct{}{}
						}
					}
				}
			}
		case *ast.FuncDecl:
			if d.Name == nil || !d.Name.IsExported() {
				continue
			}
			if d.Recv == nil {
				names[d.Name.Name] = struct{}{}
				continue
			}
			if len(d.Recv.List) > 0 {
				recv := d.Recv.List[0].Type
				typeName := receiverTypeName(recv)
				if typeName != "" && ast.IsExported(typeName) {
					names[typeName+"."+d.Name.Name] = struct{}{}
				}
			}
		}
	}
}

func receiverTypeName(expr ast.Expr) string {
	switch t := expr.(type) {
	case *ast.StarExpr:
		return receiverTypeName(t.X)
	case *ast.Ident:
		return t.Name
	default:
		return ""
	}
}

// SnapshotFileName maps an import path to a testdata file name.
func SnapshotFileName(importPath string) string {
	safe := strings.TrimPrefix(importPath, "github.com/haluan/go-keylane")
	safe = strings.TrimPrefix(safe, "/")
	if safe == "" {
		return "exports_keylane.txt"
	}
	return "exports_" + strings.ReplaceAll(safe, "/", "_") + ".txt"
}

// WriteSnapshot writes export names to path (one per line).
func WriteSnapshot(path string, names []string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	var b strings.Builder
	for _, n := range names {
		b.WriteString(n)
		b.WriteByte('\n')
	}
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

// ReadSnapshot reads export names from path.
func ReadSnapshot(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) == 1 && lines[0] == "" {
		return nil, nil
	}
	return lines, nil
}

// TestdataPath resolves testdata/<file> relative to repo root from cwd.
func TestdataPath(file string) (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	// Walk up to find go.mod
	dir := wd
	for i := 0; i < 6; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return filepath.Join(dir, "internal", "apicheck", "testdata", file), nil
		}
		dir = filepath.Dir(dir)
	}
	return filepath.Join(wd, "testdata", file), nil
}
