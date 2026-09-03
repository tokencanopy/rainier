#!/usr/bin/env bash
# check-public-control.sh — guards the public control package.
#
# 1. Import hygiene: the control and controlapp packages, the public wire and
#    plane packages, and their tests may not import any internal/ path,
#    SQL/pgx, Docker, GitHub SDK, cloud SDK, billing package, or
#    provider-named package. HTTP and WebSocket are refused too, except for
#    the packages whose whole subject is an HTTP/WebSocket surface (see
#    http_ok below).
# 2. Duplicate-model inventory: exported identifiers that also exist in
#    internal/controld are the definitions the extraction lanes will migrate.
#    They are ALLOWED only while named in the allowlist below (the exact
#    interface-freeze inventory); any overlap outside it is a new duplicate
#    and fails the build.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

test -d control
test -f control/doc.go
go list ./control >/dev/null

test -d controlapp
test -f controlapp/doc.go
go list ./controlapp >/dev/null

test -d controlapp/repotest
test -f controlapp/repotest/doc.go
go list ./controlapp/repotest >/dev/null

test -d v0wire
test -f v0wire/doc.go
go list ./v0wire >/dev/null

# ---------------------------------------------------------------------------
# 1. import hygiene
# ---------------------------------------------------------------------------
# The same table is applied to every public application package — including
# controlapp/repotest, the repository contract suite a hosted store must pass,
# which is public precisely so a host outside this repository can run it, and
# v0wire, the /v0/ JSON wire a hosted cell serves from its own router. The
# failure message names which package pulled the forbidden import in.
bad_imports=0
for pkg in control controlapp controlapp/repotest v0wire; do
  # The application contract is transport-neutral and must stay that way. The
  # wire and plane packages are the exception the ADR names: their whole
  # subject is an HTTP surface, so net/http is theirs to import (v0wire renders
  # responses and decodes bodies) — and only net/http. Every other line of the
  # table below applies to them unchanged.
  http_ok=0
  case "$pkg" in
    v0wire) http_ok=1 ;;
  esac
  while IFS= read -r imp; do
    [[ -z "$imp" ]] && continue
    lower="$(printf '%s' "$imp" | tr '[:upper:]' '[:lower:]')"
    case "$lower" in
      *rainier/internal/*)   reason="internal/ path" ;;
      *net/http*)
        if [[ "$http_ok" == "1" ]]; then
          continue
        fi
        reason="HTTP server" ;;
      *coder/websocket*)     reason="WebSocket" ;;
      *database/sql*)        reason="SQL" ;;
      *jackc/pgx*)           reason="pgx" ;;
      *docker*)              reason="Docker" ;;
      *go-github*)           reason="GitHub SDK" ;;
      *cloud.google.com*)    reason="GCP SDK" ;;
      *github.com/aws*)      reason="AWS SDK" ;;
      *github.com/azure*)    reason="Azure SDK" ;;
      *github.com/oracle*)   reason="OCI SDK" ;;
      *stripe*)              reason="billing" ;;
      *billing*)             reason="billing" ;;
      *hetzner*)             reason="provider package" ;;
      *netcup*)              reason="provider package" ;;
      *)                     continue ;;
    esac
    echo "$pkg imports a forbidden package ($reason): $imp" >&2
    bad_imports=1
  done < <(go list -f '{{join .Imports "\n"}} {{join .TestImports "\n"}} {{join .XTestImports "\n"}}' "./$pkg")
done

if [[ "$bad_imports" == "1" ]]; then
  exit 1
fi

# ---------------------------------------------------------------------------
# 2. duplicate-model inventory against internal/controld (./control only)
# ---------------------------------------------------------------------------
# The exact interface-freeze allowlist: every public control model that still
# has a private twin in internal/controld, to be removed by the extraction
# lanes. Nothing else may appear in both packages.
allowlist=()

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

cat > "$tmpdir/listexports.go" <<'EOF'
package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func main() {
	for _, dir := range os.Args[1:] {
		names := map[string]bool{}
		files, _ := filepath.Glob(filepath.Join(dir, "*.go"))
		fset := token.NewFileSet()
		for _, file := range files {
			if strings.HasSuffix(file, "_test.go") {
				continue
			}
			f, err := parser.ParseFile(fset, file, nil, 0)
			if err != nil {
				continue
			}
			for _, d := range f.Decls {
				switch x := d.(type) {
				case *ast.GenDecl:
					if x.Tok == token.CONST || x.Tok == token.VAR || x.Tok == token.TYPE {
						for _, s := range x.Specs {
							switch sp := s.(type) {
							case *ast.ValueSpec:
								for _, n := range sp.Names {
									if ast.IsExported(n.Name) {
										names[n.Name] = true
									}
								}
							case *ast.TypeSpec:
								if ast.IsExported(sp.Name.Name) {
									names[sp.Name.Name] = true
								}
							}
						}
					}
				case *ast.FuncDecl:
					if x.Recv == nil && ast.IsExported(x.Name.Name) {
						names[x.Name.Name] = true
					}
				}
			}
		}
		var out []string
		for n := range names {
			out = append(out, n)
		}
		sort.Strings(out)
		for _, n := range out {
			fmt.Println(n)
		}
	}
}
EOF

go run "$tmpdir/listexports.go" ./control > "$tmpdir/control.txt"
go run "$tmpdir/listexports.go" ./internal/controld > "$tmpdir/controld.txt"

# Every public model with a private twin must be in the allowlist; anything
# else that appears in both packages is a new duplicate and fails.
new_duplicate=0
while IFS= read -r name; do
  if ! grep -Fxq "$name" "$tmpdir/controld.txt"; then
    continue
  fi
  allowed=0
  for a in ${allowlist[@]+"${allowlist[@]}"}; do
    [[ "$name" == "$a" ]] && allowed=1
  done
  if [[ "$allowed" == "1" ]]; then
    echo "migration inventory: $name (public control twin of internal/controld)"
  else
    echo "new duplicate not in the freeze allowlist: $name" >&2
    new_duplicate=1
  fi
done < "$tmpdir/control.txt"

if [[ "$new_duplicate" == "1" ]]; then
  exit 1
fi
