#!/usr/bin/env bash
set -euo pipefail

DATABASE_URL="${DATABASE_URL:-postgresql://membermatch:membermatch@localhost:5432/membermatch}"

echo "Applying migrations from sql/migrations/"
for f in sql/migrations/V*.sql; do
    echo "  -> $f"
    psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f "$f"
done
echo "Done."
