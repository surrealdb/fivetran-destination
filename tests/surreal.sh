#!/bin/bash
set -e

# Check if SurrealDB container is running
if ! docker ps | grep -q surrealdb-test; then
    echo "Error: SurrealDB container (surrealdb-test) is not running"
    echo "Please start the tests first using destination-connector-test.sh"
    exit 1
fi

# Execute surreal sql command within the container
docker exec -it surrealdb-test /surreal sql --user root --pass root --ns test --db test "$@"
