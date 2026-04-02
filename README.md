<p align="center">
  <h1 align="center">DBHub Analytics</h1>
  <p align="center">MCP Server for Databricks, BigQuery, PostgreSQL, MySQL, SQL Server, SQLite, MariaDB</p>
</p>

> Built on top of [DBHub](https://github.com/bytebase/dbhub) by [Bytebase](https://www.bytebase.com/).

```bash
            +------------------+    +--------------+    +------------------+
            |                  |    |              |    |                  |
            |                  |    |              |    |                  |
            |  Claude Desktop  +--->+              +--->+    PostgreSQL    |
            |                  |    |              |    |                  |
            |  Claude Code     +--->+              +--->+    SQL Server    |
            |                  |    |    DBHub     |    |                  |
            |  Cursor          +--->+   Analytics  +--->+    SQLite        |
            |                  |    |              |    |                  |
            |  VS Code         +--->+              +--->+    MySQL         |
            |                  |    |              |    |                  |
            |  Copilot CLI     +--->+              +--->+    MariaDB       |
            |                  |    |              |    |                  |
            |                  |    |              +--->+    Databricks    |
            |                  |    |              |    |                  |
            |                  |    |              +--->+    BigQuery      |
            |                  |    |              |    |   (coming soon)  |
            +------------------+    +--------------+    +------------------+
                 MCP Clients           MCP Server             Databases
```

DBHub Analytics is a zero-dependency, token efficient MCP server implementing the Model Context Protocol (MCP) server interface. This lightweight gateway allows MCP-compatible clients to connect to and explore different databases:

- **Cloud Data Warehouses**: Databricks SQL, Google BigQuery (coming soon) through a unified interface
- **Traditional Databases**: PostgreSQL, MySQL, MariaDB, SQL Server, SQLite
- **Multi-Connection**: Connect to multiple databases simultaneously with TOML configuration
- **Guardrails**: Read-only mode, row limiting, and query timeout to prevent runaway operations
- **Secure Access**: SSH tunneling and SSL/TLS encryption

## Supported Databases

PostgreSQL, MySQL, SQL Server, MariaDB, SQLite, Databricks SQL, and Google BigQuery (coming soon).

## MCP Tools

- **execute_sql**: Execute SQL queries with transaction support and safety controls
- **search_objects**: Search and explore database schemas, tables, columns, indexes, and procedures with progressive disclosure
- **Custom Tools**: Define reusable, parameterized SQL operations in your `dbhub.toml` configuration file

## Installation

See the full [Installation Guide](https://dbhub.ai/installation) for detailed instructions.

### Quick Start

**NPM:**

```bash
npx dbhub-analytics --transport http --port 8080 --dsn "postgres://user:password@localhost:5432/dbname?sslmode=disable"
```

**Docker:**

```bash
docker run --rm --init \
   --name dbhub-analytics \
   --publish 8080:8080 \
   amansingh63/dbhub-analytics \
   --transport http \
   --port 8080 \
   --dsn "postgres://user:password@localhost:5432/dbname?sslmode=disable"
```

**Databricks SQL:**

```bash
npx dbhub-analytics --transport http --port 8080 --dsn "databricks://token:dapi_YOUR_TOKEN@your-workspace.cloud.databricks.com/sql/2.0/warehouses/your_warehouse_id"
```

**Demo Mode:**

```bash
npx dbhub-analytics --transport http --port 8080 --demo
```

See [Command-Line Options](https://dbhub.ai/config/command-line) for all available parameters.

### Multi-Database Setup

Connect to multiple databases simultaneously using TOML configuration files. See [Multi-Database Configuration](https://dbhub.ai/config/toml) for complete setup instructions.

```toml
[[sources]]
id = "databricks_prod"
type = "databricks"
host = "adb-xxx.azuredatabricks.net"
password = "dapi..."
database = "/sql/2.0/warehouses/abc123"

[[sources]]
id = "postgres_prod"
type = "postgres"
host = "localhost"
port = 5432
database = "production"
user = "dbuser"
password = "secret"
```

See [Debug](https://dbhub.ai/config/debug) for troubleshooting tips.

## Development

```bash
# Install dependencies
pnpm install

# Run in development mode
pnpm dev

# Build and run for production
pnpm build && pnpm start --transport stdio --dsn "postgres://user:password@localhost:5432/dbname"
```

## Credits

Built on top of [DBHub](https://github.com/bytebase/dbhub) by [Bytebase](https://www.bytebase.com/), an open-source database DevSecOps platform. Licensed under MIT.
