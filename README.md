<p align="center">
  <h1 align="center">DBHub Analytics</h1>
  <p align="center">Universal Database MCP Server for Databricks, Google BigQuery, PostgreSQL, MySQL, SQL Server, SQLite, MariaDB</p>
</p>

[![npm version](https://img.shields.io/npm/v/dbhub-analytics)](https://www.npmjs.com/package/dbhub-analytics)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> Built on top of [DBHub](https://github.com/bytebase/dbhub) by [Bytebase](https://www.bytebase.com/).

## What is DBHub Analytics?

DBHub Analytics is an open-source [MCP (Model Context Protocol)](https://modelcontextprotocol.io/) server that connects AI assistants like **Claude Desktop**, **Claude Code**, **Cursor**, **VS Code Copilot**, and other MCP-compatible clients to your databases. Query Databricks SQL warehouses, Google BigQuery datasets, PostgreSQL, MySQL, and more — all through a single unified interface.

```
            +------------------+    +--------------+    +------------------+
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
            |                  |    |              |    |                  |
            +------------------+    +--------------+    +------------------+
                 MCP Clients           MCP Server             Databases
```

## Key Features

- **Cloud Data Warehouses**: Connect to Databricks SQL and Google BigQuery for analytics workloads
- **Traditional Databases**: Full support for PostgreSQL, MySQL, MariaDB, SQL Server, SQLite
- **Multi-Database**: Connect to multiple databases simultaneously with a single TOML config file
- **Schema Explorer**: Progressive disclosure — browse catalogs, datasets, schemas, tables, and columns
- **Safety Guardrails**: Read-only mode, row limits, and query timeouts to prevent runaway operations
- **Secure Access**: SSH tunneling, SSL/TLS encryption, and service account authentication
- **Zero Config**: Just provide a DSN — no drivers to install, no complex setup

## Supported Databases

| Database | DSN Protocol | Status |
|----------|-------------|--------|
| PostgreSQL | `postgres://` | Stable |
| MySQL | `mysql://` | Stable |
| MariaDB | `mariadb://` | Stable |
| SQL Server | `sqlserver://` | Stable |
| SQLite | `sqlite:///` | Stable |
| Databricks SQL | `databricks://` | Stable |
| Google BigQuery | `bigquery://` | Stable |

## MCP Tools

| Tool | Description |
|------|-------------|
| `execute_sql` | Execute SQL queries with read-only enforcement and row limiting |
| `search_objects` | Browse catalogs, schemas, tables, columns, indexes, and routines |
| Custom Tools | Define reusable, parameterized SQL queries in `dbhub.toml` |

## Quick Start

### NPM (recommended)

```bash
npx dbhub-analytics --dsn "postgres://user:password@localhost:5432/dbname"
```

### Databricks SQL

```bash
npx dbhub-analytics --dsn "databricks://token:YOUR_TOKEN@your-workspace.cloud.databricks.com/sql/2.0/warehouses/your_warehouse_id"
```

### Google BigQuery

```bash
npx dbhub-analytics --dsn "bigquery://your-gcp-project/your_dataset?keyFile=/path/to/service-account.json&location=US"
```

### Docker

```bash
docker run --rm --init \
   --name dbhub-analytics \
   --publish 8080:8080 \
   amansingh63/dbhub-analytics \
   --transport http \
   --port 8080 \
   --dsn "postgres://user:password@localhost:5432/dbname"
```

### Demo Mode

```bash
npx dbhub-analytics --demo
```

## Connect to Your AI Assistant

### Claude Desktop / Claude Code

Add to your `claude_desktop_config.json` or `.mcp.json`:

```json
{
  "mcpServers": {
    "dbhub-analytics": {
      "command": "npx",
      "args": ["-y", "dbhub-analytics", "--dsn", "bigquery://your-project/your_dataset?keyFile=/path/to/sa.json&location=US"]
    }
  }
}
```

### GitHub Copilot (VS Code)

Add to your `.vscode/mcp.json`:

```json
{
  "servers": {
    "dbhub-analytics": {
      "command": "npx",
      "args": ["-y", "dbhub-analytics", "--dsn", "postgres://user:pass@localhost:5432/dbname"]
    }
  }
}
```

### Cursor

Add to your Cursor MCP settings (`~/.cursor/mcp.json`):

```json
{
  "mcpServers": {
    "dbhub-analytics": {
      "command": "npx",
      "args": ["-y", "dbhub-analytics", "--dsn", "databricks://token:YOUR_TOKEN@host/sql/2.0/warehouses/ID"]
    }
  }
}
```

### Windsurf

Add to your Windsurf MCP config (`~/.codeium/windsurf/mcp_config.json`):

```json
{
  "mcpServers": {
    "dbhub-analytics": {
      "command": "npx",
      "args": ["-y", "dbhub-analytics", "--dsn", "mysql://user:pass@localhost:3306/dbname"]
    }
  }
}
```

### Multi-Database via TOML Config

For connecting to multiple databases, use a TOML config file with any of the above clients:

```json
{
  "mcpServers": {
    "dbhub-analytics": {
      "command": "npx",
      "args": ["-y", "dbhub-analytics", "--config", "/path/to/dbhub.toml"]
    }
  }
}
```

## Configuration

### DSN Connection Strings

| Database | Format |
|----------|--------|
| PostgreSQL | `postgres://user:pass@host:5432/dbname?sslmode=require` |
| MySQL | `mysql://user:pass@host:3306/dbname` |
| MariaDB | `mariadb://user:pass@host:3306/dbname` |
| SQL Server | `sqlserver://user:pass@host:1433/dbname` |
| SQLite | `sqlite:///path/to/file.db` or `sqlite:///:memory:` |
| Databricks | `databricks://token:TOKEN@host/sql/2.0/warehouses/ID?catalog=CAT&schema=SCH` |
| BigQuery | `bigquery://PROJECT_ID/DATASET?keyFile=/path/to/key.json&location=US` |

### Multi-Database Setup (TOML)

Connect to multiple databases simultaneously. Create a `dbhub.toml` file:

```toml
[[sources]]
id = "databricks_prod"
type = "databricks"
host = "adb-xxx.azuredatabricks.net"
password = "dapi..."
path = "/sql/2.0/warehouses/abc123"

[[sources]]
id = "bigquery_analytics"
type = "bigquery"
project = "my-gcp-project"
database = "analytics_dataset"
location = "US"

[[sources]]
id = "postgres_prod"
type = "postgres"
host = "localhost"
port = 5432
database = "production"
user = "dbuser"
password = "secret"
```

Then run:

```bash
npx dbhub-analytics --config dbhub.toml
```

See [Multi-Database Configuration](https://dbhub.ai/config/toml) and [Command-Line Options](https://dbhub.ai/config/command-line) for complete documentation.

## Use Cases

- **Data Analysts**: Ask Claude to query your Databricks or BigQuery data warehouse in natural language
- **Backend Engineers**: Explore database schemas, run diagnostic queries, and debug issues via AI
- **DevOps/SRE**: Connect to production read-replicas safely with read-only mode and row limits
- **Data Scientists**: Query multiple databases simultaneously — join insights across PostgreSQL and BigQuery

## Development

```bash
# Install dependencies
pnpm install

# Run in development mode
pnpm dev

# Build for production
pnpm build

# Run tests
pnpm test
```

## Credits

Built on top of [DBHub](https://github.com/bytebase/dbhub) by [Bytebase](https://www.bytebase.com/), an open-source database DevSecOps platform. Licensed under MIT.
