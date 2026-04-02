# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

# DBHub Development Guidelines

DBHub is a database MCP server implementing the Model Context Protocol (MCP) server interface. It bridges MCP-compatible clients (Claude Desktop, Claude Code, Cursor) with various database systems (PostgreSQL, MySQL, MariaDB, SQL Server, SQLite, Databricks SQL, Google BigQuery).

## Commands

Prerequisites: Node.js >= 20, pnpm, Docker (for integration tests).

```bash
pnpm install                    # Install dependencies (monorepo: root + frontend/)
pnpm run build                  # Full build: generate API types + tsup backend + vite frontend
pnpm run build:backend          # Backend only (generates API types + tsup)
pnpm run dev                    # Dev mode: backend (HTTP transport) + frontend (Vite) concurrently
pnpm run dev:backend            # Backend dev only with tsx
pnpm run generate:api-types     # Regenerate src/api/openapi.d.ts from src/api/openapi.yaml

# Testing (vitest)
pnpm test                       # All tests (unit + integration)
pnpm test:unit                  # Unit tests only (no Docker)
pnpm test:integration           # Integration tests only (requires Docker)
pnpm test:watch                 # Interactive watch mode
pnpm test src/utils/__tests__/sql-parser.test.ts           # Single test file
pnpm test -- --testNamePattern="PostgreSQL"                 # Tests matching pattern
pnpm test:build                 # Post-build smoke test
```

## Architecture Overview

### Monorepo Structure

pnpm workspace with two packages (`pnpm-workspace.yaml`):
- **Root** — Backend TypeScript MCP server, bundled with tsup to `dist/`
- **`frontend/`** — React SPA (Vite + Tailwind + CodeMirror), builds to `dist/public/`

### Backend (`src/`)

```
src/
├── index.ts                 # Entry point: dynamic-imports connectors, calls main()
├── server.ts                # MCP server: stdio/HTTP transport, Express routes, tool registration
├── connectors/
│   ├── interface.ts         # Connector/DSNParser interfaces, ConnectorRegistry
│   ├── manager.ts           # ConnectorManager: multi-source connection lifecycle
│   └── {postgres,mysql,mariadb,sqlserver,sqlite,databricks,bigquery}/index.ts
├── tools/
│   ├── index.ts             # registerTools(): wires tools to MCP server
│   ├── registry.ts          # ToolRegistry: manages enabled tools per source
│   ├── execute-sql.ts       # execute_sql tool handler
│   ├── search-objects.ts    # search_objects tool handler (progressive disclosure)
│   └── custom-tool-handler.ts  # User-defined SQL tools from TOML config
├── api/                     # REST API for the Workbench frontend
│   ├── openapi.yaml         # OpenAPI spec (source of truth for API types)
│   ├── openapi.d.ts         # Generated types (run generate:api-types)
│   ├── sources.ts           # /api/sources endpoints
│   └── requests.ts          # /api/requests endpoints
├── config/
│   ├── env.ts               # CLI args + env var resolution
│   ├── toml-loader.ts       # TOML config parsing and validation
│   └── demo-loader.ts       # --demo mode setup
├── requests/store.ts        # In-memory request history (FIFO, 100 per source)
├── types/                   # TypeScript interfaces (config.ts, sql.ts, ssh.ts)
└── utils/                   # ~20 utility modules
```

### Key Architectural Patterns

- **Connector Registry** (`src/connectors/interface.ts`): Static registry where each database connector self-registers. `ConnectorRegistry.register(connector)` at module load time.

- **Connector Manager** (`src/connectors/manager.ts`): Manages `Map<sourceId, Connector>` for multi-database support. `ConnectorManager.getCurrentConnector(sourceId?)` is the static accessor used by tool handlers. First source is the default.

- **Dynamic Driver Loading** (`src/index.ts`, `src/utils/module-loader.ts`): Database drivers (`pg`, `mysql2`, `@databricks/sql`, `@google-cloud/bigquery`, etc.) are `optionalDependencies` loaded via dynamic `import()`. tsup externalizes them so they aren't bundled into ESM. A missing driver skips that connector silently.

- **Transport Modes** (`src/server.ts`): stdio (default, for desktop tools) or HTTP (`--transport=http`). HTTP uses Express with `/mcp` endpoint (stateless `StreamableHTTPServerTransport`), `/api/*` REST routes, `/healthz`, and serves the frontend from `dist/public/` in production.

- **Tool System** (`src/tools/`): Two built-in tools (`execute_sql`, `search_objects`) plus user-defined custom tools from TOML `[[tools]]` sections. Tools accept optional `source_id` for multi-database routing.

- **Progressive Disclosure** (`search_objects`): Single tool for schema exploration with detail levels: `names` (minimal), `summary` (with metadata), `full` (complete structure). Pattern defaults to `%` (match all).

## Configuration

Three methods in priority order:

1. **Command-line arguments** (highest) — `--dsn`, `--transport`, `--port`, `--config`, `--demo`, `--readonly`, `--max-rows`, SSH options
2. **TOML config file** — `dbhub.toml` or `--config=path`. Supports multi-database `[[sources]]` and `[[tools]]` sections. See `dbhub.toml.example` for full reference.
3. **Environment variables / `.env` files** (lowest) — `DSN` or individual `DB_TYPE`, `DB_HOST`, etc. See `.env.example`.

## Adding a Database Connector

1. Create `src/connectors/{db-type}/index.ts`
2. Implement the `Connector` and `DSNParser` interfaces from `src/connectors/interface.ts`
3. Register with `ConnectorRegistry.register(connector)` in the module
4. Add dynamic import entry in `src/index.ts` `connectorModules` array
5. Add the driver to `optionalDependencies` in `package.json` and to `external` in `tsup.config.ts`
6. Add `"db-type"` to all `Record<ConnectorType, ...>` maps: `allowedKeywords` + `mutatingPatterns` (`src/utils/allowed-keywords.ts`), `dialectScanners` (`src/utils/sql-parser.ts`), `protocolToConnectorType` + `getDefaultPortForType` (`src/utils/dsn-obfuscate.ts`)
7. Add identifier quoting case in `src/utils/identifier-quoter.ts`
8. Add type to `ConnectionParams.type` in `src/types/config.ts`, OpenAPI enum in `src/api/openapi.yaml`, and frontend `DatabaseType` in `frontend/src/types/datasource.ts`
9. Add DSN builder in `src/config/toml-loader.ts` and env var support in `src/config/env.ts`
10. Add logo SVG in `frontend/src/assets/logos/` and register in `frontend/src/lib/db-logos.ts`

DSN formats: `postgres://`, `mysql://`, `mariadb://`, `sqlserver://`, `sqlite:///path`, `sqlite:///:memory:`, `databricks://token:TOKEN@HOST/HTTP_PATH`, `bigquery://PROJECT_ID/DATASET?keyFile=PATH&location=LOC`

## Databricks Connector

The Databricks connector uses the `@databricks/sql` Node.js driver to connect to Databricks SQL Warehouses via the Thrift protocol.

**DSN format:**
```
databricks://token:ACCESS_TOKEN@WORKSPACE_HOST:PORT/HTTP_PATH?catalog=CATALOG&schema=SCHEMA
```

- `token` (username): fixed literal, indicates PAT authentication
- `ACCESS_TOKEN` (password): Databricks Personal Access Token (e.g., `dapi...`)
- `WORKSPACE_HOST`: workspace hostname (e.g., `adb-xxx.azuredatabricks.net`)
- `PORT`: optional, defaults to 443
- `HTTP_PATH`: SQL warehouse path (e.g., `/sql/2.0/warehouses/abc123`)
- `catalog`, `schema`: optional query params for initial catalog/schema (schema defaults to `default`)

**TOML configuration:**
```toml
[[sources]]
id = "databricks_prod"
type = "databricks"
host = "adb-xxx.azuredatabricks.net"
password = "dapi..."           # access token
path = "/sql/2.0/warehouses/abc123"  # HTTP path to SQL warehouse
```

**Key differences from other connectors:**
- No traditional indexes (returns empty array for `getTableIndexes`)
- Three-level namespace: catalog.schema.table (schema discovery scoped to current catalog)
- Uses `INFORMATION_SCHEMA` for metadata queries with single-quoted string literals (not backtick identifiers) in WHERE clauses
- Integration tests require real Databricks credentials (env vars: `DATABRICKS_SERVER_HOSTNAME`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN`), not Docker/Testcontainers

## BigQuery Connector

The BigQuery connector uses the `@google-cloud/bigquery` Node.js client library to connect to Google BigQuery via HTTP API.

**DSN format:**
```
bigquery://PROJECT_ID/DATASET?keyFile=/path/to/keyfile.json&location=US
```

- `PROJECT_ID` (hostname): GCP project ID (required)
- `DATASET` (pathname): default dataset (optional)
- `keyFile`: path to service account JSON key file (optional, falls back to ADC / `GOOGLE_APPLICATION_CREDENTIALS`)
- `location`: BigQuery processing location (optional, e.g., `US`, `EU`)

**TOML configuration:**
```toml
[[sources]]
id = "bigquery_prod"
type = "bigquery"
project = "my-gcp-project"        # GCP project ID
database = "analytics_dataset"    # default dataset (optional)
password = "/path/to/sa-key.json" # key file path (optional)
location = "US"                   # processing location (optional, e.g., US, EU)
```

**Key differences from other connectors:**
- Stateless HTTP client — no persistent connection, `disconnect()` is a no-op
- No traditional indexes (returns empty array for `getTableIndexes`)
- Three-level namespace: project (catalog) > dataset (schema) > table
- `getCatalogs()` returns the connected project ID
- Uses `@google-cloud/bigquery` client API for metadata (not raw SQL `INFORMATION_SCHEMA`)
- Backtick quoting for identifiers (same as MySQL/Databricks)
- Integration tests require real GCP credentials (env vars: `BIGQUERY_PROJECT_ID`, `BIGQUERY_KEY_FILE`), not Docker/Testcontainers

## Testing

Vitest with two projects (`vitest.config.ts`):
- **unit**: `*.test.ts` excluding `*integration*` in filename — no Docker needed
- **integration**: `*integration*.test.ts` — requires Docker + Testcontainers

**Naming convention matters**: integration tests MUST have `integration` in their filename to be routed correctly.

Database connector integration tests extend `IntegrationTestBase` (`src/connectors/__tests__/shared/integration-test-base.ts`) which provides container lifecycle, shared test suites, and standard test data (`users` + `orders` tables).

SQL Server containers are the slowest to start (3-5 min) and need 4GB+ Docker memory.

Databricks integration tests require real credentials via environment variables (no Docker):
```bash
DATABRICKS_SERVER_HOSTNAME=host DATABRICKS_HTTP_PATH=/sql/... DATABRICKS_TOKEN=dapi... pnpm test -- databricks.integration
```

BigQuery integration tests require real GCP credentials via environment variables (no Docker):
```bash
BIGQUERY_PROJECT_ID=my-project BIGQUERY_KEY_FILE=/path/to/sa-key.json pnpm test -- bigquery.integration
```

See `.claude/skills/testing/SKILL.md` for detailed testing guidance.

## Code Style

- TypeScript strict mode, ES modules with `.js` extension in imports
- Import order: Node.js core → third-party → local modules
- camelCase for variables/functions, PascalCase for classes/types
- async/await, try/finally for DB connections, parameterized queries
- Input validation with zod schemas
