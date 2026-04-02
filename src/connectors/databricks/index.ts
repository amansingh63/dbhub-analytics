/**
 * Databricks SQL Connector Implementation
 *
 * Implements Databricks database connectivity for DBHub using @databricks/sql driver.
 * DSN format: databricks://token:ACCESS_TOKEN@HOST:PORT/HTTP_PATH?catalog=CATALOG&schema=SCHEMA
 *
 * Example:
 *   databricks://token:dapi1234abcd@adb-1234.azuredatabricks.net/sql/2.0/warehouses/abc123
 *   databricks://token:dapi1234abcd@my-workspace.cloud.databricks.com:443/sql/2.0/warehouses/abc123?catalog=main&schema=default
 */

import { DBSQLClient } from "@databricks/sql";
import {
  Connector,
  ConnectorType,
  ConnectorRegistry,
  DSNParser,
  SQLResult,
  TableColumn,
  TableIndex,
  StoredProcedure,
  ExecuteOptions,
  ConnectorConfig,
} from "../interface.js";
import { SafeURL } from "../../utils/safe-url.js";
import { obfuscateDSNPassword } from "../../utils/dsn-obfuscate.js";
import { SQLRowLimiter } from "../../utils/sql-row-limiter.js";
import { splitSQLStatements } from "../../utils/sql-parser.js";
import { quoteIdentifier } from "../../utils/identifier-quoter.js";
import {
  ConnectionOptions,
  OpenSessionRequest,
} from "@databricks/sql/dist/contracts/IDBSQLClient.js";

interface DatabricksConnectionConfig {
  host: string;
  port: number;
  path: string;
  token: string;
  catalog?: string;
  schema?: string;
}

/**
 * Databricks DSN Parser
 * Handles DSN strings like:
 *   databricks://token:ACCESS_TOKEN@HOST:PORT/HTTP_PATH?catalog=CAT&schema=SCH
 *
 * Components:
 *   - username: "token" (fixed, indicates PAT authentication)
 *   - password: the Personal Access Token
 *   - host: Databricks workspace hostname (e.g., adb-xxx.azuredatabricks.net)
 *   - port: optional (default 443)
 *   - path: HTTP path to SQL warehouse (e.g., /sql/2.0/warehouses/abc123)
 *   - catalog: optional query param, sets initial catalog
 *   - schema: optional query param, sets initial schema (default: "default")
 */
class DatabricksDSNParser implements DSNParser {
  async parse(dsn: string, config?: ConnectorConfig): Promise<DatabricksConnectionConfig> {
    if (!this.isValidDSN(dsn)) {
      const obfuscatedDSN = obfuscateDSNPassword(dsn);
      const expectedFormat = this.getSampleDSN();
      throw new Error(
        `Invalid Databricks DSN format.\nProvided: ${obfuscatedDSN}\nExpected: ${expectedFormat}`
      );
    }

    try {
      const url = new SafeURL(dsn);

      const host = url.hostname;
      if (!host) {
        throw new Error("Host is required in Databricks DSN");
      }

      const token = url.password;
      if (!token) {
        throw new Error("Access token (password) is required in Databricks DSN");
      }

      const port = url.port ? parseInt(url.port) : 443;

      // The pathname is the HTTP path to the SQL warehouse
      const httpPath = url.pathname;
      if (!httpPath || httpPath === "/") {
        throw new Error(
          "HTTP path to SQL warehouse is required in Databricks DSN (e.g., /sql/2.0/warehouses/abc123)"
        );
      }

      const result: DatabricksConnectionConfig = {
        host,
        port,
        path: httpPath,
        token,
      };

      // Parse optional query parameters
      url.forEachSearchParam((value, key) => {
        if (key === "catalog") {
          result.catalog = value;
        } else if (key === "schema") {
          result.schema = value;
        }
      });

      return result;
    } catch (error) {
      if (error instanceof Error && error.message.includes("Databricks DSN")) {
        throw error;
      }
      throw new Error(
        `Failed to parse Databricks DSN: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  getSampleDSN(): string {
    return "databricks://token:dapi***@adb-xxx.azuredatabricks.net/sql/2.0/warehouses/xxx";
  }

  isValidDSN(dsn: string): boolean {
    return dsn.startsWith("databricks://");
  }
}

/**
 * Databricks SQL Connector Implementation
 */
export class DatabricksConnector implements Connector {
  id: ConnectorType = "databricks";
  name = "Databricks";
  dsnParser = new DatabricksDSNParser();

  private client: DBSQLClient | null = null;
  private session: any = null; // IDBSQLSession
  private defaultSchema: string = "default";
  private defaultCatalog: string | undefined;

  // Source ID is set by ConnectorManager after cloning
  private sourceId: string = "default";

  getId(): string {
    return this.sourceId;
  }

  clone(): Connector {
    return new DatabricksConnector();
  }

  async connect(dsn: string, initScript?: string, config?: ConnectorConfig): Promise<void> {
    const parsedConfig = await this.dsnParser.parse(dsn, config);

    try {
      this.client = new DBSQLClient();

      const connectOptions: ConnectionOptions = {
        host: parsedConfig.host,
        path: parsedConfig.path,
        token: parsedConfig.token,
      };

      if (parsedConfig.port !== 443) {
        connectOptions.port = parsedConfig.port;
      }

      if (config?.connectionTimeoutSeconds) {
        connectOptions.socketTimeout = config.connectionTimeoutSeconds * 1000;
      }

      await this.client.connect(connectOptions);

      // Open a session with optional catalog/schema
      const sessionOptions: OpenSessionRequest = {};
      if (parsedConfig.catalog) {
        sessionOptions.initialCatalog = parsedConfig.catalog;
        this.defaultCatalog = parsedConfig.catalog;
      }
      if (parsedConfig.schema) {
        sessionOptions.initialSchema = parsedConfig.schema;
        this.defaultSchema = parsedConfig.schema;
      }

      this.session = await this.client.openSession(sessionOptions);

      // Run init script if provided
      if (initScript) {
        const statements = splitSQLStatements(initScript, "databricks");
        for (const stmt of statements) {
          await this.runStatement(stmt);
        }
      }
    } catch (err) {
      console.error("Failed to connect to Databricks:", err);
      throw err;
    }
  }

  async disconnect(): Promise<void> {
    if (this.session) {
      try {
        await this.session.close();
      } catch (err) {
        console.error("Error closing Databricks session:", err);
      }
      this.session = null;
    }
    if (this.client) {
      try {
        await this.client.close();
      } catch (err) {
        console.error("Error closing Databricks client:", err);
      }
      this.client = null;
    }
  }

  /**
   * Execute a SQL statement and return rows.
   * Handles the operation lifecycle: execute -> fetch -> close.
   */
  private async runStatement(sql: string, options?: { queryTimeout?: number }): Promise<any[]> {
    if (!this.session) {
      throw new Error("Not connected to Databricks database");
    }

    const stmtOptions: any = {};
    if (options?.queryTimeout) {
      stmtOptions.queryTimeout = options.queryTimeout;
    }

    const operation = await this.session.executeStatement(sql, stmtOptions);
    try {
      const rows = await operation.fetchAll();
      return rows;
    } finally {
      await operation.close();
    }
  }

  /**
   * Build a three-part qualified identifier: `catalog`.`schema`.`table`.
   * When catalog is not provided, returns `schema`.`table` (session-scoped).
   */
  private qualifiedName(schema: string, catalog?: string, table?: string): string {
    const parts: string[] = [];
    if (catalog) parts.push(quoteIdentifier(catalog, "databricks"));
    parts.push(quoteIdentifier(schema, "databricks"));
    if (table) parts.push(quoteIdentifier(table, "databricks"));
    return parts.join(".");
  }

  /**
   * List all catalogs.
   * SQL: SHOW CATALOGS
   */
  async getCatalogs(): Promise<string[]> {
    if (!this.session) {
      throw new Error("Not connected to Databricks database");
    }

    const rows = await this.runStatement(`SHOW CATALOGS`);
    return rows.map((row: any) => row.catalog);
  }

  /**
   * List schemas. When catalog is provided, lists schemas in that catalog.
   * SQL: SHOW SCHEMAS IN <catalog>
   */
  async getSchemas(catalog?: string): Promise<string[]> {
    if (!this.session) {
      throw new Error("Not connected to Databricks database");
    }

    const target = catalog
      ? quoteIdentifier(catalog, "databricks")
      : quoteIdentifier(this.defaultCatalog || "current_catalog()", "databricks");

    // Use SHOW SCHEMAS — works for all catalogs (unlike information_schema)
    const sql = catalog
      ? `SHOW SCHEMAS IN ${quoteIdentifier(catalog, "databricks")}`
      : `SHOW SCHEMAS`;
    const rows = await this.runStatement(sql);

    return rows
      .map((row: any) => row.databaseName || row.namespace || row.schema_name)
      .filter((name: string) => name !== "information_schema")
      .sort();
  }

  /**
   * List tables in a schema. When catalog is provided, uses catalog.schema.
   * SQL: SHOW TABLES IN <catalog>.<schema>
   */
  async getTables(schema?: string, catalog?: string): Promise<string[]> {
    if (!this.session) {
      throw new Error("Not connected to Databricks database");
    }

    const schemaToUse = schema || this.defaultSchema;
    const qualifiedSchema = catalog
      ? this.qualifiedName(schemaToUse, catalog)
      : quoteIdentifier(schemaToUse, "databricks");

    const rows = await this.runStatement(`SHOW TABLES IN ${qualifiedSchema}`);

    return rows
      .map((row: any) => row.tableName || row.table_name)
      .sort();
  }

  /**
   * Check if a table exists.
   * Uses SHOW TABLES and checks membership.
   */
  async tableExists(tableName: string, schema?: string, catalog?: string): Promise<boolean> {
    if (!this.session) {
      throw new Error("Not connected to Databricks database");
    }

    const tables = await this.getTables(schema, catalog);
    return tables.includes(tableName);
  }

  /**
   * Get column information for a table.
   * SQL: DESCRIBE TABLE EXTENDED <catalog>.<schema>.<table>
   */
  async getTableSchema(tableName: string, schema?: string, catalog?: string): Promise<TableColumn[]> {
    if (!this.session) {
      throw new Error("Not connected to Databricks database");
    }

    const schemaToUse = schema || this.defaultSchema;
    const qualifiedTable = catalog
      ? this.qualifiedName(schemaToUse, catalog, tableName)
      : `${quoteIdentifier(schemaToUse, "databricks")}.${quoteIdentifier(tableName, "databricks")}`;

    const rows = await this.runStatement(`DESCRIBE TABLE ${qualifiedTable}`);

    // Filter out metadata rows (e.g., "# Clustering Information", "# col_name", empty rows)
    return rows
      .filter((row: any) => row.col_name && !row.col_name.startsWith("#") && row.data_type !== "")
      .map((row: any) => ({
        column_name: row.col_name,
        data_type: row.data_type || "unknown",
        is_nullable: "YES",
        column_default: null,
        description: row.comment ?? null,
      }));
  }

  async getTableIndexes(_tableName: string, _schema?: string): Promise<TableIndex[]> {
    // Databricks does not support traditional database indexes.
    // Data is organized using Delta Lake file layout and Z-ordering,
    // which are not compatible with the TableIndex interface.
    return [];
  }

  /**
   * Get table comment.
   * SQL: DESCRIBE TABLE EXTENDED <catalog>.<schema>.<table>
   * Looks for the "Comment" row in the extended output.
   */
  async getTableComment(tableName: string, schema?: string, catalog?: string): Promise<string | null> {
    if (!this.session) {
      throw new Error("Not connected to Databricks database");
    }

    const schemaToUse = schema || this.defaultSchema;
    const qualifiedTable = catalog
      ? this.qualifiedName(schemaToUse, catalog, tableName)
      : `${quoteIdentifier(schemaToUse, "databricks")}.${quoteIdentifier(tableName, "databricks")}`;

    try {
      const rows = await this.runStatement(`DESCRIBE TABLE EXTENDED ${qualifiedTable}`);
      const commentRow = rows.find((row: any) => row.col_name === "Comment");
      return commentRow?.data_type || null;
    } catch {
      return null;
    }
  }

  /**
   * List stored procedures/functions.
   * SQL: SHOW FUNCTIONS IN <catalog>.<schema>
   */
  async getStoredProcedures(
    schema?: string,
    routineType?: "procedure" | "function",
    catalog?: string
  ): Promise<string[]> {
    if (!this.session) {
      throw new Error("Not connected to Databricks database");
    }

    const schemaToUse = schema || this.defaultSchema;
    const qualifiedSchema = catalog
      ? this.qualifiedName(schemaToUse, catalog)
      : quoteIdentifier(schemaToUse, "databricks");

    try {
      const rows = await this.runStatement(`SHOW FUNCTIONS IN ${qualifiedSchema}`);
      return rows
        .map((row: any) => row.function || row.routine_name || "")
        .filter((name: string) => name && !name.includes("."))
        .sort();
    } catch {
      return [];
    }
  }

  /**
   * Get details for a stored procedure/function.
   * SQL: DESCRIBE FUNCTION <catalog>.<schema>.<name>
   */
  async getStoredProcedureDetail(procedureName: string, schema?: string, catalog?: string): Promise<StoredProcedure> {
    if (!this.session) {
      throw new Error("Not connected to Databricks database");
    }

    const schemaToUse = schema || this.defaultSchema;
    const qualifiedFunc = catalog
      ? this.qualifiedName(schemaToUse, catalog, procedureName)
      : `${quoteIdentifier(schemaToUse, "databricks")}.${quoteIdentifier(procedureName, "databricks")}`;

    try {
      const rows = await this.runStatement(`DESCRIBE FUNCTION ${qualifiedFunc}`);
      return {
        procedure_name: procedureName,
        procedure_type: "function",
        language: "sql",
        parameter_list: "",
        return_type: undefined,
        definition: rows.map((r: any) => r.function_desc || "").join("\n") || undefined,
      };
    } catch (error) {
      throw new Error(
        `Failed to get details for '${procedureName}' in schema '${schemaToUse}': ${error}`
      );
    }
  }

  async executeSQL(sql: string, options: ExecuteOptions, parameters?: any[]): Promise<SQLResult> {
    if (!this.session) {
      throw new Error("Not connected to Databricks database");
    }

    const statements = splitSQLStatements(sql, "databricks");

    if (statements.length === 1) {
      return this.executeSingleStatement(statements[0], options, parameters);
    }

    // Multiple statements - parameters not supported
    if (parameters && parameters.length > 0) {
      throw new Error("Parameters are not supported for multi-statement queries in Databricks");
    }

    let allRows: any[] = [];
    let totalRowCount = 0;

    for (const statement of statements) {
      const result = await this.executeSingleStatement(statement, options);
      if (result.rows.length > 0) {
        allRows.push(...result.rows);
      }
      totalRowCount += result.rowCount;
    }

    return { rows: allRows, rowCount: totalRowCount };
  }

  private async executeSingleStatement(
    sql: string,
    options: ExecuteOptions,
    parameters?: any[]
  ): Promise<SQLResult> {
    // Apply maxRows limit to SELECT queries
    const processedSQL = SQLRowLimiter.applyMaxRows(sql, options.maxRows);

    const stmtOptions: any = {};
    if (parameters && parameters.length > 0) {
      stmtOptions.ordinalParameters = parameters;
    }

    const operation = await this.session.executeStatement(processedSQL, stmtOptions);
    try {
      const rows = await operation.fetchAll();

      // For DML operations, Databricks may return a row with num_affected_rows
      if (rows.length === 1 && rows[0]?.num_affected_rows !== undefined) {
        return { rows: [], rowCount: Number(rows[0].num_affected_rows) };
      }

      return { rows, rowCount: rows.length };
    } catch (error) {
      console.error(`[Databricks executeSQL] ERROR: ${(error as Error).message}`);
      console.error(`[Databricks executeSQL] SQL: ${processedSQL}`);
      if (parameters) {
        console.error(`[Databricks executeSQL] Parameters: ${JSON.stringify(parameters)}`);
      }
      throw error;
    } finally {
      await operation.close();
    }
  }
}

// Create and register the connector
const databricksConnector = new DatabricksConnector();
ConnectorRegistry.register(databricksConnector);
