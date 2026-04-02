/**
 * Google BigQuery Connector Implementation
 *
 * Implements BigQuery database connectivity for DBHub using @google-cloud/bigquery.
 * DSN format: bigquery://PROJECT_ID/DATASET?keyFile=/path/to/keyfile.json&location=US
 *
 * Example:
 *   bigquery://my-gcp-project/analytics_dataset?keyFile=./sa-key.json
 *   bigquery://my-gcp-project?location=US
 */

import { BigQuery } from "@google-cloud/bigquery";
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

interface BigQueryConnectionConfig {
  projectId: string;
  datasetId?: string;
  keyFilename?: string;
  location?: string;
}

/**
 * BigQuery DSN Parser
 * Handles DSN strings like:
 *   bigquery://PROJECT_ID/DATASET?keyFile=/path/to/keyfile.json&location=US
 *
 * Components:
 *   - hostname: GCP project ID (required)
 *   - pathname: default dataset (optional)
 *   - keyFile: path to service account JSON key file (optional, falls back to ADC)
 *   - location: BigQuery processing location (optional)
 */
class BigQueryDSNParser implements DSNParser {
  async parse(dsn: string, config?: ConnectorConfig): Promise<BigQueryConnectionConfig> {
    if (!this.isValidDSN(dsn)) {
      const obfuscatedDSN = obfuscateDSNPassword(dsn);
      const expectedFormat = this.getSampleDSN();
      throw new Error(
        `Invalid BigQuery DSN format.\nProvided: ${obfuscatedDSN}\nExpected: ${expectedFormat}`
      );
    }

    try {
      const url = new SafeURL(dsn);

      const projectId = url.hostname;
      if (!projectId) {
        throw new Error("Project ID (hostname) is required in BigQuery DSN");
      }

      const result: BigQueryConnectionConfig = {
        projectId,
      };

      // pathname is the default dataset (optional)
      if (url.pathname && url.pathname !== "/") {
        result.datasetId = url.pathname.startsWith("/")
          ? url.pathname.substring(1)
          : url.pathname;
      }

      // Parse optional query parameters
      url.forEachSearchParam((value, key) => {
        if (key === "keyFile") {
          result.keyFilename = value;
        } else if (key === "location") {
          result.location = value;
        }
      });

      return result;
    } catch (error) {
      if (error instanceof Error && error.message.includes("BigQuery DSN")) {
        throw error;
      }
      throw new Error(
        `Failed to parse BigQuery DSN: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  getSampleDSN(): string {
    return "bigquery://my-gcp-project/my_dataset?keyFile=/path/to/keyfile.json";
  }

  isValidDSN(dsn: string): boolean {
    return dsn.startsWith("bigquery://");
  }
}

/**
 * BigQuery Connector Implementation
 *
 * Key differences from traditional database connectors:
 * - Stateless HTTP client — no persistent connection, disconnect() is a no-op
 * - No traditional indexes (returns empty array)
 * - Three-level namespace: project (catalog) > dataset (schema) > table
 * - getCatalogs() returns the connected project ID
 * - Uses backtick quoting for identifiers
 */
export class BigQueryConnector implements Connector {
  id: ConnectorType = "bigquery";
  name = "BigQuery";
  dsnParser = new BigQueryDSNParser();

  private client: BigQuery | null = null;
  private projectId: string | undefined;
  private defaultDataset: string | undefined;
  private queryTimeoutMs: number | undefined;

  // Source ID is set by ConnectorManager after cloning
  private sourceId: string = "default";

  getId(): string {
    return this.sourceId;
  }

  clone(): Connector {
    return new BigQueryConnector();
  }

  async connect(dsn: string, initScript?: string, config?: ConnectorConfig): Promise<void> {
    const parsedConfig = await this.dsnParser.parse(dsn, config);

    try {
      const clientOptions: any = {
        projectId: parsedConfig.projectId,
      };

      if (parsedConfig.keyFilename) {
        clientOptions.keyFilename = parsedConfig.keyFilename;
      }

      if (parsedConfig.location) {
        clientOptions.location = parsedConfig.location;
      }

      this.client = new BigQuery(clientOptions);
      this.projectId = parsedConfig.projectId;
      this.defaultDataset = parsedConfig.datasetId;

      if (config?.queryTimeoutSeconds) {
        this.queryTimeoutMs = config.queryTimeoutSeconds * 1000;
      }

      // Run init script if provided
      if (initScript) {
        const statements = splitSQLStatements(initScript, "bigquery");
        for (const stmt of statements) {
          await this.runQuery(stmt);
        }
      }
    } catch (err) {
      console.error("Failed to connect to BigQuery:", err);
      throw err;
    }
  }

  async disconnect(): Promise<void> {
    // BigQuery client is stateless HTTP — no connection to close
    this.client = null;
    this.projectId = undefined;
    this.defaultDataset = undefined;
  }

  /**
   * Execute a query and return rows.
   */
  private async runQuery(sql: string, params?: any[]): Promise<any[]> {
    if (!this.client) {
      throw new Error("Not connected to BigQuery");
    }

    const queryOptions: any = {
      query: sql,
    };

    if (params && params.length > 0) {
      queryOptions.params = params;
    }

    if (this.queryTimeoutMs) {
      queryOptions.jobTimeoutMs = this.queryTimeoutMs;
    }

    const [rows] = await this.client.query(queryOptions);
    return rows;
  }

  /**
   * List catalogs (projects).
   * BigQuery three-level namespace: project (catalog) > dataset (schema) > table.
   * Returns the connected project ID.
   */
  async getCatalogs(): Promise<string[]> {
    if (!this.client) {
      throw new Error("Not connected to BigQuery");
    }
    return this.projectId ? [this.projectId] : [];
  }

  /**
   * List datasets (schemas) in the project.
   * @param catalog Ignored — BigQuery connection is scoped to one project.
   */
  async getSchemas(catalog?: string): Promise<string[]> {
    if (!this.client) {
      throw new Error("Not connected to BigQuery");
    }

    const [datasets] = await this.client.getDatasets();
    return datasets
      .map((ds: any) => ds.id || "")
      .filter((id: string) => id)
      .sort();
  }

  /**
   * List tables in a dataset.
   * @param schema Dataset name. If not provided, uses the default dataset.
   */
  async getTables(schema?: string, catalog?: string): Promise<string[]> {
    if (!this.client) {
      throw new Error("Not connected to BigQuery");
    }

    const datasetId = schema || this.defaultDataset;
    if (!datasetId) {
      throw new Error(
        "Dataset name is required. Provide a schema parameter or set a default dataset in the DSN."
      );
    }

    const dataset = this.client.dataset(datasetId);
    const [tables] = await dataset.getTables();
    return tables
      .map((t: any) => t.id || "")
      .filter((id: string) => id)
      .sort();
  }

  /**
   * Check if a table exists in a dataset.
   */
  async tableExists(tableName: string, schema?: string, catalog?: string): Promise<boolean> {
    if (!this.client) {
      throw new Error("Not connected to BigQuery");
    }

    const tables = await this.getTables(schema);
    return tables.includes(tableName);
  }

  /**
   * Get column information for a table.
   * Uses table metadata API to retrieve the schema.
   */
  async getTableSchema(tableName: string, schema?: string, catalog?: string): Promise<TableColumn[]> {
    if (!this.client) {
      throw new Error("Not connected to BigQuery");
    }

    const datasetId = schema || this.defaultDataset;
    if (!datasetId) {
      throw new Error("Dataset name is required for getTableSchema");
    }

    const table = this.client.dataset(datasetId).table(tableName);
    const [metadata] = await table.getMetadata();
    const fields = metadata.schema?.fields || [];

    return fields.map((field: any) => ({
      column_name: field.name,
      data_type: field.type || "unknown",
      is_nullable: field.mode === "REQUIRED" ? "NO" : "YES",
      column_default: null,
      description: field.description || null,
    }));
  }

  /**
   * BigQuery does not support traditional indexes.
   */
  async getTableIndexes(_tableName: string, _schema?: string, _catalog?: string): Promise<TableIndex[]> {
    return [];
  }

  /**
   * Get table description from metadata.
   */
  async getTableComment(tableName: string, schema?: string, catalog?: string): Promise<string | null> {
    if (!this.client) {
      throw new Error("Not connected to BigQuery");
    }

    const datasetId = schema || this.defaultDataset;
    if (!datasetId) {
      return null;
    }

    try {
      const table = this.client.dataset(datasetId).table(tableName);
      const [metadata] = await table.getMetadata();
      return metadata.description || null;
    } catch {
      return null;
    }
  }

  /**
   * Get estimated row count from table metadata.
   */
  async getTableRowCount(tableName: string, schema?: string, catalog?: string): Promise<number | null> {
    if (!this.client) {
      throw new Error("Not connected to BigQuery");
    }

    const datasetId = schema || this.defaultDataset;
    if (!datasetId) {
      return null;
    }

    try {
      const table = this.client.dataset(datasetId).table(tableName);
      const [metadata] = await table.getMetadata();
      return metadata.numRows ? Number(metadata.numRows) : null;
    } catch {
      return null;
    }
  }

  /**
   * List routines (functions/procedures) in a dataset.
   * Uses INFORMATION_SCHEMA.ROUTINES query.
   */
  async getStoredProcedures(
    schema?: string,
    routineType?: "procedure" | "function",
    catalog?: string
  ): Promise<string[]> {
    if (!this.client) {
      throw new Error("Not connected to BigQuery");
    }

    const datasetId = schema || this.defaultDataset;
    if (!datasetId) {
      return [];
    }

    try {
      let sql = `SELECT routine_name FROM ${quoteIdentifier(datasetId, "bigquery")}.INFORMATION_SCHEMA.ROUTINES`;
      if (routineType === "procedure") {
        sql += ` WHERE routine_type = 'PROCEDURE'`;
      } else if (routineType === "function") {
        sql += ` WHERE routine_type = 'FUNCTION'`;
      }
      sql += ` ORDER BY routine_name`;

      const rows = await this.runQuery(sql);
      return rows.map((row: any) => row.routine_name).filter(Boolean);
    } catch {
      return [];
    }
  }

  /**
   * Get details for a stored procedure/function.
   */
  async getStoredProcedureDetail(procedureName: string, schema?: string, catalog?: string): Promise<StoredProcedure> {
    if (!this.client) {
      throw new Error("Not connected to BigQuery");
    }

    const datasetId = schema || this.defaultDataset;
    if (!datasetId) {
      throw new Error("Dataset name is required for getStoredProcedureDetail");
    }

    try {
      const sql = `SELECT routine_name, routine_type, routine_definition, data_type
        FROM ${quoteIdentifier(datasetId, "bigquery")}.INFORMATION_SCHEMA.ROUTINES
        WHERE routine_name = '${procedureName}'`;

      const rows = await this.runQuery(sql);
      if (rows.length === 0) {
        throw new Error(`Routine '${procedureName}' not found in dataset '${datasetId}'`);
      }

      const row = rows[0];
      return {
        procedure_name: procedureName,
        procedure_type: (row.routine_type || "FUNCTION").toLowerCase() === "procedure" ? "procedure" : "function",
        language: "sql",
        parameter_list: "",
        return_type: row.data_type || undefined,
        definition: row.routine_definition || undefined,
      };
    } catch (error) {
      throw new Error(
        `Failed to get details for '${procedureName}' in dataset '${datasetId}': ${error}`
      );
    }
  }

  async executeSQL(sql: string, options: ExecuteOptions, parameters?: any[]): Promise<SQLResult> {
    if (!this.client) {
      throw new Error("Not connected to BigQuery");
    }

    const statements = splitSQLStatements(sql, "bigquery");

    if (statements.length === 1) {
      return this.executeSingleStatement(statements[0], options, parameters);
    }

    // Multiple statements — parameters not supported
    if (parameters && parameters.length > 0) {
      throw new Error("Parameters are not supported for multi-statement queries in BigQuery");
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
    if (!this.client) {
      throw new Error("Not connected to BigQuery");
    }

    // Apply maxRows limit to SELECT queries
    const processedSQL = SQLRowLimiter.applyMaxRows(sql, options.maxRows);

    const queryOptions: any = {
      query: processedSQL,
    };

    if (parameters && parameters.length > 0) {
      queryOptions.params = parameters;
    }

    if (this.queryTimeoutMs) {
      queryOptions.jobTimeoutMs = this.queryTimeoutMs;
    }

    try {
      const [rows, , response] = await this.client.query(queryOptions);

      // For DML operations, check job statistics for affected rows
      const numDmlAffectedRows = response?.statistics?.query?.numDmlAffectedRows;
      if (numDmlAffectedRows !== undefined && numDmlAffectedRows !== null) {
        return { rows: [], rowCount: Number(numDmlAffectedRows) };
      }

      return { rows, rowCount: rows.length };
    } catch (error) {
      console.error(`[BigQuery executeSQL] ERROR: ${(error as Error).message}`);
      console.error(`[BigQuery executeSQL] SQL: ${processedSQL}`);
      if (parameters) {
        console.error(`[BigQuery executeSQL] Parameters: ${JSON.stringify(parameters)}`);
      }
      throw error;
    }
  }
}

// Create and register the connector
const bigqueryConnector = new BigQueryConnector();
ConnectorRegistry.register(bigqueryConnector);
