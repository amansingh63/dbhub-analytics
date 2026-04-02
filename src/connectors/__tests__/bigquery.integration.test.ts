import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { BigQueryConnector } from "../bigquery/index.js";
import type { Connector } from "../interface.js";

/**
 * BigQuery integration tests.
 *
 * Creates a dedicated test dataset with sample tables,
 * exercises the connector's dataset-level namespace discovery
 * and query execution, then tears everything down.
 *
 * Test infrastructure:
 *   - Dataset: dbhub_integration_test_<timestamp>
 *     - Table: users (id, name, email, age)
 *     - Table: orders (id, user_id, amount, created_at)
 *
 * Required environment variables:
 *   BIGQUERY_PROJECT_ID  - GCP project ID
 *   BIGQUERY_KEY_FILE    - path to service account JSON key file
 *                          (or set GOOGLE_APPLICATION_CREDENTIALS)
 *   BIGQUERY_LOCATION    - (optional) BigQuery location, defaults to US
 *
 * Run:
 *   BIGQUERY_PROJECT_ID=... BIGQUERY_KEY_FILE=... \
 *   pnpm test -- bigquery.integration
 */

const TEST_DATASET = `dbhub_integration_test_${Date.now()}`;

function getDSN(): string | null {
  const projectId = process.env.BIGQUERY_PROJECT_ID;
  const keyFile = process.env.BIGQUERY_KEY_FILE || process.env.GOOGLE_APPLICATION_CREDENTIALS;
  const location = process.env.BIGQUERY_LOCATION || "US";
  if (!projectId) return null;

  let dsn = `bigquery://${projectId}/${TEST_DATASET}?location=${location}`;
  if (keyFile) {
    dsn += `&keyFile=${encodeURIComponent(keyFile)}`;
  }
  return dsn;
}

const dsn = getDSN();
const describeOrSkip = dsn ? describe : describe.skip;

describeOrSkip("BigQuery Connector Integration Tests", () => {
  let connector: Connector;

  beforeAll(async () => {
    connector = new BigQueryConnector();
    await connector.connect(dsn!);

    // Create test dataset and tables
    await connector.executeSQL(
      `CREATE SCHEMA IF NOT EXISTS ${TEST_DATASET}`,
      {}
    );

    await connector.executeSQL(
      `CREATE TABLE IF NOT EXISTS ${TEST_DATASET}.users (
        id INT64 NOT NULL,
        name STRING NOT NULL,
        email STRING NOT NULL,
        age INT64
      )`,
      {}
    );

    await connector.executeSQL(
      `CREATE TABLE IF NOT EXISTS ${TEST_DATASET}.orders (
        id INT64 NOT NULL,
        user_id INT64 NOT NULL,
        amount NUMERIC,
        created_at TIMESTAMP
      )`,
      {}
    );

    await connector.executeSQL(
      `INSERT INTO ${TEST_DATASET}.users (id, name, email, age) VALUES
       (1, 'John Doe', 'john@example.com', 30),
       (2, 'Jane Smith', 'jane@example.com', 25),
       (3, 'Bob Johnson', 'bob@example.com', 35)`,
      {}
    );

    await connector.executeSQL(
      `INSERT INTO ${TEST_DATASET}.orders (id, user_id, amount, created_at) VALUES
       (1, 1, 99.99, CURRENT_TIMESTAMP()),
       (2, 1, 149.50, CURRENT_TIMESTAMP()),
       (3, 2, 75.25, CURRENT_TIMESTAMP())`,
      {}
    );
  }, 120_000);

  afterAll(async () => {
    if (connector) {
      try {
        await connector.executeSQL(`DROP SCHEMA IF EXISTS ${TEST_DATASET} CASCADE`, {});
      } catch {
        /* best-effort */
      }
      await connector.disconnect();
    }
  }, 60_000);

  // --- Connection ---

  describe("Connection", () => {
    it("should be connected", () => {
      expect(connector).toBeDefined();
    });

    it("should validate DSN format", () => {
      const sample = connector.dsnParser.getSampleDSN();
      expect(sample).toContain("bigquery://");
      expect(connector.dsnParser.isValidDSN(sample)).toBe(true);
      expect(connector.dsnParser.isValidDSN("invalid-dsn")).toBe(false);
    });
  });

  // --- Schema Discovery ---

  describe("Schema Discovery", () => {
    it("should list datasets and include test dataset", async () => {
      const schemas = await connector.getSchemas();
      expect(schemas.length).toBeGreaterThan(0);
      expect(schemas).toContain(TEST_DATASET);
    });

    it("should list tables in the test dataset", async () => {
      const tables = await connector.getTables(TEST_DATASET);
      expect(tables).toContain("users");
      expect(tables).toContain("orders");
    });

    it("should get table schema for users", async () => {
      const columns = await connector.getTableSchema("users", TEST_DATASET);
      expect(columns.length).toBe(4);
      expect(columns.map((c) => c.column_name)).toEqual(
        expect.arrayContaining(["id", "name", "email", "age"])
      );
      const idCol = columns.find((c) => c.column_name === "id");
      // BigQuery API may return "INTEGER" or "INT64" for INT64 columns
      expect(["INT64", "INTEGER"]).toContain(idCol?.data_type);
      expect(idCol?.is_nullable).toBe("NO"); // NOT NULL
    });

    it("should check table existence", async () => {
      expect(await connector.tableExists("users", TEST_DATASET)).toBe(true);
      expect(await connector.tableExists("orders", TEST_DATASET)).toBe(true);
      expect(await connector.tableExists("nonexistent_xyz", TEST_DATASET)).toBe(false);
    });

    it("should return empty indexes", async () => {
      const indexes = await connector.getTableIndexes("users", TEST_DATASET);
      expect(indexes).toEqual([]);
    });

    it("should get table comment (may be null)", async () => {
      const comment = await connector.getTableComment!("users", TEST_DATASET);
      expect(comment === null || typeof comment === "string").toBe(true);
    });
  });

  // --- SQL Execution ---

  describe("SQL Execution", () => {
    it("should SELECT COUNT from test table", async () => {
      const result = await connector.executeSQL(
        `SELECT COUNT(*) as count FROM ${TEST_DATASET}.users`,
        {}
      );
      expect(result.rows).toHaveLength(1);
      expect(Number(result.rows[0].count)).toBe(3);
    });

    it("should SELECT with WHERE clause", async () => {
      const result = await connector.executeSQL(
        `SELECT name, age FROM ${TEST_DATASET}.users WHERE age > 26 ORDER BY name`,
        {}
      );
      expect(result.rows.length).toBe(2);
      expect(result.rows[0].name).toBe("Bob Johnson");
    });

    it("should handle JOIN within the test dataset", async () => {
      const result = await connector.executeSQL(
        `SELECT u.name as user_name, o.amount
         FROM ${TEST_DATASET}.users u
         JOIN ${TEST_DATASET}.orders o ON u.id = o.user_id
         WHERE u.id = 1
         ORDER BY o.amount`,
        {}
      );
      expect(result.rows.length).toBe(2);
      expect(result.rows[0].user_name).toBe("John Doe");
    });

    it("should respect maxRows limit", async () => {
      const result = await connector.executeSQL(
        `SELECT * FROM ${TEST_DATASET}.users ORDER BY id`,
        { maxRows: 2 }
      );
      expect(result.rows).toHaveLength(2);
    });

    it("should execute INSERT and DELETE", async () => {
      await connector.executeSQL(
        `INSERT INTO ${TEST_DATASET}.users (id, name, email, age) VALUES (4, 'Test User', 'test@example.com', 28)`,
        {}
      );
      const result = await connector.executeSQL(
        `SELECT * FROM ${TEST_DATASET}.users WHERE id = 4`,
        {}
      );
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].name).toBe("Test User");

      await connector.executeSQL(`DELETE FROM ${TEST_DATASET}.users WHERE id = 4`, {});
    }, 30_000);

    it("should execute SELECT 1", async () => {
      const result = await connector.executeSQL("SELECT 1 as val", {});
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].val).toBe(1);
    });
  });

  // --- Error Handling ---

  describe("Error Handling", () => {
    it("should throw on invalid SQL", async () => {
      await expect(connector.executeSQL("INVALID SQL QUERY", {})).rejects.toThrow();
    });

    it("should throw on nonexistent table", async () => {
      await expect(
        connector.executeSQL("SELECT * FROM nonexistent_table_xyz_123", {})
      ).rejects.toThrow();
    });

    it("should throw when not connected", async () => {
      const newConnector = new BigQueryConnector();
      await expect(newConnector.executeSQL("SELECT 1", {})).rejects.toThrow(
        /Not connected to BigQuery/
      );
    });
  });
});
