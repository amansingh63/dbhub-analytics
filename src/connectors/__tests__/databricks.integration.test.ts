import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { DatabricksConnector } from "../databricks/index.js";
import type { Connector } from "../interface.js";

/**
 * Databricks SQL integration tests.
 *
 * Creates a dedicated test catalog with two schemas and sample data,
 * exercises the connector's three-level namespace (catalog > schema > table)
 * discovery and query execution, then tears everything down.
 *
 * Test infrastructure:
 *   - Catalog: dbhub_integration_test
 *     - Schema: schema_a
 *       - Table: users (id, name, email, age)
 *       - Table: orders (id, user_id, amount, created_at)
 *     - Schema: schema_b
 *       - Table: products (id, name, price, category)
 *       - Table: reviews (id, product_id, rating, comment)
 *
 * Required environment variables:
 *   DATABRICKS_SERVER_HOSTNAME  - workspace hostname
 *   DATABRICKS_HTTP_PATH        - SQL warehouse HTTP path
 *   DATABRICKS_TOKEN            - personal access token
 *   DATABRICKS_SAMPLE_CATALOG   - an existing catalog to read storage root from (for CREATE CATALOG)
 *
 * Run:
 *   DATABRICKS_SERVER_HOSTNAME=... DATABRICKS_HTTP_PATH=... DATABRICKS_TOKEN=... \
 *   pnpm test -- databricks.integration
 */

const TEST_CATALOG = "dbhub_integration_test";
const SCHEMA_A = "schema_a";
const SCHEMA_B = "schema_b";

const SAMPLE_CATALOG = process.env.DATABRICKS_SAMPLE_CATALOG;

function getDSN(): string | null {
  const host = process.env.DATABRICKS_SERVER_HOSTNAME;
  const path = process.env.DATABRICKS_HTTP_PATH;
  const token = process.env.DATABRICKS_TOKEN;
  if (!host || !path || !token || !SAMPLE_CATALOG) return null;

  const httpPath = path.startsWith("/") ? path.substring(1) : path;
  return `databricks://token:${encodeURIComponent(token)}@${host}/${httpPath}`;
}

const dsn = getDSN();
const describeOrSkip = dsn ? describe : describe.skip;

describeOrSkip("Databricks Connector Integration Tests", () => {
  let connector: Connector;

  // Fully-qualified helper: TEST_CATALOG.schema.table
  const fq = (schema: string, table?: string) =>
    table ? `${TEST_CATALOG}.${schema}.${table}` : `${TEST_CATALOG}.${schema}`;

  beforeAll(async () => {
    connector = new DatabricksConnector();
    await connector.connect(dsn!);

    // --- Create test catalog ---
    // Discover the storage root from DATABRICKS_SAMPLE_CATALOG env var
    const storageResult = await connector.executeSQL(
      `DESCRIBE CATALOG EXTENDED ${SAMPLE_CATALOG}`,
      {}
    );
    const storageRoot = storageResult.rows.find(
      (r: any) => r.info_name === "Storage Root"
    )?.info_value;

    const locationClause = storageRoot ? `MANAGED LOCATION '${storageRoot}/dbhub_test'` : "";

    await connector.executeSQL(
      `CREATE CATALOG IF NOT EXISTS ${TEST_CATALOG} ${locationClause}`,
      {}
    );

    // --- Schema A: users + orders ---
    await connector.executeSQL(`CREATE SCHEMA IF NOT EXISTS ${fq(SCHEMA_A)}`, {});
    await connector.executeSQL(
      `CREATE TABLE IF NOT EXISTS ${fq(SCHEMA_A, "users")} (
        id BIGINT, name STRING NOT NULL, email STRING NOT NULL, age INT
      ) USING DELTA`,
      {}
    );
    await connector.executeSQL(
      `CREATE TABLE IF NOT EXISTS ${fq(SCHEMA_A, "orders")} (
        id BIGINT, user_id BIGINT, amount DECIMAL(10,2), created_at TIMESTAMP
      ) USING DELTA`,
      {}
    );
    await connector.executeSQL(`TRUNCATE TABLE ${fq(SCHEMA_A, "users")}`, {});
    await connector.executeSQL(`TRUNCATE TABLE ${fq(SCHEMA_A, "orders")}`, {});
    await connector.executeSQL(
      `INSERT INTO ${fq(SCHEMA_A, "users")} VALUES
       (1, 'John Doe', 'john@example.com', 30),
       (2, 'Jane Smith', 'jane@example.com', 25),
       (3, 'Bob Johnson', 'bob@example.com', 35)`,
      {}
    );
    await connector.executeSQL(
      `INSERT INTO ${fq(SCHEMA_A, "orders")} VALUES
       (1, 1, 99.99, current_timestamp()),
       (2, 1, 149.50, current_timestamp()),
       (3, 2, 75.25, current_timestamp())`,
      {}
    );

    // --- Schema B: products + reviews ---
    await connector.executeSQL(`CREATE SCHEMA IF NOT EXISTS ${fq(SCHEMA_B)}`, {});
    await connector.executeSQL(
      `CREATE TABLE IF NOT EXISTS ${fq(SCHEMA_B, "products")} (
        id BIGINT, name STRING NOT NULL, price DECIMAL(10,2), category STRING
      ) USING DELTA`,
      {}
    );
    await connector.executeSQL(
      `CREATE TABLE IF NOT EXISTS ${fq(SCHEMA_B, "reviews")} (
        id BIGINT, product_id BIGINT, rating INT, comment STRING
      ) USING DELTA`,
      {}
    );
    await connector.executeSQL(`TRUNCATE TABLE ${fq(SCHEMA_B, "products")}`, {});
    await connector.executeSQL(`TRUNCATE TABLE ${fq(SCHEMA_B, "reviews")}`, {});
    await connector.executeSQL(
      `INSERT INTO ${fq(SCHEMA_B, "products")} VALUES
       (1, 'Widget', 9.99, 'tools'),
       (2, 'Gadget', 24.99, 'electronics'),
       (3, 'Thingamajig', 4.50, 'misc')`,
      {}
    );
    await connector.executeSQL(
      `INSERT INTO ${fq(SCHEMA_B, "reviews")} VALUES
       (1, 1, 5, 'Great product!'),
       (2, 2, 3, 'Decent'),
       (3, 1, 4, 'Good value')`,
      {}
    );
  }, 120_000);

  afterAll(async () => {
    if (connector) {
      try {
        await connector.executeSQL(`DROP CATALOG IF EXISTS ${TEST_CATALOG} CASCADE`, {});
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
      expect(sample).toContain("databricks://");
      expect(connector.dsnParser.isValidDSN(sample)).toBe(true);
      expect(connector.dsnParser.isValidDSN("invalid-dsn")).toBe(false);
    });
  });

  // --- Catalog Operations (three-level namespace: catalog > schema > table) ---

  describe("Catalog Operations", () => {
    it("should list catalogs and include test catalog", async () => {
      const dbConnector = connector as any;
      const catalogs: string[] = await dbConnector.getCatalogs();
      expect(catalogs.length).toBeGreaterThan(0);
      expect(catalogs).toContain(TEST_CATALOG);
    });

    it("should list schemas in the test catalog", async () => {
      const schemas = await connector.getSchemas(TEST_CATALOG);
      expect(schemas).toContain(SCHEMA_A);
      expect(schemas).toContain(SCHEMA_B);
      expect(schemas).not.toContain("information_schema");
    });

    it("should list tables in catalog.schema_a", async () => {
      const tables = await connector.getTables(SCHEMA_A, TEST_CATALOG);
      expect(tables).toContain("users");
      expect(tables).toContain("orders");
      expect(tables).not.toContain("products");
    });

    it("should list tables in catalog.schema_b", async () => {
      const tables = await connector.getTables(SCHEMA_B, TEST_CATALOG);
      expect(tables).toContain("products");
      expect(tables).toContain("reviews");
      expect(tables).not.toContain("users");
    });

    it("should get table schema with explicit catalog", async () => {
      const columns = await connector.getTableSchema("users", SCHEMA_A, TEST_CATALOG);
      expect(columns.length).toBe(4);
      expect(columns.map((c) => c.column_name)).toEqual(
        expect.arrayContaining(["id", "name", "email", "age"])
      );
      const idCol = columns.find((c) => c.column_name === "id");
      expect(idCol?.data_type).toContain("bigint");
    });

    it("should get table schema from schema_b with explicit catalog", async () => {
      const columns = await connector.getTableSchema("products", SCHEMA_B, TEST_CATALOG);
      expect(columns.length).toBe(4);
      expect(columns.map((c) => c.column_name)).toEqual(
        expect.arrayContaining(["id", "name", "price", "category"])
      );
    });

    it("should check table existence with explicit catalog", async () => {
      expect(await connector.tableExists("users", SCHEMA_A, TEST_CATALOG)).toBe(true);
      expect(await connector.tableExists("products", SCHEMA_B, TEST_CATALOG)).toBe(true);
      expect(await connector.tableExists("nonexistent_xyz", SCHEMA_A, TEST_CATALOG)).toBe(false);
      // Cross-schema: users should NOT exist in schema_b
      expect(await connector.tableExists("users", SCHEMA_B, TEST_CATALOG)).toBe(false);
    });

    it("should get table comment with explicit catalog", async () => {
      const comment = await connector.getTableComment!("users", SCHEMA_A, TEST_CATALOG);
      expect(comment === null || typeof comment === "string").toBe(true);
    });
  });

  // --- SQL Execution against the test catalog ---

  describe("SQL Execution", () => {
    it("should SELECT from catalog-qualified table", async () => {
      const result = await connector.executeSQL(
        `SELECT COUNT(*) as count FROM ${fq(SCHEMA_A, "users")}`,
        {}
      );
      expect(result.rows).toHaveLength(1);
      expect(Number(result.rows[0].count)).toBe(3);
    });

    it("should SELECT with WHERE on schema_b", async () => {
      const result = await connector.executeSQL(
        `SELECT name, price FROM ${fq(SCHEMA_B, "products")} WHERE price > 5 ORDER BY name`,
        {}
      );
      expect(result.rows.length).toBe(2);
      expect(result.rows[0].name).toBe("Gadget");
    });

    it("should handle cross-schema JOIN within the test catalog", async () => {
      const result = await connector.executeSQL(
        `SELECT u.name as user_name, p.name as product_name
         FROM ${fq(SCHEMA_A, "users")} u
         CROSS JOIN ${fq(SCHEMA_B, "products")} p
         WHERE u.id = 1 AND p.id = 1`,
        {}
      );
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].user_name).toBe("John Doe");
      expect(result.rows[0].product_name).toBe("Widget");
    });

    it("should respect maxRows limit", async () => {
      const result = await connector.executeSQL(
        `SELECT * FROM ${fq(SCHEMA_A, "users")} ORDER BY id`,
        { maxRows: 2 }
      );
      expect(result.rows).toHaveLength(2);
    });

    it("should execute INSERT and DELETE", async () => {
      await connector.executeSQL(
        `INSERT INTO ${fq(SCHEMA_A, "users")} VALUES (4, 'Test User', 'test@example.com', 28)`,
        {}
      );
      const result = await connector.executeSQL(
        `SELECT * FROM ${fq(SCHEMA_A, "users")} WHERE id = 4`,
        {}
      );
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].name).toBe("Test User");

      await connector.executeSQL(`DELETE FROM ${fq(SCHEMA_A, "users")} WHERE id = 4`, {});
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
      const newConnector = new DatabricksConnector();
      await expect(newConnector.executeSQL("SELECT 1", {})).rejects.toThrow(
        /Not connected to Databricks database/
      );
    });
  });
});
