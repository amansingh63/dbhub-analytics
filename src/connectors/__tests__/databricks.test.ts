import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { DatabricksConnector } from '../databricks/index.js';

// Mock @databricks/sql using a class-based approach for proper `new` support
const mockFetchAll = vi.fn();
const mockOperationClose = vi.fn();
const mockExecuteStatement = vi.fn();
const mockSessionClose = vi.fn();
const mockOpenSession = vi.fn();
const mockConnect = vi.fn();
const mockClientClose = vi.fn();

vi.mock('@databricks/sql', () => {
  return {
    DBSQLClient: class MockDBSQLClient {
      async connect(...args: any[]) { return mockConnect(...args); }
      async openSession(...args: any[]) { return mockOpenSession(...args); }
      async close(...args: any[]) { return mockClientClose(...args); }
    }
  };
});

function resetMocks() {
  vi.clearAllMocks();
  mockFetchAll.mockResolvedValue([]);
  mockOperationClose.mockResolvedValue(undefined);
  mockExecuteStatement.mockResolvedValue({
    fetchAll: mockFetchAll,
    close: mockOperationClose,
  });
  mockSessionClose.mockResolvedValue(undefined);
  mockOpenSession.mockResolvedValue({
    executeStatement: mockExecuteStatement,
    close: mockSessionClose,
  });
  mockConnect.mockResolvedValue(undefined);
  mockClientClose.mockResolvedValue(undefined);
}

describe('Databricks Connector', () => {
  let connector: DatabricksConnector;

  beforeEach(() => {
    connector = new DatabricksConnector();
    resetMocks();
  });

  afterEach(async () => {
    try {
      await connector.disconnect();
    } catch {
      // ignore
    }
  });

  describe('DSN Parser', () => {
    const parser = new DatabricksConnector().dsnParser;

    it('should validate Databricks DSN format', () => {
      expect(parser.isValidDSN('databricks://token:dapi123@host/sql/2.0/warehouses/abc')).toBe(true);
      expect(parser.isValidDSN('postgres://user:pass@localhost:5432/db')).toBe(false);
      expect(parser.isValidDSN('invalid-dsn')).toBe(false);
      expect(parser.isValidDSN('')).toBe(false);
    });

    it('should parse basic DSN', async () => {
      const config = await parser.parse(
        'databricks://token:dapi1234567890@adb-123.azuredatabricks.net/sql/2.0/warehouses/abc123'
      );
      expect(config.host).toBe('adb-123.azuredatabricks.net');
      expect(config.port).toBe(443);
      expect(config.path).toBe('/sql/2.0/warehouses/abc123');
      expect(config.token).toBe('dapi1234567890');
      expect(config.catalog).toBeUndefined();
      expect(config.schema).toBeUndefined();
    });

    it('should parse DSN with custom port', async () => {
      const config = await parser.parse(
        'databricks://token:dapi123@host:8443/sql/2.0/warehouses/abc'
      );
      expect(config.port).toBe(8443);
    });

    it('should parse DSN with catalog and schema query params', async () => {
      const config = await parser.parse(
        'databricks://token:dapi123@host/sql/2.0/warehouses/abc?catalog=main&schema=myschema'
      );
      expect(config.catalog).toBe('main');
      expect(config.schema).toBe('myschema');
    });

    it('should parse DSN with encoded token', async () => {
      const config = await parser.parse(
        'databricks://token:dapi%2B%2Fspecial@host/sql/2.0/warehouses/abc'
      );
      expect(config.token).toBe('dapi+/special');
    });

    it('should throw on invalid DSN format', async () => {
      await expect(parser.parse('postgres://user:pass@host/db')).rejects.toThrow(
        'Invalid Databricks DSN format'
      );
    });

    it('should throw when token is missing', async () => {
      await expect(parser.parse('databricks://token@host/sql/2.0/warehouses/abc')).rejects.toThrow(
        'Access token'
      );
    });

    it('should throw when HTTP path is missing', async () => {
      await expect(parser.parse('databricks://token:dapi123@host/')).rejects.toThrow(
        'HTTP path'
      );
    });

    it('should return a sample DSN', () => {
      const sample = parser.getSampleDSN();
      expect(sample).toContain('databricks://');
      expect(parser.isValidDSN(sample)).toBe(true);
    });
  });

  describe('Connection', () => {
    const dsn = 'databricks://token:dapi123@adb-test.azuredatabricks.net/sql/2.0/warehouses/test123';

    it('should connect successfully', async () => {
      await connector.connect(dsn);

      expect(mockConnect).toHaveBeenCalledWith(
        expect.objectContaining({
          host: 'adb-test.azuredatabricks.net',
          path: '/sql/2.0/warehouses/test123',
          token: 'dapi123',
        })
      );
      expect(mockOpenSession).toHaveBeenCalled();
    });

    it('should pass catalog and schema to session', async () => {
      await connector.connect(
        'databricks://token:dapi123@host/sql/2.0/warehouses/abc?catalog=prod&schema=analytics'
      );

      expect(mockOpenSession).toHaveBeenCalledWith(
        expect.objectContaining({
          initialCatalog: 'prod',
          initialSchema: 'analytics',
        })
      );
    });

    it('should pass connection timeout', async () => {
      await connector.connect(dsn, undefined, { connectionTimeoutSeconds: 30 });

      expect(mockConnect).toHaveBeenCalledWith(
        expect.objectContaining({
          socketTimeout: 30000,
        })
      );
    });

    it('should disconnect cleanly', async () => {
      await connector.connect(dsn);
      await connector.disconnect();

      expect(mockSessionClose).toHaveBeenCalled();
      expect(mockClientClose).toHaveBeenCalled();
    });

    it('should handle connection errors', async () => {
      mockConnect.mockRejectedValueOnce(new Error('Connection refused'));

      await expect(connector.connect(dsn)).rejects.toThrow('Connection refused');
    });

    it('should have correct connector metadata', () => {
      expect(connector.id).toBe('databricks');
      expect(connector.name).toBe('Databricks');
    });

    it('should clone correctly', () => {
      const cloned = connector.clone();
      expect(cloned).toBeInstanceOf(DatabricksConnector);
      expect(cloned).not.toBe(connector);
    });
  });

  describe('Schema Operations', () => {
    const dsn = 'databricks://token:dapi123@host/sql/2.0/warehouses/abc';

    beforeEach(async () => {
      await connector.connect(dsn);
    });

    it('should list schemas', async () => {
      // SHOW SCHEMAS returns databaseName column
      mockFetchAll.mockResolvedValueOnce([
        { databaseName: 'default' },
        { databaseName: 'analytics' },
        { databaseName: 'staging' },
      ]);

      const schemas = await connector.getSchemas();
      expect(schemas).toEqual(['analytics', 'default', 'staging']); // sorted
      expect(mockExecuteStatement).toHaveBeenCalledWith(
        'SHOW SCHEMAS',
        expect.anything()
      );
    });

    it('should list schemas in a specific catalog', async () => {
      mockFetchAll.mockResolvedValueOnce([
        { databaseName: 'bronze' },
        { databaseName: 'silver' },
        { databaseName: 'information_schema' },
      ]);

      const schemas = await connector.getSchemas('my_catalog');
      expect(schemas).toEqual(['bronze', 'silver']); // information_schema filtered out
      expect(mockExecuteStatement).toHaveBeenCalledWith(
        'SHOW SCHEMAS IN `my_catalog`',
        expect.anything()
      );
    });

    it('should list tables in default schema', async () => {
      mockFetchAll.mockResolvedValueOnce([
        { tableName: 'users', database: 'default', isTemporary: false },
        { tableName: 'orders', database: 'default', isTemporary: false },
      ]);

      const tables = await connector.getTables();
      expect(tables).toEqual(['orders', 'users']); // sorted
      expect(mockExecuteStatement).toHaveBeenCalledWith(
        'SHOW TABLES IN `default`',
        expect.anything()
      );
    });

    it('should list tables in specified schema', async () => {
      mockFetchAll.mockResolvedValueOnce([
        { tableName: 'events', database: 'analytics', isTemporary: false },
      ]);

      const tables = await connector.getTables('analytics');
      expect(tables).toEqual(['events']);
      expect(mockExecuteStatement).toHaveBeenCalledWith(
        'SHOW TABLES IN `analytics`',
        expect.anything()
      );
    });

    it('should list tables with explicit catalog', async () => {
      mockFetchAll.mockResolvedValueOnce([
        { tableName: 'fact_orders', database: 'gold', isTemporary: false },
      ]);

      const tables = await connector.getTables('gold', 'my_catalog');
      expect(tables).toEqual(['fact_orders']);
      expect(mockExecuteStatement).toHaveBeenCalledWith(
        'SHOW TABLES IN `my_catalog`.`gold`',
        expect.anything()
      );
    });

    it('should check if table exists', async () => {
      // tableExists uses getTables internally
      mockFetchAll.mockResolvedValueOnce([
        { tableName: 'users', database: 'default', isTemporary: false },
        { tableName: 'orders', database: 'default', isTemporary: false },
      ]);
      expect(await connector.tableExists('users')).toBe(true);

      mockFetchAll.mockResolvedValueOnce([]);
      expect(await connector.tableExists('nonexistent')).toBe(false);
    });

    it('should get table schema via DESCRIBE TABLE', async () => {
      mockFetchAll.mockResolvedValueOnce([
        { col_name: 'id', data_type: 'bigint', comment: 'Primary key' },
        { col_name: 'name', data_type: 'string', comment: null },
        { col_name: 'age', data_type: 'int', comment: null },
      ]);

      const schema = await connector.getTableSchema('users');
      expect(schema).toHaveLength(3);
      expect(schema[0]).toEqual({
        column_name: 'id',
        data_type: 'bigint',
        is_nullable: 'YES',
        column_default: null,
        description: 'Primary key',
      });
      expect(mockExecuteStatement).toHaveBeenCalledWith(
        'DESCRIBE TABLE `default`.`users`',
        expect.anything()
      );
    });

    it('should filter out metadata rows from DESCRIBE TABLE', async () => {
      mockFetchAll.mockResolvedValueOnce([
        { col_name: 'id', data_type: 'bigint', comment: null },
        { col_name: '# Clustering Information', data_type: '', comment: '' },
        { col_name: '# col_name', data_type: 'data_type', comment: 'comment' },
        { col_name: 'id', data_type: 'bigint', comment: null },
      ]);

      const schema = await connector.getTableSchema('users');
      // Should filter out rows starting with # and empty data_type
      expect(schema.every(c => !c.column_name.startsWith('#'))).toBe(true);
    });

    it('should return empty array for table indexes', async () => {
      const indexes = await connector.getTableIndexes('users');
      expect(indexes).toEqual([]);
    });

    it('should get table comment via DESCRIBE TABLE EXTENDED', async () => {
      mockFetchAll.mockResolvedValueOnce([
        { col_name: 'id', data_type: 'bigint', comment: null },
        { col_name: '', data_type: '', comment: '' },
        { col_name: '# Detailed Table Information', data_type: '', comment: '' },
        { col_name: 'Comment', data_type: 'Application users table', comment: '' },
      ]);
      const comment = await connector.getTableComment('users');
      expect(comment).toBe('Application users table');
      expect(mockExecuteStatement).toHaveBeenCalledWith(
        'DESCRIBE TABLE EXTENDED `default`.`users`',
        expect.anything()
      );
    });

    it('should return null for table without comment', async () => {
      mockFetchAll.mockResolvedValueOnce([
        { col_name: 'id', data_type: 'bigint', comment: null },
      ]);
      const comment = await connector.getTableComment('users');
      expect(comment).toBeNull();
    });
  });

  describe('Stored Procedures / Functions', () => {
    const dsn = 'databricks://token:dapi123@host/sql/2.0/warehouses/abc';

    beforeEach(async () => {
      await connector.connect(dsn);
    });

    it('should list stored procedures via SHOW FUNCTIONS', async () => {
      mockFetchAll.mockResolvedValueOnce([
        { function: 'calculate_total' },
        { function: 'format_name' },
      ]);

      const procs = await connector.getStoredProcedures();
      expect(procs).toEqual(['calculate_total', 'format_name']);
      expect(mockExecuteStatement).toHaveBeenCalledWith(
        'SHOW FUNCTIONS IN `default`',
        expect.anything()
      );
    });

    it('should return empty array if SHOW FUNCTIONS fails', async () => {
      mockExecuteStatement.mockRejectedValueOnce(new Error('TABLE_OR_VIEW_NOT_FOUND'));

      const procs = await connector.getStoredProcedures();
      expect(procs).toEqual([]);
    });

    it('should get stored procedure detail via DESCRIBE FUNCTION', async () => {
      mockFetchAll.mockResolvedValueOnce([
        { function_desc: 'Function: default.calculate_total' },
        { function_desc: 'Returns: INT' },
        { function_desc: 'Body: SELECT x + y' },
      ]);

      const detail = await connector.getStoredProcedureDetail('calculate_total');
      expect(detail.procedure_name).toBe('calculate_total');
      expect(detail.procedure_type).toBe('function');
      expect(detail.definition).toContain('calculate_total');
      expect(mockExecuteStatement).toHaveBeenCalledWith(
        'DESCRIBE FUNCTION `default`.`calculate_total`',
        expect.anything()
      );
    });

    it('should throw when DESCRIBE FUNCTION fails', async () => {
      mockExecuteStatement.mockRejectedValueOnce(new Error('FUNCTION_NOT_FOUND'));

      await expect(connector.getStoredProcedureDetail('nonexistent'))
        .rejects.toThrow("Failed to get details");
    });
  });

  describe('SQL Execution', () => {
    const dsn = 'databricks://token:dapi123@host/sql/2.0/warehouses/abc';

    beforeEach(async () => {
      await connector.connect(dsn);
    });

    it('should execute SELECT query', async () => {
      mockFetchAll.mockResolvedValueOnce([
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
      ]);

      const result = await connector.executeSQL('SELECT * FROM users', {});
      expect(result.rows).toHaveLength(2);
      expect(result.rowCount).toBe(2);
      expect(result.rows[0].name).toBe('Alice');
    });

    it('should apply maxRows limit', async () => {
      mockFetchAll.mockResolvedValueOnce([{ id: 1 }]);

      await connector.executeSQL('SELECT * FROM users', { maxRows: 10 });

      expect(mockExecuteStatement).toHaveBeenCalledWith(
        expect.stringContaining('LIMIT 10'),
        expect.anything()
      );
    });

    it('should handle DML with num_affected_rows', async () => {
      mockFetchAll.mockResolvedValueOnce([{ num_affected_rows: 5 }]);

      const result = await connector.executeSQL("UPDATE users SET age = 30 WHERE id = 1", {});
      expect(result.rows).toHaveLength(0);
      expect(result.rowCount).toBe(5);
    });

    it('should handle DML without rows', async () => {
      mockFetchAll.mockResolvedValueOnce([]);

      const result = await connector.executeSQL('CREATE TABLE test (id INT)', {});
      expect(result.rows).toHaveLength(0);
      expect(result.rowCount).toBe(0);
    });

    it('should handle multi-statement queries', async () => {
      // First statement (INSERT)
      mockFetchAll.mockResolvedValueOnce([{ num_affected_rows: 1 }]);
      // Second statement (SELECT)
      mockFetchAll.mockResolvedValueOnce([{ count: 5 }]);

      const result = await connector.executeSQL(
        "INSERT INTO users (name) VALUES ('test'); SELECT COUNT(*) as count FROM users",
        {}
      );

      expect(mockExecuteStatement).toHaveBeenCalledTimes(2);
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].count).toBe(5);
    });

    it('should reject parameters in multi-statement queries', async () => {
      await expect(
        connector.executeSQL(
          "INSERT INTO t VALUES (?); SELECT 1",
          {},
          ['test']
        )
      ).rejects.toThrow('Parameters are not supported for multi-statement queries');
    });

    it('should pass ordinal parameters', async () => {
      mockFetchAll.mockResolvedValueOnce([{ id: 1 }]);

      await connector.executeSQL('SELECT * FROM users WHERE id = ?', {}, [42]);

      expect(mockExecuteStatement).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({ ordinalParameters: [42] })
      );
    });

    it('should throw when not connected', async () => {
      await connector.disconnect();

      await expect(
        connector.executeSQL('SELECT 1', {})
      ).rejects.toThrow('Not connected to Databricks database');
    });

    it('should throw on query error', async () => {
      mockExecuteStatement.mockRejectedValueOnce(new Error('SYNTAX_ERROR'));

      await expect(
        connector.executeSQL('INVALID SQL', {})
      ).rejects.toThrow('SYNTAX_ERROR');
    });
  });

  describe('Error Handling', () => {
    it('should throw on getSchemas when not connected', async () => {
      await expect(connector.getSchemas()).rejects.toThrow('Not connected to Databricks database');
    });

    it('should throw on getTables when not connected', async () => {
      await expect(connector.getTables()).rejects.toThrow('Not connected to Databricks database');
    });

    it('should throw on getTableSchema when not connected', async () => {
      await expect(connector.getTableSchema('users')).rejects.toThrow('Not connected to Databricks database');
    });

    it('should throw on tableExists when not connected', async () => {
      await expect(connector.tableExists('users')).rejects.toThrow('Not connected to Databricks database');
    });

    it('should throw on getStoredProcedures when not connected', async () => {
      await expect(connector.getStoredProcedures()).rejects.toThrow('Not connected to Databricks database');
    });

    it('should throw on getStoredProcedureDetail when not connected', async () => {
      await expect(connector.getStoredProcedureDetail('fn')).rejects.toThrow('Not connected to Databricks database');
    });

    it('should throw on getTableComment when not connected', async () => {
      await expect(connector.getTableComment('users')).rejects.toThrow('Not connected to Databricks database');
    });
  });

  describe('Default Schema from DSN', () => {
    it('should use "default" when no schema in DSN', async () => {
      await connector.connect('databricks://token:dapi123@host/sql/2.0/warehouses/abc');
      mockFetchAll.mockResolvedValueOnce([{ tableName: 't1' }]);

      await connector.getTables();
      expect(mockExecuteStatement).toHaveBeenCalledWith(
        'SHOW TABLES IN `default`',
        expect.anything()
      );
    });

    it('should use schema from DSN when provided', async () => {
      await connector.connect(
        'databricks://token:dapi123@host/sql/2.0/warehouses/abc?schema=analytics'
      );
      mockFetchAll.mockResolvedValueOnce([{ tableName: 't1' }]);

      await connector.getTables();
      expect(mockExecuteStatement).toHaveBeenCalledWith(
        'SHOW TABLES IN `analytics`',
        expect.anything()
      );
    });
  });
});
