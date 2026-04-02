import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { BigQueryConnector } from '../bigquery/index.js';

// Mock @google-cloud/bigquery
const mockQuery = vi.fn();
const mockGetDatasets = vi.fn();
const mockGetTables = vi.fn();
const mockGetMetadata = vi.fn();
const mockDataset = vi.fn();
const mockTable = vi.fn();

vi.mock('@google-cloud/bigquery', () => {
  return {
    BigQuery: class MockBigQuery {
      constructor(public options?: any) {}
      async query(...args: any[]) { return mockQuery(...args); }
      getDatasets(...args: any[]) { return mockGetDatasets(...args); }
      dataset(id: string) { return mockDataset(id); }
    }
  };
});

function resetMocks() {
  vi.clearAllMocks();
  mockQuery.mockResolvedValue([[], null, {}]);
  mockGetDatasets.mockResolvedValue([[]]);
  mockGetTables.mockResolvedValue([[]]);
  mockGetMetadata.mockResolvedValue([{}]);
  mockDataset.mockReturnValue({
    getTables: mockGetTables,
    table: mockTable,
  });
  mockTable.mockReturnValue({
    getMetadata: mockGetMetadata,
  });
}

describe('BigQuery Connector', () => {
  let connector: BigQueryConnector;

  beforeEach(() => {
    connector = new BigQueryConnector();
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
    const parser = new BigQueryConnector().dsnParser;

    it('should validate BigQuery DSN format', () => {
      expect(parser.isValidDSN('bigquery://my-project/my_dataset')).toBe(true);
      expect(parser.isValidDSN('bigquery://my-project')).toBe(true);
      expect(parser.isValidDSN('postgres://user:pass@localhost:5432/db')).toBe(false);
      expect(parser.isValidDSN('invalid-dsn')).toBe(false);
      expect(parser.isValidDSN('')).toBe(false);
    });

    it('should parse basic DSN with project and dataset', async () => {
      const config = await parser.parse('bigquery://my-gcp-project/analytics_dataset');
      expect(config.projectId).toBe('my-gcp-project');
      expect(config.datasetId).toBe('analytics_dataset');
      expect(config.keyFilename).toBeUndefined();
      expect(config.location).toBeUndefined();
    });

    it('should parse DSN with project only', async () => {
      const config = await parser.parse('bigquery://my-gcp-project');
      expect(config.projectId).toBe('my-gcp-project');
      expect(config.datasetId).toBeUndefined();
    });

    it('should parse DSN with keyFile query param', async () => {
      const config = await parser.parse(
        'bigquery://my-project/dataset?keyFile=/path/to/sa-key.json'
      );
      expect(config.projectId).toBe('my-project');
      expect(config.datasetId).toBe('dataset');
      expect(config.keyFilename).toBe('/path/to/sa-key.json');
    });

    it('should parse DSN with location query param', async () => {
      const config = await parser.parse('bigquery://my-project?location=US');
      expect(config.projectId).toBe('my-project');
      expect(config.location).toBe('US');
    });

    it('should parse DSN with all options', async () => {
      const config = await parser.parse(
        'bigquery://my-project/my_dataset?keyFile=/sa.json&location=EU'
      );
      expect(config.projectId).toBe('my-project');
      expect(config.datasetId).toBe('my_dataset');
      expect(config.keyFilename).toBe('/sa.json');
      expect(config.location).toBe('EU');
    });

    it('should throw on invalid DSN format', async () => {
      await expect(parser.parse('postgres://user:pass@host/db')).rejects.toThrow(
        'Invalid BigQuery DSN format'
      );
    });

    it('should throw when project ID is missing', async () => {
      await expect(parser.parse('bigquery://')).rejects.toThrow(
        'Project ID'
      );
    });

    it('should return a sample DSN', () => {
      const sample = parser.getSampleDSN();
      expect(sample).toContain('bigquery://');
      expect(parser.isValidDSN(sample)).toBe(true);
    });
  });

  describe('Connection', () => {
    const dsn = 'bigquery://my-project/my_dataset';

    it('should connect successfully', async () => {
      await connector.connect(dsn);
      // BigQuery client is created during connect — verify no errors
    });

    it('should connect with keyFile', async () => {
      await connector.connect('bigquery://my-project/ds?keyFile=/path/to/key.json');
      // Should not throw
    });

    it('should disconnect cleanly', async () => {
      await connector.connect(dsn);
      await connector.disconnect();
      // After disconnect, operations should throw
      await expect(connector.getSchemas()).rejects.toThrow('Not connected');
    });

    it('should have correct connector metadata', () => {
      expect(connector.id).toBe('bigquery');
      expect(connector.name).toBe('BigQuery');
    });

    it('should clone correctly', () => {
      const cloned = connector.clone();
      expect(cloned).toBeInstanceOf(BigQueryConnector);
      expect(cloned).not.toBe(connector);
    });
  });

  describe('Catalog Operations (Project)', () => {
    const dsn = 'bigquery://my-project/default_dataset';

    beforeEach(async () => {
      await connector.connect(dsn);
    });

    it('should return connected project as catalog', async () => {
      const catalogs = await connector.getCatalogs!();
      expect(catalogs).toEqual(['my-project']);
    });

    it('should throw on getCatalogs when not connected', async () => {
      await connector.disconnect();
      await expect(connector.getCatalogs!()).rejects.toThrow('Not connected to BigQuery');
    });
  });

  describe('Schema Operations', () => {
    const dsn = 'bigquery://my-project/default_dataset';

    beforeEach(async () => {
      await connector.connect(dsn);
    });

    it('should list datasets as schemas', async () => {
      mockGetDatasets.mockResolvedValueOnce([
        [{ id: 'analytics' }, { id: 'staging' }, { id: 'production' }]
      ]);

      const schemas = await connector.getSchemas();
      expect(schemas).toEqual(['analytics', 'production', 'staging']); // sorted
    });

    it('should list tables in default dataset', async () => {
      mockGetTables.mockResolvedValueOnce([
        [{ id: 'users' }, { id: 'orders' }]
      ]);

      const tables = await connector.getTables();
      expect(tables).toEqual(['orders', 'users']); // sorted
      expect(mockDataset).toHaveBeenCalledWith('default_dataset');
    });

    it('should list tables in specified dataset', async () => {
      mockGetTables.mockResolvedValueOnce([
        [{ id: 'events' }]
      ]);

      const tables = await connector.getTables('analytics');
      expect(tables).toEqual(['events']);
      expect(mockDataset).toHaveBeenCalledWith('analytics');
    });

    it('should throw when no dataset specified and no default', async () => {
      // Connect without default dataset
      await connector.disconnect();
      await connector.connect('bigquery://my-project');

      await expect(connector.getTables()).rejects.toThrow('Dataset name is required');
    });

    it('should check if table exists', async () => {
      mockGetTables.mockResolvedValueOnce([
        [{ id: 'users' }, { id: 'orders' }]
      ]);
      expect(await connector.tableExists('users')).toBe(true);

      mockGetTables.mockResolvedValueOnce([[]]);
      expect(await connector.tableExists('nonexistent')).toBe(false);
    });

    it('should get table schema from metadata', async () => {
      mockGetMetadata.mockResolvedValueOnce([{
        schema: {
          fields: [
            { name: 'id', type: 'INT64', mode: 'REQUIRED', description: 'Primary key' },
            { name: 'name', type: 'STRING', mode: 'NULLABLE', description: null },
            { name: 'age', type: 'INT64', mode: 'NULLABLE', description: null },
          ]
        }
      }]);

      const schema = await connector.getTableSchema('users');
      expect(schema).toHaveLength(3);
      expect(schema[0]).toEqual({
        column_name: 'id',
        data_type: 'INT64',
        is_nullable: 'NO',
        column_default: null,
        description: 'Primary key',
      });
      expect(schema[1].is_nullable).toBe('YES');
    });

    it('should return empty array for table indexes', async () => {
      const indexes = await connector.getTableIndexes('users');
      expect(indexes).toEqual([]);
    });

    it('should get table comment from metadata', async () => {
      mockGetMetadata.mockResolvedValueOnce([{
        description: 'Application users table'
      }]);

      const comment = await connector.getTableComment('users');
      expect(comment).toBe('Application users table');
    });

    it('should return null for table without comment', async () => {
      mockGetMetadata.mockResolvedValueOnce([{}]);

      const comment = await connector.getTableComment('users');
      expect(comment).toBeNull();
    });

    it('should get table row count from metadata', async () => {
      mockGetMetadata.mockResolvedValueOnce([{
        numRows: '42'
      }]);

      const count = await connector.getTableRowCount('users');
      expect(count).toBe(42);
    });
  });

  describe('Stored Procedures / Functions', () => {
    const dsn = 'bigquery://my-project/my_dataset';

    beforeEach(async () => {
      await connector.connect(dsn);
    });

    it('should list routines via INFORMATION_SCHEMA', async () => {
      mockQuery.mockResolvedValueOnce([
        [{ routine_name: 'calculate_total' }, { routine_name: 'format_name' }],
        null,
        {}
      ]);

      const procs = await connector.getStoredProcedures();
      expect(procs).toEqual(['calculate_total', 'format_name']);
    });

    it('should return empty array if INFORMATION_SCHEMA query fails', async () => {
      mockQuery.mockRejectedValueOnce(new Error('NOT_FOUND'));

      const procs = await connector.getStoredProcedures();
      expect(procs).toEqual([]);
    });

    it('should get stored procedure detail', async () => {
      mockQuery.mockResolvedValueOnce([
        [{ routine_name: 'calculate_total', routine_type: 'FUNCTION', routine_definition: 'SELECT x + y', data_type: 'INT64' }],
        null,
        {}
      ]);

      const detail = await connector.getStoredProcedureDetail('calculate_total');
      expect(detail.procedure_name).toBe('calculate_total');
      expect(detail.procedure_type).toBe('function');
      expect(detail.return_type).toBe('INT64');
      expect(detail.definition).toBe('SELECT x + y');
    });

    it('should throw when routine not found', async () => {
      mockQuery.mockResolvedValueOnce([[], null, {}]);

      await expect(connector.getStoredProcedureDetail('nonexistent'))
        .rejects.toThrow("Failed to get details");
    });
  });

  describe('SQL Execution', () => {
    const dsn = 'bigquery://my-project/my_dataset';

    beforeEach(async () => {
      await connector.connect(dsn);
    });

    it('should execute SELECT query', async () => {
      mockQuery.mockResolvedValueOnce([
        [{ id: 1, name: 'Alice' }, { id: 2, name: 'Bob' }],
        null,
        {}
      ]);

      const result = await connector.executeSQL('SELECT * FROM users', {});
      expect(result.rows).toHaveLength(2);
      expect(result.rowCount).toBe(2);
      expect(result.rows[0].name).toBe('Alice');
    });

    it('should apply maxRows limit', async () => {
      mockQuery.mockResolvedValueOnce([[{ id: 1 }], null, {}]);

      await connector.executeSQL('SELECT * FROM users', { maxRows: 10 });

      expect(mockQuery).toHaveBeenCalledWith(
        expect.objectContaining({
          query: expect.stringContaining('LIMIT 10'),
        })
      );
    });

    it('should handle DML with numDmlAffectedRows', async () => {
      mockQuery.mockResolvedValueOnce([
        [],
        null,
        { statistics: { query: { numDmlAffectedRows: '5' } } }
      ]);

      const result = await connector.executeSQL("UPDATE users SET age = 30 WHERE id = 1", {});
      expect(result.rows).toHaveLength(0);
      expect(result.rowCount).toBe(5);
    });

    it('should handle DDL without rows', async () => {
      mockQuery.mockResolvedValueOnce([[], null, {}]);

      const result = await connector.executeSQL('CREATE TABLE test (id INT64)', {});
      expect(result.rows).toHaveLength(0);
      expect(result.rowCount).toBe(0);
    });

    it('should handle multi-statement queries', async () => {
      // First statement (DML)
      mockQuery.mockResolvedValueOnce([
        [],
        null,
        { statistics: { query: { numDmlAffectedRows: '1' } } }
      ]);
      // Second statement (SELECT)
      mockQuery.mockResolvedValueOnce([
        [{ count: 5 }],
        null,
        {}
      ]);

      const result = await connector.executeSQL(
        "INSERT INTO users (name) VALUES ('test'); SELECT COUNT(*) as count FROM users",
        {}
      );

      expect(mockQuery).toHaveBeenCalledTimes(2);
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

    it('should pass positional parameters', async () => {
      mockQuery.mockResolvedValueOnce([[{ id: 1 }], null, {}]);

      await connector.executeSQL('SELECT * FROM users WHERE id = ?', {}, [42]);

      expect(mockQuery).toHaveBeenCalledWith(
        expect.objectContaining({
          params: [42],
        })
      );
    });

    it('should throw when not connected', async () => {
      await connector.disconnect();

      await expect(
        connector.executeSQL('SELECT 1', {})
      ).rejects.toThrow('Not connected to BigQuery');
    });

    it('should throw on query error', async () => {
      mockQuery.mockRejectedValueOnce(new Error('SYNTAX_ERROR'));

      await expect(
        connector.executeSQL('INVALID SQL', {})
      ).rejects.toThrow('SYNTAX_ERROR');
    });
  });

  describe('Error Handling', () => {
    it('should throw on getSchemas when not connected', async () => {
      await expect(connector.getSchemas()).rejects.toThrow('Not connected to BigQuery');
    });

    it('should throw on getTables when not connected', async () => {
      await expect(connector.getTables()).rejects.toThrow('Not connected to BigQuery');
    });

    it('should throw on getTableSchema when not connected', async () => {
      await expect(connector.getTableSchema('users')).rejects.toThrow('Not connected to BigQuery');
    });

    it('should throw on tableExists when not connected', async () => {
      await expect(connector.tableExists('users')).rejects.toThrow('Not connected to BigQuery');
    });

    it('should throw on getStoredProcedures when not connected', async () => {
      await expect(connector.getStoredProcedures()).rejects.toThrow('Not connected to BigQuery');
    });

    it('should throw on getStoredProcedureDetail when not connected', async () => {
      await expect(connector.getStoredProcedureDetail('fn')).rejects.toThrow('Not connected to BigQuery');
    });

    it('should throw on getTableComment when not connected', async () => {
      await expect(connector.getTableComment('users')).rejects.toThrow('Not connected to BigQuery');
    });
  });

  describe('Default Dataset from DSN', () => {
    it('should use default dataset when provided in DSN', async () => {
      await connector.connect('bigquery://my-project/analytics_ds');
      mockGetTables.mockResolvedValueOnce([[{ id: 't1' }]]);

      await connector.getTables();
      expect(mockDataset).toHaveBeenCalledWith('analytics_ds');
    });

    it('should require explicit dataset when none in DSN', async () => {
      await connector.connect('bigquery://my-project');

      await expect(connector.getTables()).rejects.toThrow('Dataset name is required');
    });

    it('should allow explicit dataset override', async () => {
      await connector.connect('bigquery://my-project/default_ds');
      mockGetTables.mockResolvedValueOnce([[{ id: 't1' }]]);

      await connector.getTables('other_ds');
      expect(mockDataset).toHaveBeenCalledWith('other_ds');
    });
  });
});
