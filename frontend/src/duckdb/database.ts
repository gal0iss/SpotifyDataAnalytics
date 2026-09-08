import * as duckdb from '@duckdb/duckdb-wasm';
import duckdbWasm from '@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url';
import duckdbWorker from '@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url';

let database: duckdb.AsyncDuckDB | null = null;
let initialization: Promise<void> | null = null;
let connection: duckdb.AsyncDuckDBConnection | null = null;
let queryQueue = Promise.resolve();

const parquetFiles = [
  'fact_table',
  'dim_date',
  'dim_device',
  'dim_track',
  'dim_episode',
  'dim_location',
  'dim_location_enriched',
] as const;

export async function initializeDatabase(): Promise<void> {
  if (initialization) return initialization;

  initialization = (async () => {
    const worker = new Worker(duckdbWorker);
    database = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
    await database.instantiate(duckdbWasm, undefined);
    connection = await database.connect();
    for (const file of parquetFiles) {
      await database.registerFileURL(
        `${file}.parquet`,
        `${import.meta.env.BASE_URL}parquet/${file}.parquet`,
        duckdb.DuckDBDataProtocol.HTTP,
        false,
      );
    }
  })();

  try {
    await initialization;
  } catch (error) {
    initialization = null;
    throw error;
  }
}

export async function query<T>(sql: string): Promise<T[]> {
  const run = queryQueue.then(async () => {
    await initializeDatabase();
    if (!connection) throw new Error('DuckDB connection is unavailable.');
    const result = await connection.query(sql);
    const fields = result.schema.fields.map((field) => field.name);
    return result.toArray().map((row) => {
      const record = row as unknown as Record<string, unknown>;
      return Object.fromEntries(fields.map((field) => [field, normalizeValue(record[field])])) as T;
    });
  });
  queryQueue = run.then(() => undefined, () => undefined);
  return run;
}

function normalizeValue(value: unknown): unknown {
  if (typeof value === 'bigint') return Number(value);
  if (Array.isArray(value)) return value.map(normalizeValue);
  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.entries(value).map(([key, nested]) => [key, normalizeValue(nested)]));
  }
  return value;
}
