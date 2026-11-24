const assert = require('node:assert/strict');
const { before, after, test } = require('node:test');
const duckdb = require('@duckdb/duckdb-wasm/blocking');
const React = require('react');
const { renderToStaticMarkup } = require('react-dom/server');
const { sourceLoader } = require('./load-source.cjs');

const databaseMock = { query: () => { throw new Error('Unexpected browser query'); } };
const { SQL } = sourceLoader({ '../duckdb/database': databaseMock })('data/queries.ts');
let db;
let connection;
function query(sql) {
  // Substitute only storage reads; execute the production SQL against synthetic tables.
  const fixtureSql = sql.replace(/read_parquet\('(fact_table|dim_date|dim_device|dim_track)\.parquet'\)/g, '$1');
  return connection.query(fixtureSql).toArray().map((row) =>
    JSON.parse(JSON.stringify(row.toJSON(), (_, value) =>
      typeof value === 'bigint' ? Number(value) : value)));
}

before(async () => {
  db = await duckdb.createDuckDB({
    mvp: { mainModule: require.resolve('@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm') },
    eh: { mainModule: require.resolve('@duckdb/duckdb-wasm/dist/duckdb-eh.wasm') },
  }, new duckdb.VoidLogger(), duckdb.NODE_RUNTIME);
  await db.instantiate();
  connection = db.connect();
  const tables = {
    dim_date: `SELECT '2024010100' AS date_id, 2024 AS year, 1 AS month,
      1 AS day, 'Monday' AS weekday, 0 AS hour`,
    dim_device: `SELECT 1 AS device_id, 'desktop' AS device_type`,
    dim_track: `SELECT * FROM (VALUES
      (1, 'Same title', 'Zero Artist', 'Shared album'),
      (2, 'Same title', 'Zero Artist', 'Shared album'),
      (3, 'Winner', 'Other Artist', 'Shared album'),
      (4, 'Same title', 'Other Artist', 'Shared album')
    ) AS t(track_id, track_name, artist_name, album_name)`,
    fact_table: `SELECT track_id, '2024010100' AS date_id, 1 AS device_id,
      ms_played, false AS skipped, false AS shuffle, false AS offline
      FROM (VALUES (1, 0), (1, 0), (2, 0), (2, 0),
        (3, 60000), (3, 60000), (3, 60000), (4, 60000)) AS f(track_id, ms_played)`,
  };
  for (const [name, select] of Object.entries(tables)) {
    connection.query(`CREATE TABLE ${name} AS ${select}`);
  }
});

after(() => { connection?.close(); db?.reset(); });

test('track rankings keep homonyms separate by identity', () => {
  const rows = query(SQL.tracks('all', 'all'));
  assert.equal(rows.length, 4);
  assert.deepEqual(rows.map((row) => row.events), [3, 2, 2, 1]);
  assert.equal(new Set(rows.map((row) => row.trackId)).size, 4);
});

test('discovery selects one track while preserving album-name metrics', () => {
  const [row] = query(SQL.discovery('all', 'all'));
  assert.equal(row.topTrack, 'Winner');
  assert.equal(row.topTrackEvents, 3);
  assert.equal(row.uniqueAlbums, 1);
  assert.equal(row.topAlbumEvents, 8);
});

test('artist catalog carries distinct identities for same-title tracks', () => {
  const rows = query(SQL.artistCatalog('all', 'all', 'Zero Artist'));
  assert.deepEqual(rows.filter((row) => row.kind === 'track').map((row) => row.trackId), [1, 2]);
  assert.equal(rows.filter((row) => row.kind === 'album').length, 1);
  const { rowKeys } = renderArtist('Zero Artist');
  assert.equal(rowKeys.length, 3);
  assert.equal(new Set(rowKeys).size, 3);
});

function renderArtist(artist, year = 'all') {
  const responses = [
    query(SQL.artistSummary(year, 'all', artist))[0],
    query(SQL.artistTimeline(year, 'all', artist)),
    query(SQL.artistCatalog(year, 'all', artist)),
    query(SQL.artistPatterns(year, 'all', artist)),
    [],
  ];
  let index = 0;
  const charts = [];
  const rowKeys = [];
  const jsxRuntime = require('react/jsx-runtime');
  const captureKey = (render) => (type, props, key) => {
    if (type === 'tr' && key !== undefined) rowKeys.push(key);
    return render(type, props, key);
  };
  const { ArtistDetail } = sourceLoader({
    'react/jsx-runtime': {
      ...jsxRuntime,
      jsx: captureKey(jsxRuntime.jsx),
      jsxs: captureKey(jsxRuntime.jsxs),
    },
    '../duckdb/database': databaseMock,
    '../../hooks/useQuery': {
      useQuery: () => ({ data: responses[index++], loading: false, error: null }),
    },
    '../Chart': { Chart: ({ option }) => { charts.push(option); return null; } },
  })('components/analytics/ArtistDetail.tsx');
  return {
    html: renderToStaticMarkup(React.createElement(ArtistDetail, {
      artist, year, device: 'all', onClose() {},
    })),
    charts,
    rowKeys,
  };
}

test('zero-time artist renders an empty time distribution without losing event patterns', () => {
  const rows = query(SQL.artistPatterns('all', 'all', 'Zero Artist'));
  assert.ok(rows.every((row) => row.shareHours === null));
  assert.ok(rows.every((row) => row.shareEvents === 100));
  const { html, charts } = renderArtist('Zero Artist');
  assert.match(html, /state-panel/);
  assert.equal(charts.length, 4); // Monthly hours/events and weekday/hour patterns.
});

test('positive-time artist keeps finite comparison percentages', () => {
  const { html, charts } = renderArtist('Other Artist');
  assert.doesNotMatch(html, /NaN|Infinity/);
  assert.equal(charts.length, 5);
  assert.deepEqual(charts[4].series.map((series) => series.data), [[100], [100]]);
});

test('empty artist segment renders without charts or invalid values', () => {
  const { html, charts } = renderArtist('Zero Artist', '1999');
  assert.match(html, /state-panel/);
  assert.doesNotMatch(html, /NaN|Infinity/);
  assert.equal(charts.length, 0);
});

test('zero total listening time also handles a null global distribution', () => {
  connection.query('UPDATE fact_table SET ms_played = 0');
  try {
    const rows = query(SQL.artistPatterns('all', 'all', 'Zero Artist'));
    assert.ok(rows.every((row) => row.globalShareHours === null));
    const { html, charts } = renderArtist('Zero Artist');
    assert.match(html, /state-panel/);
    assert.doesNotMatch(html, /NaN|Infinity/);
    assert.equal(charts.length, 4);
  } finally {
    connection.query('UPDATE fact_table SET ms_played = 60000 WHERE track_id IN (3, 4)');
  }
});
