import { useMemo, useState, type ReactNode } from 'react';
import { BarChart3, Database, GitBranch, LayoutDashboard, Menu, Moon, Play, Sun, X } from 'lucide-react';
import { Chart } from './components/Chart';
import { SQLViewer } from './components/SQLViewer';
import { SQL, queryArtists, queryArtistsAll, queryArtistsByTime, queryCities, queryConsistency, queryCountries, queryDevices, queryDiscovery, queryGrowth, queryHeatmap, queryInsights, queryMetrics, queryOptions, queryQuality, queryTracks, queryYears } from './data/queries';
import { useQuery } from './hooks/useQuery';
import { LanguageContext, ThemeContext, translations, useLanguage, useTheme, type CopyKey, type Language, type Theme } from './i18n';
import type { Filters, MetricSummary } from './types';

type Page = 'overview' | 'analytics' | 'model' | 'pipeline';
const initialFilters: Filters = { year: 'all', device: 'all' };
const weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'];
const weekdayEs: Record<string, string> = { Monday: 'Lunes', Tuesday: 'Martes', Wednesday: 'Miércoles', Thursday: 'Jueves', Friday: 'Viernes', Saturday: 'Sábado', Sunday: 'Domingo' };

function useCopy() {
  const { language } = useLanguage();
  const copy = translations[language];
  return (key: CopyKey) => copy[key];
}

function StatePanel({ loading, error, empty = false }: { loading: boolean; error: string | null; empty?: boolean }) {
  const t = useCopy();
  if (loading) return <div className="state-panel"><span className="loader" /> {t('querying')}</div>;
  if (error) return <div className="state-panel state-error"><strong>{t('unavailable')}</strong><span>{error}</span></div>;
  if (empty) return <div className="state-panel">{t('noData')}</div>;
  return null;
}

function MetricStrip({ summary }: { summary: MetricSummary | null }) {
  const t = useCopy();
  const metrics = [[t('events'), summary ? summary.events.toLocaleString() : '—', t('rowsFact')], [t('listeningTime'), summary ? `${Number(summary.hours).toFixed(1)} h` : '—', t('derivedMs')], [t('tracks'), summary ? summary.tracks.toLocaleString() : '—', t('excludesUnknown')], [t('artists'), summary ? summary.artists.toLocaleString() : '—', t('distinctArtists')], [t('years'), summary ? summary.years.toLocaleString() : '—', t('hourlyDimension')]];
  return <div className="metric-grid">{metrics.map(([label, value, note]) => <div className="metric" key={label}><span className="metric-label">{label}</span><strong>{value}</strong><span className="metric-note">{note}</span></div>)}</div>;
}

function InsightStrip({ year, device }: { year: string; device: string }) {
  const t = useCopy();
  const { language } = useLanguage();
  const result = useQuery(() => queryInsights(year, device), [year, device]);
  const data = result.data;
  const weekday = data ? (language === 'es' ? weekdayEs[data.activeWeekday] : data.activeWeekday) : '—';
  const cards = [[t('peakHour'), data ? `${String(data.peakHour).padStart(2, '0')}:00` : '—', data ? `${Number(data.peakHourEvents).toLocaleString()} ${t('eventsAt')}` : ''], [t('activeWeekday'), weekday, data ? `${Number(data.activeWeekdayEvents).toLocaleString()} ${t('eventsAt')}` : ''], [t('averageEvent'), data ? `${Number(data.averageMinutes).toFixed(1)} ${t('minutes')}` : '—', 'SUM(ms_played) / COUNT(*)'], [t('skipRate'), data ? `${Number(data.skipRate).toFixed(1)}%` : '—', 'skipped = true'], [t('shuffleRate'), data ? `${Number(data.shuffleRate).toFixed(1)}%` : '—', 'shuffle = true']];
  return <>{result.error && <StatePanel loading={false} error={result.error} />}<div className="insight-grid">{cards.map(([label, value, note]) => <div className="insight-card" key={label}><span>{label}</span><strong>{value}</strong><small>{note}</small></div>)}</div></>;
}

function DiscoverySection({ year, device }: { year: string; device: string }) {
  const t = useCopy();
  const discovery = useQuery(() => queryDiscovery(year, device), [year, device]);
  const growth = useQuery(() => queryGrowth(year, device), [year, device]);
  const data = discovery.data;
  const cards = [[t('topTrack'), data?.topTrack ?? '—', data ? `${Number(data.topTrackEvents).toLocaleString()} ${t('eventsAt')}` : ''], [t('topAlbum'), data?.topAlbum ?? '—', data ? `${Number(data.topAlbumEvents).toLocaleString()} ${t('eventsAt')}` : ''], [t('uniqueAlbums'), data ? Number(data.uniqueAlbums).toLocaleString() : '—', 'album_name distinct'], [t('diversity'), data ? `${(Number(data.diversity) * 100).toFixed(2)}%` : '—', 'unique artists / events']];
  const option = { tooltip: { trigger: 'axis' as const }, legend: { bottom: 0, textStyle: { color: '#667085', fontFamily: 'DM Sans' } }, grid: { left: 42, right: 18, top: 24, bottom: 45, containLabel: true }, xAxis: { type: 'category' as const, data: growth.data?.map((row) => String(row.year)) }, yAxis: { type: 'value' as const, splitLine: { lineStyle: { color: '#e9edf2' } } }, series: [{ name: t('newArtists'), type: 'line' as const, data: growth.data?.map((row) => row.newArtists), itemStyle: { color: '#175cd3' } }, { name: t('newTracks'), type: 'line' as const, data: growth.data?.map((row) => row.newTracks), itemStyle: { color: '#4d8df7' } }, { name: t('newAlbums'), type: 'line' as const, data: growth.data?.map((row) => row.newAlbums), itemStyle: { color: '#98a2b3' } }] };
  return <section className="insight-section"><div className="section-intro compact"><div><span className="eyebrow">{t('discovery')}</span><h2>{t('discovery')}</h2></div><p>{t('discoveryText')}</p></div><div className="highlight-grid">{cards.map(([label, value, note]) => <div className="highlight-card" key={label}><span>{label}</span><strong title={value}>{value}</strong><small>{note}</small></div>)}</div><div className="growth-card"><div className="card-heading"><div><span className="eyebrow">{t('artistExplorer')}</span><h3>{t('newArtists')} / {t('newTracks')} / {t('newAlbums')}</h3><p>{t('growthText')}</p></div><span className="data-badge">{growth.data?.length ?? 0} years</span></div><StatePanel loading={growth.loading} error={growth.error} empty={!growth.loading && !growth.error && !growth.data?.length} />{growth.data?.length ? <Chart option={option} height={300} /> : null}</div></section>;
}

function ConsistencySection({ year, device }: { year: string; device: string }) {
  const t = useCopy();
  const result = useQuery(() => queryConsistency(year, device), [year, device]);
  const data = result.data;
  const formatDay = (value: string) => value === '—' ? value : new Date(`${value}T00:00:00`).toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: 'numeric' });
  const cards = [[t('peakDay'), data ? formatDay(data.peakDay) : '—', data ? `${Number(data.peakDayHours).toFixed(1)} h` : ''], [t('peakMonth'), data?.peakMonth ?? '—', data ? `${Number(data.peakMonthHours).toFixed(1)} h` : ''], [t('activeDays'), data ? Number(data.activeDays).toLocaleString() : '—', 'unique calendar days'], [t('inactiveDays'), data ? Number(data.inactiveDays).toLocaleString() : '—', 'within observed range'], [t('maxStreak'), data ? `${Number(data.maxStreak)} days` : '—', 'consecutive active days']];
  return <section className="insight-section consistency-section"><div className="section-intro compact"><div><span className="eyebrow">{t('consistency')}</span><h2>{t('consistency')}</h2></div><p>{t('consistencyText')}</p></div><StatePanel loading={result.loading} error={result.error} /><div className="highlight-grid five">{cards.map(([label, value, note]) => <div className="highlight-card" key={label}><span>{label}</span><strong title={value}>{value}</strong><small>{note}</small></div>)}</div></section>;
}

function ArtistExplorer({ rows, year, device }: { rows: { name: string; events: number; hours: number }[] | null; year: string; device: string }) {
  const t = useCopy();
  const [expanded, setExpanded] = useState(false);
  const [search, setSearch] = useState('');
  const allArtists = useQuery(() => expanded ? queryArtistsAll(year, device) : Promise.resolve([]), [expanded, year, device]);
  const sourceRows = expanded && allArtists.data?.length ? allArtists.data : rows ?? [];
  const filtered = sourceRows.filter((row) => row.name.toLowerCase().includes(search.toLowerCase()));
  if (!sourceRows.length && !allArtists.loading) return null;
  return <section className="artist-explorer"><div className="section-intro compact"><div><span className="eyebrow">{t('artistExplorer')}</span><h2>{t('artistExplorer')}</h2></div><p>{t('artistExplorerText')}</p></div><div className="explorer-controls"><input value={search} onChange={(event) => setSearch(event.target.value)} placeholder={t('searchArtists')} aria-label={t('searchArtists')} /><button className="secondary-button" onClick={() => setExpanded((value) => !value)}>{expanded ? t('hideTable') : t('showTable')}</button><span className="data-badge">{expanded ? `${allArtists.data?.length ?? 0} artists` : 'Top 10'}</span></div>{expanded && allArtists.loading ? <StatePanel loading error={null} /> : <div className="artist-table-wrap"><table><thead><tr><th>{t('rank')}</th><th>Artist</th><th>{t('events')}</th><th>{t('hours')}</th></tr></thead><tbody>{filtered.map((row, index) => <tr key={row.name}><td>{index + 1}</td><td>{row.name}</td><td>{Number(row.events).toLocaleString()}</td><td>{Number(row.hours).toFixed(1)}</td></tr>)}</tbody></table></div>}</section>;
}

function FiltersBar({ filters, setFilters, years, devices }: { filters: Filters; setFilters: (filters: Filters) => void; years: number[]; devices: string[] }) {
  const t = useCopy();
  return <div className="filters-bar"><div><span className="eyebrow">{t('exploreSlice')}</span><strong>{t('filterModel')}</strong></div><label>{t('year')}<select value={filters.year} onChange={(event) => setFilters({ ...filters, year: event.target.value })}><option value="all">{t('allYears')}</option>{years.map((year) => <option key={year} value={year}>{year}</option>)}</select></label><label>{t('device')}<select value={filters.device} onChange={(event) => setFilters({ ...filters, device: event.target.value })}><option value="all">{t('allDevices')}</option>{devices.map((device) => <option key={device} value={device}>{device}</option>)}</select></label></div>;
}

function ChartCard({ title, description, sql, children }: { title: string; description: string; sql: string; children: ReactNode }) {
  const [showSql, setShowSql] = useState(false);
  const t = useCopy();
  return <section className="chart-card"><div className="card-heading"><div><span className="eyebrow">{t('analysis')}</span><h3>{title}</h3><p>{description}</p></div><button className="text-button" onClick={() => setShowSql(true)}>{t('viewSql')}</button></div>{children}{showSql && <SQLViewer title={title} sql={sql} onClose={() => setShowSql(false)} />}</section>;
}

function QualityStrip({ year, device }: { year: string; device: string }) {
  const t = useCopy();
  const result = useQuery(() => queryQuality(year, device), [year, device]);
  const data = result.data;
  const cards = [[t('unknownTracks'), data ? Number(data.unknownTracks).toLocaleString() : '—', data ? `${(100 - Number(data.trackCoverage)).toFixed(2)}% of events` : ''], [t('unknownLocations'), data ? Number(data.unknownLocations).toLocaleString() : '—', data ? `${(100 - Number(data.locationCoverage)).toFixed(2)}% without country` : ''], [t('trackCoverage'), data ? `${Number(data.trackCoverage).toFixed(1)}%` : '—', 'track_id mapped'], [t('locationCoverage'), data ? `${Number(data.locationCoverage).toFixed(1)}%` : '—', 'country enriched']];
  return <section className="quality-section"><div className="section-label"><span className="eyebrow">{t('quality')}</span></div><div className="quality-grid">{cards.map(([label, value, note]) => <div className="quality-card" key={label}><span>{label}</span><strong>{value}</strong><small>{note}</small></div>)}</div></section>;
}

function Overview({ goTo }: { goTo: (page: Page) => void }) {
  const t = useCopy();
  const summary = useQuery(() => queryMetrics('all', 'all'), []);
  return <main className="page"><section className="hero"><div className="hero-copy"><span className="kicker">{t('beta')}</span><h1>{t('heroTitle')}</h1><p>{t('heroText')}</p><div className="hero-actions"><button className="primary-button" onClick={() => goTo('analytics')}><Play size={16} /> {t('explore')}</button><button className="secondary-button" onClick={() => goTo('pipeline')}>{t('seePipeline')} <GitBranch size={16} /></button></div></div><div className="hero-architecture"><div className="architecture-label">{t('path')}</div>{['Spotify JSON', 'Python ETL', 'Star schema', 'Parquet + SQL', 'React + ECharts'].map((item, index) => <div className="path-step" key={item}><span>0{index + 1}</span><strong>{item}</strong>{index < 4 && <i>↓</i>}</div>)}</div></section><section className="section-block"><div className="section-intro"><div><span className="eyebrow">{t('currentDataset')}</span><h2>{t('queryable')}</h2></div><p>{t('queryableText')}</p></div><StatePanel loading={summary.loading} error={summary.error} /><MetricStrip summary={summary.data} /></section><section className="split-callout"><div><span className="eyebrow">{t('technicalBeta')}</span><h2>{t('inspectModel')}</h2><p>{t('inspectText')}</p></div><button className="secondary-button" onClick={() => goTo('model')}>{t('openModel')} <Database size={16} /></button></section></main>;
}

function Analytics() {
  const t = useCopy();
  const { language } = useLanguage();
  const [filters, setFilters] = useState(initialFilters);
  const options = useQuery(queryOptions, []);
  const summary = useQuery(() => queryMetrics(filters.year, filters.device), [filters.year, filters.device]);
  const years = useQuery(() => queryYears(filters.year, filters.device), [filters.year, filters.device]);
  const artists = useQuery(() => queryArtists(filters.year, filters.device), [filters.year, filters.device]);
  const artistsByTime = useQuery(() => queryArtistsByTime(filters.year, filters.device), [filters.year, filters.device]);
  const tracks = useQuery(() => queryTracks(filters.year, filters.device), [filters.year, filters.device]);
  const heatmap = useQuery(() => queryHeatmap(filters.year, filters.device), [filters.year, filters.device]);
  const devices = useQuery(() => queryDevices(filters.year), [filters.year]);
  const countries = useQuery(() => queryCountries(filters.year, filters.device), [filters.year, filters.device]);
  const cities = useQuery(() => queryCities(filters.year, filters.device), [filters.year, filters.device]);
  const chartText = { textStyle: { fontFamily: 'DM Sans, sans-serif', color: '#667085' }, grid: { left: 42, right: 18, top: 20, bottom: 32, containLabel: true }, tooltip: { trigger: 'axis' as const } };
  const yearOption = { ...chartText, xAxis: { type: 'category' as const, data: years.data?.map((row) => String(row.year)) }, yAxis: { type: 'value' as const, splitLine: { lineStyle: { color: '#e9edf2' } } }, series: [{ type: 'line' as const, smooth: true, data: years.data?.map((row) => Number(row.hours.toFixed(1))), lineStyle: { color: '#175cd3', width: 3 }, areaStyle: { color: 'rgba(23, 92, 211, .12)' }, itemStyle: { color: '#175cd3' } }] };
  const rankingOption = (rows: typeof artists.data, field: 'events' | 'hours', color: string) => ({ ...chartText, grid: { ...chartText.grid, left: 112 }, xAxis: { type: 'value' as const, splitLine: { lineStyle: { color: '#e9edf2' } } }, yAxis: { type: 'category' as const, data: rows?.map((row) => row.name).reverse() }, series: [{ type: 'bar' as const, data: rows?.map((row) => Number(row[field])).reverse(), itemStyle: { color, borderRadius: [0, 3, 3, 0] }, barMaxWidth: 20 }] });
  const trackOption = { ...chartText, grid: { ...chartText.grid, left: 150 }, xAxis: { type: 'value' as const }, yAxis: { type: 'category' as const, data: tracks.data?.map((row) => row.name).reverse(), axisLabel: { width: 130, overflow: 'truncate' as const } }, series: [{ type: 'bar' as const, data: tracks.data?.map((row) => Number(row.events)).reverse(), itemStyle: { color: '#98a2b3', borderRadius: [0, 3, 3, 0] }, barMaxWidth: 20 }] };
  const deviceOption = { ...chartText, xAxis: { type: 'category' as const, data: devices.data?.map((row) => row.device) }, yAxis: [{ type: 'value' as const, name: 'events' }, { type: 'value' as const, name: 'hours' }], series: [{ name: 'events', type: 'bar' as const, data: devices.data?.map((row) => Number(row.events)), itemStyle: { color: '#1d2939', borderRadius: [3, 3, 0, 0] } }, { name: 'hours', type: 'line' as const, yAxisIndex: 1, data: devices.data?.map((row) => Number(row.hours.toFixed(1))), itemStyle: { color: '#175cd3' } }] };
  const countryOption = (rows: { country: string; events: number }[] | null | undefined) => ({ ...chartText, xAxis: { type: 'category' as const, data: rows?.map((row) => row.country), axisLabel: { rotate: 28 } }, yAxis: { type: 'value' as const }, series: [{ type: 'bar' as const, data: rows?.map((row) => Number(row.events)), itemStyle: { color: '#5b9bf5', borderRadius: [3, 3, 0, 0] } }] });
  const heatmapData = heatmap.data?.map((row) => [row.hour, weekdays.indexOf(row.weekday), row.events]) ?? [];
  const heatmapLabels = language === 'es' ? weekdays.map((day) => weekdayEs[day]) : weekdays;
  const heatmapOption = { ...chartText, grid: { left: 54, right: 20, top: 24, bottom: 42 }, xAxis: { type: 'category' as const, data: Array.from({ length: 24 }, (_, hour) => String(hour).padStart(2, '0')) }, yAxis: { type: 'category' as const, data: heatmapLabels }, visualMap: { min: 0, max: Math.max(...(heatmap.data?.map((row) => Number(row.events)) ?? [1])), calculable: false, orient: 'horizontal' as const, left: 'center', bottom: 0, inRange: { color: ['#eef4ff', '#8bb8fb', '#175cd3'] } }, series: [{ type: 'heatmap' as const, data: heatmapData, emphasis: { itemStyle: { shadowBlur: 8, shadowColor: 'rgba(0,0,0,.18)' } } }] };
  const rankedCard = (title: string, description: string, sql: string, result: typeof artists, field: 'events' | 'hours', color: string) => <ChartCard title={title} description={description} sql={sql}><StatePanel loading={result.loading} error={result.error} empty={!result.loading && !result.error && !result.data?.length} />{result.data?.length ? <Chart option={rankingOption(result.data.slice(0, 10), field, color)} /> : null}</ChartCard>;
  return <main className="page analytics-page"><div className="page-title"><div><span className="kicker">{t('interactive')}</span><h1>{t('findShape')}</h1><p>{t('exploreText')}</p></div><div className="query-status"><span className="status-dot" /> {t('localQuery')}</div></div><FiltersBar filters={filters} setFilters={setFilters} years={options.data?.years ?? []} devices={options.data?.devices ?? []} /><StatePanel loading={summary.loading} error={summary.error} /><MetricStrip summary={summary.data} /><InsightStrip year={filters.year} device={filters.device} /><DiscoverySection year={filters.year} device={filters.device} /><ConsistencySection year={filters.year} device={filters.device} /><div className="chart-grid"><ChartCard title={t('timeOverYears')} description={t('timeOverYearsText')} sql={SQL.years(filters.year, filters.device)}><StatePanel loading={years.loading} error={years.error} empty={!years.loading && !years.error && !years.data?.length} />{years.data?.length ? <Chart option={yearOption} /> : null}</ChartCard>{rankedCard(t('topArtists'), t('topArtistsText'), SQL.artists(filters.year, filters.device), artists, 'events', '#4d8df7')}{rankedCard(t('topArtistsTime'), t('topArtistsTimeText'), SQL.artistsByTime(filters.year, filters.device), artistsByTime, 'hours', '#175cd3')}<ChartCard title={t('rhythm')} description={t('rhythmText')} sql={SQL.heatmap(filters.year, filters.device)}><StatePanel loading={heatmap.loading} error={heatmap.error} empty={!heatmap.loading && !heatmap.error && !heatmap.data?.length} />{heatmap.data?.length ? <Chart option={heatmapOption} height={340} /> : null}</ChartCard><ChartCard title={t('topTracks')} description={t('topTracksText')} sql={SQL.tracks(filters.year, filters.device)}><StatePanel loading={tracks.loading} error={tracks.error} empty={!tracks.loading && !tracks.error && !tracks.data?.length} />{tracks.data?.length ? <Chart option={trackOption} /> : null}</ChartCard><ChartCard title={t('deviceProfile')} description={t('deviceProfileText')} sql={SQL.devices(filters.year)}><StatePanel loading={devices.loading} error={devices.error} empty={!devices.loading && !devices.error && !devices.data?.length} />{devices.data?.length ? <Chart option={deviceOption} /> : null}</ChartCard><ChartCard title={t('geography')} description={t('geographyText')} sql={SQL.countries(filters.year, filters.device)}><StatePanel loading={countries.loading} error={countries.error} empty={!countries.loading && !countries.error && !countries.data?.length} />{countries.data?.length ? <Chart option={countryOption(countries.data)} /> : null}</ChartCard><ChartCard title={t('cities')} description={t('citiesText')} sql={SQL.cities(filters.year, filters.device)}><StatePanel loading={cities.loading} error={cities.error} empty={!cities.loading && !cities.error && !cities.data?.length} />{cities.data?.length ? <Chart option={countryOption(cities.data)} /> : null}</ChartCard></div><ArtistExplorer rows={artists.data} year={filters.year} device={filters.device} /><QualityStrip year={filters.year} device={filters.device} /></main>;
}

interface SchemaField {
  name: string;
  type: 'PK' | 'FK' | 'MEASURE' | 'ATTRIBUTE' | 'SENSITIVE';
}

const schemaRelations = {
  date: 'fact_table.date_id -> dim_date.date_id',
  device: 'fact_table.device_id -> dim_device.device_id',
  episode: 'fact_table.episode_id -> dim_episode.episode_id',
  track: 'fact_table.track_id -> dim_track.track_id',
  location: 'fact_table.location_id -> dim_location.location_id',
} as const;

function SchemaCard({ id, title, kind, fields, relation, hovered, onHover }: { id: string; title: string; kind: string; fields: SchemaField[]; relation?: string; hovered: string | null; onHover: (value: string | null) => void }) {
  const isActive = hovered === id || (relation ? hovered === relation : false);
  return <article className={`schema-card schema-card-${id} ${isActive ? 'is-active' : ''}`} onMouseEnter={() => onHover(id)} onMouseLeave={() => onHover(null)}>
    <header className="schema-card-header"><div><span className="schema-card-kind">{kind}</span><h3>{title}</h3></div><span className="schema-card-dot" /></header>
    <div className="schema-fields">{fields.map((field) => <div className={`schema-field schema-field-${field.type.toLowerCase()}`} key={field.name} onMouseEnter={(event) => { event.stopPropagation(); onHover(id); }} onMouseLeave={(event) => { event.stopPropagation(); onHover(id); }}><span className="schema-field-marker">{field.type === 'PK' ? 'KEY' : field.type === 'FK' ? 'FK' : field.type === 'MEASURE' ? 'MEAS' : field.type === 'SENSITIVE' ? 'LOCK' : 'ATTR'}</span><code>{field.name}</code><span className="schema-field-type">{field.type}</span></div>)}</div>
    {relation && <footer className="schema-card-relation">{relation.replace(' -> ', '  ->  ')}</footer>}
  </article>;
}

function StarSchemaDiagram() {
  const [hovered, setHovered] = useState<string | null>(null);
  const factFields: SchemaField[] = [
    { name: 'date_id', type: 'FK' }, { name: 'device_id', type: 'FK' }, { name: 'episode_id', type: 'FK' }, { name: 'event_id', type: 'PK' }, { name: 'incognito_mode', type: 'ATTRIBUTE' }, { name: 'location_id', type: 'FK' }, { name: 'ms_played', type: 'MEASURE' }, { name: 'offline', type: 'ATTRIBUTE' }, { name: 'shuffle', type: 'ATTRIBUTE' },
  ];
  const dimensions = [
    { id: 'date', title: 'DIM_DATE', kind: 'DIMENSION', fields: [{ name: 'date_id', type: 'PK' }, { name: 'day', type: 'ATTRIBUTE' }, { name: 'hour', type: 'ATTRIBUTE' }, { name: 'month', type: 'ATTRIBUTE' }, { name: 'weekday', type: 'ATTRIBUTE' }, { name: 'year', type: 'ATTRIBUTE' }] as SchemaField[], relation: schemaRelations.date },
    { id: 'track', title: 'DIM_TRACK', kind: 'DIMENSION', fields: [{ name: 'track_id', type: 'PK' }, { name: 'Album_name', type: 'ATTRIBUTE' }, { name: 'Artist_name', type: 'ATTRIBUTE' }, { name: 'Spotify_track_uri', type: 'ATTRIBUTE' }, { name: 'track_name', type: 'ATTRIBUTE' }] as SchemaField[], relation: schemaRelations.track },
    { id: 'device', title: 'DIM_DEVICE', kind: 'DIMENSION', fields: [{ name: 'device_id', type: 'PK' }, { name: 'device_type', type: 'ATTRIBUTE' }, { name: 'plataform', type: 'ATTRIBUTE' }] as SchemaField[], relation: schemaRelations.device },
    { id: 'episode', title: 'DIM_EPISODE', kind: 'DIMENSION', fields: [{ name: 'episode_id', type: 'PK' }, { name: 'episode_name', type: 'ATTRIBUTE' }, { name: 'show_name', type: 'ATTRIBUTE' }, { name: 'spotify_episode_uri', type: 'ATTRIBUTE' }] as SchemaField[], relation: schemaRelations.episode },
    { id: 'location', title: 'DIM_LOCATION', kind: 'DIMENSION', fields: [{ name: 'location_id', type: 'PK' }, { name: 'conn_country', type: 'ATTRIBUTE' }, { name: 'ip_addr', type: 'SENSITIVE' }] as SchemaField[], relation: schemaRelations.location },
    { id: 'enriched', title: 'DIM_LOCATION / ENRICHED', kind: 'ENRICHED DIMENSION', fields: [{ name: 'location_id', type: 'PK' }, { name: 'city', type: 'ATTRIBUTE' }, { name: 'conn_country', type: 'ATTRIBUTE' }, { name: 'country', type: 'ATTRIBUTE' }, { name: 'ip_addr', type: 'SENSITIVE' }, { name: 'isp', type: 'SENSITIVE' }, { name: 'latitude', type: 'ATTRIBUTE' }, { name: 'region', type: 'ATTRIBUTE' }, { name: 'longitude', type: 'ATTRIBUTE' }] as SchemaField[], relation: schemaRelations.location },
  ];
  return <div className="star-schema-wrap"><div className="schema-legend"><span><b className="legend-pk">KEY</b> Primary Key</span><span><b className="legend-fk">FK</b> Foreign Key</span><span><b className="legend-measure">MEAS</b> Measure</span><span><b className="legend-sensitive">LOCK</b> Sensitive / hidden</span></div><div className="star-schema-board" onMouseLeave={() => setHovered(null)}><svg className="schema-connections" viewBox="0 0 1000 1180" preserveAspectRatio="none" aria-hidden="true"><line className={hovered === 'date' ? 'is-active' : ''} x1="500" y1="340" x2="500" y2="302" /><line className={hovered === 'track' ? 'is-active' : ''} x1="365" y1="430" x2="170" y2="430" /><line className={hovered === 'device' ? 'is-active' : ''} x1="635" y1="430" x2="830" y2="430" /><line className={hovered === 'episode' ? 'is-active' : ''} x1="500" y1="658" x2="350" y2="620" /><line className={hovered === 'location' ? 'is-active' : ''} x1="500" y1="658" x2="680" y2="620" /></svg><div className="schema-node schema-node-date"><SchemaCard {...dimensions[0]} hovered={hovered} onHover={setHovered} /></div><div className="schema-node schema-node-track"><SchemaCard {...dimensions[1]} hovered={hovered} onHover={setHovered} /></div><div className="schema-node schema-node-fact"><SchemaCard id="fact" title="FACT_TABLE" kind="FACT" fields={factFields} hovered={hovered} onHover={setHovered} /></div><div className="schema-node schema-node-device"><SchemaCard {...dimensions[2]} hovered={hovered} onHover={setHovered} /></div><div className="schema-node schema-node-episode"><SchemaCard {...dimensions[3]} hovered={hovered} onHover={setHovered} /></div><div className="schema-node schema-node-location"><SchemaCard {...dimensions[4]} hovered={hovered} onHover={setHovered} /></div><div className="schema-node schema-node-enriched"><SchemaCard {...dimensions[5]} hovered={hovered} onHover={setHovered} /></div></div><div className="schema-explanation"><strong>Star Schema</strong><p><code>fact_table</code> contiene los eventos de escucha y las métricas asociadas. Las dimensiones proporcionan el contexto temporal, musical, de dispositivo, podcast y geográfico necesario para realizar consultas analíticas.</p><p><b>Grain:</b> una fila representa un evento de escucha registrado.</p></div><div className="schema-privacy"><strong>Privacy boundary</strong><span><code>ip_addr</code>, <code>isp</code> y las coordenadas exactas existen en el modelo físico, pero permanecen ocultos en la interfaz.</span></div></div>;
}

function TechnicalPage({ page }: { page: 'model' | 'pipeline' }) {
  const t = useCopy();
  if (page === 'model') return <main className="page technical-page"><span className="kicker">{t('modelKicker')}</span><h1>{t('modelTitle')}</h1><p className="lede">{t('modelText')}</p><StarSchemaDiagram /></main>;
  return <main className="page technical-page"><span className="kicker">{t('pipelineKicker')}</span><h1>{t('pipelineTitle')}</h1><p className="lede">{t('pipelineText')}</p><div className="pipeline-list">{[['01', 'Spotify JSON', t('rawStep')], ['02', 'Pre-validation', t('validationStep')], ['03', 'Extract + clean', t('cleanStep')], ['04', 'Transform', t('transformStep')], ['05', 'Parquet', t('parquetStep')], ['06', 'GeoLite2 enrichment', t('geoStep')], ['07', 'Browser insight', t('browserStep')]].map(([number, title, description]) => <div className="pipeline-step" key={number}><span>{number}</span><div><h3>{title}</h3><p>{description}</p></div></div>)}</div><div className="error-model"><div><strong>{t('fatal')}</strong><p>{t('fatalText')}</p></div><div><strong>{t('recoverable')}</strong><p>{t('recoverableText')}</p></div></div></main>;
}

function AppContent() {
  const t = useCopy();
  const { language, setLanguage } = useLanguage();
  const { theme, setTheme } = useTheme();
  const [page, setPage] = useState<Page>('overview');
  const [menuOpen, setMenuOpen] = useState(false);
  const navigate = (next: Page) => { setPage(next); setMenuOpen(false); window.scrollTo({ top: 0, behavior: 'smooth' }); };
  const navItems: [Page, string, typeof LayoutDashboard][] = [['overview', t('overview'), LayoutDashboard], ['analytics', t('habits'), BarChart3], ['model', t('model'), Database], ['pipeline', t('pipeline'), GitBranch]];
return (
  <div className="app-shell">
    <header className="topbar">
      <button className="brand" onClick={() => navigate('overview')}>
        <span className="brand-mark">∿</span>
        <span>Listening Data <em>/ beta</em></span>
      </button>

      <button
        className="mobile-menu"
        onClick={() => setMenuOpen(!menuOpen)}
        aria-label="Toggle navigation"
      >
        {menuOpen ? <X size={20} /> : <Menu size={20} />}
      </button>

      <nav className={menuOpen ? 'nav-open' : ''}>
        {navItems.map(([id, label, Icon]) => (
          <button
            className={page === id ? 'nav-link active' : 'nav-link'}
            key={id}
            onClick={() => navigate(id)}
          >
            <Icon size={15} />
            {label}
          </button>
        ))}
      </nav>

      <div className="topbar-actions">
        <button
          className="theme-toggle"
          onClick={() => setTheme(theme === 'light' ? 'dark' : 'light')}
          aria-label={theme === 'light' ? t('darkMode') : t('lightMode')}
          title={theme === 'light' ? t('darkMode') : t('lightMode')}
        >
          {theme === 'light' ? <Moon size={16} /> : <Sun size={16} />}
        </button>

        <button
          className="language-toggle"
          onClick={() => setLanguage(language === 'en' ? 'es' : 'en')}
          aria-label={`${t('language')}: ${language === 'en' ? t('spanish') : t('english')}`}
        >
          {language === 'en' ? 'ES' : 'EN'}
        </button>

        <a
          className="docs-link"
          href={`${import.meta.env.BASE_URL}Docs/Listening-data-Documentacion-Tecnica.pdf`}
          target="_blank"
          rel="noreferrer"
        >
          {t('docs')}
        </a>
      </div>
    </header>

    {page === 'overview' && <Overview goTo={navigate} />}
    {page === 'analytics' && <Analytics />}
    {(page === 'model' || page === 'pipeline') && <TechnicalPage page={page} />}

    <footer className="footer">
      <span>{t('footer')}</span>
      <span>{t('stack')}</span>
    </footer>
  </div>
);
}

export default function App() {
  const [language, setLanguage] = useState<Language>('es');
  const [theme, setTheme] = useState<Theme>(() => (localStorage.getItem('spotify-analytics-theme') as Theme | null) === 'dark' ? 'dark' : 'light');
  const languageValue = useMemo(() => ({ language, setLanguage }), [language]);
  const themeValue = useMemo(() => ({ theme, setTheme: (nextTheme: Theme) => { localStorage.setItem('spotify-analytics-theme', nextTheme); setTheme(nextTheme); } }), [theme]);
  document.documentElement.dataset.theme = theme;
  return <LanguageContext.Provider value={languageValue}><ThemeContext.Provider value={themeValue}><AppContent /></ThemeContext.Provider></LanguageContext.Provider>;
}
