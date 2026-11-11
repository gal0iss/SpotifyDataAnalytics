import { useMemo, useState } from 'react';
import type { ReactNode } from 'react';
import { Chart } from '../Chart';
import { StatePanel } from '../StatePanel';
import {
  queryArtistCatalog,
  queryArtistCountries,
  queryArtistPatterns,
  queryArtistSummary,
  queryArtistTimeline,
} from '../../data/queries';
import { useCopy } from '../../hooks/useCopy';
import { useQuery } from '../../hooks/useQuery';
import { useLanguage, useTheme } from '../../i18n';
import type { ArtistCatalogRow, ArtistPatternRow } from '../../types';
import { weekdayEs, weekdays } from './InsightSections';

interface ArtistDetailProps {
  artist: string;
  year: string;
  device: string;
  onClose: () => void;
}

export function ArtistDetail({ artist, year, device, onClose }: ArtistDetailProps) {
  const t = useCopy();
  const { language } = useLanguage();
  const { theme } = useTheme();
  const foreground = theme === 'dark' ? '#aab8ca' : '#667085';
  const gridColor = theme === 'dark' ? '#344256' : '#e9edf2';
  const summary = useQuery(
    () => queryArtistSummary(year, device, artist),
    [artist, year, device],
  );
  const timeline = useQuery(
    () => queryArtistTimeline(year, device, artist),
    [artist, year, device],
  );
  const catalog = useQuery(
    () => queryArtistCatalog(year, device, artist),
    [artist, year, device],
  );
  const patterns = useQuery(
    () => queryArtistPatterns(year, device, artist),
    [artist, year, device],
  );
  const countries = useQuery(
    () => queryArtistCountries(year, device, artist),
    [artist, year, device],
  );

  const monthlyRows = timeline.data ?? [];
  const mostActiveMonth = [...monthlyRows].sort((a, b) => b.hours - a.hours)[0];
  const leastActiveMonth = [...monthlyRows].sort((a, b) => a.hours - b.hours)[0];
  const totalListeningHours = monthlyRows.reduce((total, row) => total + row.totalHours, 0);
  const artistListeningHours = monthlyRows.reduce((total, row) => total + row.hours, 0);
  const overallShare = totalListeningHours
    ? (artistListeningHours / totalListeningHours) * 100
    : 0;

  const tooltip = {
    trigger: 'axis' as const,
    backgroundColor: theme === 'dark' ? '#192638' : '#fff',
    borderColor: theme === 'dark' ? '#41516a' : '#e4e7ec',
    textStyle: { color: theme === 'dark' ? '#edf4ff' : '#1d2939' },
  };

  const monthlyHoursOption = {
    tooltip,
    grid: { left: 48, right: 18, top: 20, bottom: 42, containLabel: true },
    xAxis: {
      type: 'category' as const,
      data: monthlyRows.map((row) => row.period),
      axisLabel: { color: foreground, rotate: 35 },
    },
    yAxis: {
      type: 'value' as const,
      name: t('hoursUnit'),
      nameTextStyle: { color: foreground },
      axisLabel: { color: foreground },
      splitLine: { lineStyle: { color: gridColor } },
    },
    series: [
      {
        type: 'line' as const,
        smooth: true,
        data: monthlyRows.map((row) => Number(row.hours.toFixed(2))),
        itemStyle: { color: '#175cd3' },
        areaStyle: { color: 'rgba(23, 92, 211, .12)' },
      },
    ],
  };

  const monthlyEventsOption = {
    tooltip,
    grid: { left: 48, right: 18, top: 20, bottom: 42, containLabel: true },
    xAxis: {
      type: 'category' as const,
      data: monthlyRows.map((row) => row.period),
      axisLabel: { color: foreground, rotate: 35 },
    },
    yAxis: {
      type: 'value' as const,
      name: t('eventsUnit'),
      nameTextStyle: { color: foreground },
      axisLabel: { color: foreground },
      splitLine: { lineStyle: { color: gridColor } },
    },
    series: [
      {
        type: 'bar' as const,
        data: monthlyRows.map((row) => Number(row.events)),
        itemStyle: { color: '#4d8df7', borderRadius: [3, 3, 0, 0] },
      },
    ],
  };

  const patternRows = patterns.data ?? [];
  const localizedPattern = (row: ArtistPatternRow) =>
    row.kind === 'weekday' && language === 'es'
      ? weekdayEs[row.label] ?? row.label
      : row.label;
  const patternOption = (
    rows: ArtistPatternRow[],
    value: 'shareEvents' | 'shareHours',
    globalValue: 'globalShareEvents' | 'globalShareHours',
  ) => ({
    tooltip,
    legend: {
      bottom: 0,
      textStyle: { color: foreground },
      data: [t('artist'), t('overallListening')],
    },
    grid: { left: 90, right: 20, top: 12, bottom: 38, containLabel: true },
    xAxis: {
      type: 'value' as const,
      name: '%',
      axisLabel: { color: foreground },
      splitLine: { lineStyle: { color: gridColor } },
    },
    yAxis: {
      type: 'category' as const,
      data: rows.map(localizedPattern).reverse(),
      axisLabel: { color: foreground },
    },
    series: [
      {
        name: t('artist'),
        type: 'bar' as const,
        data: rows.map((row) => Number(row[value].toFixed(2))).reverse(),
        itemStyle: { color: '#4d8df7', borderRadius: [0, 3, 3, 0] },
      },
      {
        name: t('overallListening'),
        type: 'bar' as const,
        data: rows.map((row) => Number(row[globalValue].toFixed(2))).reverse(),
        itemStyle: { color: '#98a2b3', borderRadius: [0, 3, 3, 0] },
      },
    ],
  });

  const countryOption = {
    tooltip,
    grid: { left: 100, right: 20, top: 12, bottom: 24, containLabel: true },
    xAxis: {
      type: 'value' as const,
      axisLabel: { color: foreground },
      splitLine: { lineStyle: { color: gridColor } },
    },
    yAxis: {
      type: 'category' as const,
      data: countries.data?.map((row) => row.country).reverse(),
      axisLabel: { color: foreground },
    },
    series: [
      {
        type: 'bar' as const,
        data: countries.data?.map((row) => row.events).reverse(),
        itemStyle: { color: '#4d8df7', borderRadius: [0, 3, 3, 0] },
      },
    ],
  };

  const weekdayRows = useMemo(
    () =>
      patternRows
        .filter((row) => row.kind === 'weekday')
        .sort((a, b) => weekdays.indexOf(a.label) - weekdays.indexOf(b.label)),
    [patternRows],
  );
  const hourRows = useMemo(
    () =>
      patternRows
        .filter((row) => row.kind === 'hour')
        .sort((a, b) => a.label.localeCompare(b.label)),
    [patternRows],
  );
  const deviceRows = useMemo(
    () =>
      patternRows
        .filter((row) => row.kind === 'device')
        .sort((a, b) => b.hours - a.hours),
    [patternRows],
  );

  const formatDate = (date: string | null | undefined) =>
    date
      ? new Date(`${date}T00:00:00`).toLocaleDateString(
          language === 'es' ? 'es-ES' : 'en-US',
          { year: 'numeric', month: 'short', day: 'numeric' },
        )
      : '—';

  const formatMonth = (period: string | undefined) =>
    period
      ? new Date(`${period}-01T00:00:00`).toLocaleDateString(
          language === 'es' ? 'es-ES' : 'en-US',
          { year: 'numeric', month: 'long' },
        )
      : '—';

  return (
    <section className="artist-detail" aria-live="polite">
      <div className="artist-detail-heading">
        <div>
          <span className="eyebrow">{t('artistDetail')}</span>
          <h3>{artist}</h3>
        </div>
        <button className="secondary-button" onClick={onClose}>
          {t('closeDetail')}
        </button>
      </div>

      <StatePanel
        loading={summary.loading}
        error={summary.error}
        empty={!summary.loading && !summary.error && !summary.data?.events}
      />
      {summary.data?.events ? (
        <>
          <div className="artist-detail-metrics">
            <Metric
              label={t('plays')}
              value={Number(summary.data.events).toLocaleString()}
            />
            <Metric
              label={t('listeningTime')}
              value={`${Number(summary.data.hours).toFixed(1)} h`}
            />
            <Metric
              label={t('uniqueTracks')}
              value={Number(summary.data.tracks).toLocaleString()}
            />
            <Metric
              label={t('uniqueAlbums')}
              value={Number(summary.data.albums).toLocaleString()}
            />
            <Metric label={t('firstPlay')} value={formatDate(summary.data.firstPlay)} />
            <Metric label={t('lastPlay')} value={formatDate(summary.data.lastPlay)} />
          </div>

          <div className="artist-detail-section">
            <div className="artist-detail-subheading">
              <h4>{t('comparisons')}</h4>
            </div>
            <div className="artist-comparison-grid">
              <Comparison
                label={t('artistShare')}
                artist={`${overallShare.toFixed(1)}%`}
                global="100%"
                globalLabel={t('overallListening')}
              />
              <Comparison
                label={t('averageDuration')}
                artist={`${Number(summary.data.averageMinutes).toFixed(1)} ${t('minutesUnit')}`}
                global={`${Number(summary.data.globalAverageMinutes).toFixed(1)} ${t('minutesUnit')}`}
                globalLabel={t('overallListening')}
              />
              <Comparison
                label={t('skipRate')}
                artist={`${Number(summary.data.skipRate).toFixed(1)}%`}
                global={`${Number(summary.data.globalSkipRate).toFixed(1)}%`}
                globalLabel={t('overallListening')}
              />
              <Comparison
                label={t('shuffleRate')}
                artist={`${Number(summary.data.shuffleRate).toFixed(1)}%`}
                global=""
                globalLabel=""
              />
              <Comparison
                label={t('offlineRate')}
                artist={`${Number(summary.data.offlineRate).toFixed(1)}%`}
                global=""
                globalLabel=""
              />
            </div>
          </div>

          <div className="artist-detail-section">
            <div className="artist-detail-subheading">
              <h4>{t('listeningTime')}</h4>
              <div className="artist-period-extremes">
                <span>
                  {t('mostActiveMonth')}: {formatMonth(mostActiveMonth?.period)}
                </span>
                <span>
                  {t('leastActiveMonth')}: {formatMonth(leastActiveMonth?.period)}
                </span>
              </div>
            </div>
            <StatePanel
              loading={timeline.loading}
              error={timeline.error}
              empty={!timeline.loading && !timeline.error && !monthlyRows.length}
            />
            {monthlyRows.length > 0 && (
              <div className="artist-detail-chart-grid">
                <DetailChart title={t('monthlyListening')}>
                  <Chart option={monthlyHoursOption} height={260} />
                </DetailChart>
                <DetailChart title={t('monthlyPlays')}>
                  <Chart option={monthlyEventsOption} height={260} />
                </DetailChart>
              </div>
            )}
          </div>

          <div className="artist-detail-section">
            <div className="artist-detail-subheading">
              <h4>{t('catalog')}</h4>
            </div>
            <StatePanel
              loading={catalog.loading}
              error={catalog.error}
              empty={!catalog.loading && !catalog.error && !catalog.data?.length}
            />
            {catalog.data?.length ? (
              <div className="artist-detail-catalog-grid">
                <CatalogTable
                  title={t('topTracksByArtist')}
                  kind="track"
                  rows={catalog.data}
                  totalEvents={Number(summary.data.events)}
                  t={t}
                />
                <CatalogTable
                  title={t('topAlbumsByArtist')}
                  kind="album"
                  rows={catalog.data}
                  totalEvents={Number(summary.data.events)}
                  t={t}
                />
              </div>
            ) : null}
          </div>

          <div className="artist-detail-section">
            <div className="artist-detail-subheading">
              <h4>{t('patterns')}</h4>
            </div>
            <StatePanel
              loading={patterns.loading}
              error={patterns.error}
              empty={!patterns.loading && !patterns.error && !patterns.data?.length}
            />
            {patterns.data?.length ? (
              <div className="artist-detail-chart-grid">
                <DetailChart title={t('playsByWeekday')}>
                  <Chart
                    option={patternOption(
                      weekdayRows,
                      'shareEvents',
                      'globalShareEvents',
                    )}
                    height={270}
                  />
                </DetailChart>
                <DetailChart title={t('playsByHour')}>
                  <Chart
                    option={patternOption(
                      hourRows,
                      'shareEvents',
                      'globalShareEvents',
                    )}
                    height={300}
                  />
                </DetailChart>
                <DetailChart title={t('timeByDevice')}>
                  <Chart
                    option={patternOption(
                      deviceRows,
                      'shareHours',
                      'globalShareHours',
                    )}
                    height={270}
                  />
                </DetailChart>
              </div>
            ) : null}
          </div>

          {countries.data?.length ? (
            <div className="artist-detail-section">
              <div className="artist-detail-subheading">
                <h4>{t('countryListening')}</h4>
              </div>
              <Chart option={countryOption} height={280} />
            </div>
          ) : countries.error ? (
            <StatePanel loading={false} error={countries.error} />
          ) : null}
        </>
      ) : null}
    </section>
  );
}

interface MetricProps {
  label: string;
  value: string;
}

function Metric({ label, value }: MetricProps) {
  return (
    <div className="artist-detail-metric">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

interface ComparisonProps {
  label: string;
  artist: string;
  global: string;
  globalLabel: string;
}

function Comparison({ label, artist, global, globalLabel }: ComparisonProps) {
  return (
    <div className="artist-comparison">
      <span>{label}</span>
      <strong>{artist}</strong>
      {global && <small>{globalLabel}: {global}</small>}
    </div>
  );
}

interface DetailChartProps {
  title: string;
  children: ReactNode;
}

function DetailChart({ title, children }: DetailChartProps) {
  return (
    <div className="artist-detail-chart">
      <h5>{title}</h5>
      {children}
    </div>
  );
}

interface CatalogTableProps {
  title: string;
  kind: ArtistCatalogRow['kind'];
  rows: ArtistCatalogRow[];
  totalEvents: number;
  t: ReturnType<typeof useCopy>;
}

function CatalogTable({ title, kind, rows, totalEvents, t }: CatalogTableProps) {
  const [sortKey, setSortKey] = useState<'name' | 'events' | 'hours'>('events');
  const [ascending, setAscending] = useState(false);
  const matchingRows = rows
    .filter((row) => row.kind === kind)
    .slice()
    .sort((a, b) => {
      const direction = ascending ? 1 : -1;
      if (sortKey === 'name') {
        return direction * a.name.localeCompare(b.name);
      }
      return direction * (a[sortKey] - b[sortKey]);
    });

  const changeSort = (key: 'name' | 'events' | 'hours') => {
    if (sortKey === key) {
      setAscending((value) => !value);
      return;
    }
    setSortKey(key);
    setAscending(key === 'name');
  };

  const sortButton = (label: string, key: 'name' | 'events' | 'hours') => (
    <button
      className="artist-sort-button"
      type="button"
      onClick={() => changeSort(key)}
      aria-label={`${t('sortBy')} ${label}`}
    >
      {label}
      {sortKey === key ? (ascending ? ' ↑' : ' ↓') : ''}
    </button>
  );

  return (
    <div className="artist-catalog-table">
      <h5>{title}</h5>
      <div className="artist-table-wrap">
        <table>
          <thead>
            <tr>
              <th>{t('rank')}</th>
              <th>{sortButton(kind === 'track' ? t('trackName') : t('albumName'), 'name')}</th>
              <th>{sortButton(t('plays'), 'events')}</th>
              {kind === 'track' && <th>{t('playShare')}</th>}
              <th>{sortButton(t('hours'), 'hours')}</th>
            </tr>
          </thead>
          <tbody>
            {matchingRows.map((row, index) => (
              <tr key={`${kind}-${row.name}`}>
                <td>{index + 1}</td>
                <td>{row.name}</td>
                <td>{Number(row.events).toLocaleString()}</td>
                {kind === 'track' && (
                  <td>{((row.events / totalEvents) * 100).toFixed(1)}%</td>
                )}
                <td>{Number(row.hours).toFixed(1)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {!matchingRows.length && <StatePanel loading={false} error={null} empty />}
    </div>
  );
}
