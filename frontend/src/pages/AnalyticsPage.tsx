import { useState } from 'react';
import { Chart } from '../components/Chart';
import { ArtistExplorer } from '../components/analytics/ArtistExplorer';
import { FilterBar } from '../components/analytics/FilterBar';
import {
  ConsistencySection,
  DiscoverySection,
  InsightStrip,
  weekdays,
} from '../components/analytics/InsightSections';
import { QualityStrip } from '../components/analytics/QualityStrip';
import { ChartCard } from '../components/ChartCard';
import { MetricStrip } from '../components/MetricStrip';
import { StatePanel } from '../components/StatePanel';
import {
  SQL,
  queryArtists,
  queryArtistsByTime,
  queryCities,
  queryCountries,
  queryDevices,
  queryHeatmap,
  queryMetrics,
  queryOptions,
  queryTracks,
  queryYears,
} from '../data/queries';
import { useCopy } from '../hooks/useCopy';
import { useQuery } from '../hooks/useQuery';
import { useLanguage } from '../i18n';
import type { Filters } from '../types';
import { weekdayEs } from '../components/analytics/InsightSections';

const initialFilters: Filters = { year: 'all', device: 'all' };

export function AnalyticsPage() {
  const t = useCopy();
  const { language } = useLanguage();
  const [filters, setFilters] = useState(initialFilters);
  const options = useQuery(queryOptions, []);
  const summary = useQuery(
    () => queryMetrics(filters.year, filters.device),
    [filters.year, filters.device],
  );
  const years = useQuery(
    () => queryYears(filters.year, filters.device),
    [filters.year, filters.device],
  );
  const artists = useQuery(
    () => queryArtists(filters.year, filters.device),
    [filters.year, filters.device],
  );
  const artistsByTime = useQuery(
    () => queryArtistsByTime(filters.year, filters.device),
    [filters.year, filters.device],
  );
  const tracks = useQuery(
    () => queryTracks(filters.year, filters.device),
    [filters.year, filters.device],
  );
  const heatmap = useQuery(
    () => queryHeatmap(filters.year, filters.device),
    [filters.year, filters.device],
  );
  const devices = useQuery(() => queryDevices(filters.year), [filters.year]);
  const countries = useQuery(
    () => queryCountries(filters.year, filters.device),
    [filters.year, filters.device],
  );
  const cities = useQuery(
    () => queryCities(filters.year, filters.device),
    [filters.year, filters.device],
  );

  const chartText = {
    textStyle: { fontFamily: 'DM Sans, sans-serif', color: '#667085' },
    grid: { left: 42, right: 18, top: 20, bottom: 32, containLabel: true },
    tooltip: { trigger: 'axis' as const },
  };

  const yearOption = {
    ...chartText,
    xAxis: {
      type: 'category' as const,
      data: years.data?.map((row) => String(row.year)),
    },
    yAxis: {
      type: 'value' as const,
      splitLine: { lineStyle: { color: '#e9edf2' } },
    },
    series: [
      {
        type: 'line' as const,
        smooth: true,
        data: years.data?.map((row) => Number(row.hours.toFixed(1))),
        lineStyle: { color: '#175cd3', width: 3 },
        areaStyle: { color: 'rgba(23, 92, 211, .12)' },
        itemStyle: { color: '#175cd3' },
      },
    ],
  };

  const rankingOption = (
    rows: typeof artists.data,
    field: 'events' | 'hours',
    color: string,
  ) => ({
    ...chartText,
    grid: { ...chartText.grid, left: 112 },
    xAxis: {
      type: 'value' as const,
      splitLine: { lineStyle: { color: '#e9edf2' } },
    },
    yAxis: {
      type: 'category' as const,
      data: rows?.map((row) => row.name).reverse(),
    },
    series: [
      {
        type: 'bar' as const,
        data: rows?.map((row) => Number(row[field])).reverse(),
        itemStyle: { color, borderRadius: [0, 3, 3, 0] },
        barMaxWidth: 20,
      },
    ],
  });

  const trackOption = {
    ...chartText,
    grid: { ...chartText.grid, left: 150 },
    xAxis: { type: 'value' as const },
    yAxis: {
      type: 'category' as const,
      data: tracks.data?.map((row) => row.name).reverse(),
      axisLabel: { width: 130, overflow: 'truncate' as const },
    },
    series: [
      {
        type: 'bar' as const,
        data: tracks.data?.map((row) => Number(row.events)).reverse(),
        itemStyle: { color: '#98a2b3', borderRadius: [0, 3, 3, 0] },
        barMaxWidth: 20,
      },
    ],
  };

  const deviceOption = {
    ...chartText,
    xAxis: {
      type: 'category' as const,
      data: devices.data?.map((row) => row.device),
    },
    yAxis: [
      { type: 'value' as const, name: 'events' },
      { type: 'value' as const, name: 'hours' },
    ],
    series: [
      {
        name: 'events',
        type: 'bar' as const,
        data: devices.data?.map((row) => Number(row.events)),
        itemStyle: { color: '#1d2939', borderRadius: [3, 3, 0, 0] },
      },
      {
        name: 'hours',
        type: 'line' as const,
        yAxisIndex: 1,
        data: devices.data?.map((row) => Number(row.hours.toFixed(1))),
        itemStyle: { color: '#175cd3' },
      },
    ],
  };

  const countryOption = (
    rows: { country: string; events: number }[] | null | undefined,
  ) => ({
    ...chartText,
    xAxis: {
      type: 'category' as const,
      data: rows?.map((row) => row.country),
      axisLabel: { rotate: 28 },
    },
    yAxis: { type: 'value' as const },
    series: [
      {
        type: 'bar' as const,
        data: rows?.map((row) => Number(row.events)),
        itemStyle: { color: '#5b9bf5', borderRadius: [3, 3, 0, 0] },
      },
    ],
  });

  const heatmapData =
    heatmap.data?.map((row) => [row.hour, weekdays.indexOf(row.weekday), row.events]) ?? [];
  const heatmapLabels =
    language === 'es' ? weekdays.map((day) => weekdayEs[day]) : weekdays;
  const heatmapOption = {
    ...chartText,
    grid: { left: 54, right: 20, top: 24, bottom: 42 },
    xAxis: {
      type: 'category' as const,
      data: Array.from({ length: 24 }, (_, hour) => String(hour).padStart(2, '0')),
    },
    yAxis: { type: 'category' as const, data: heatmapLabels },
    visualMap: {
      min: 0,
      max: Math.max(...(heatmap.data?.map((row) => Number(row.events)) ?? [1])),
      calculable: false,
      orient: 'horizontal' as const,
      left: 'center',
      bottom: 0,
      inRange: { color: ['#eef4ff', '#8bb8fb', '#175cd3'] },
    },
    series: [
      {
        type: 'heatmap' as const,
        data: heatmapData,
        emphasis: {
          itemStyle: {
            shadowBlur: 8,
            shadowColor: 'rgba(0,0,0,.18)',
          },
        },
      },
    ],
  };

  const rankedCard = (
    title: string,
    description: string,
    sql: string,
    result: typeof artists,
    field: 'events' | 'hours',
    color: string,
  ) => (
    <ChartCard title={title} description={description} sql={sql}>
      <StatePanel
        loading={result.loading}
        error={result.error}
        empty={!result.loading && !result.error && !result.data?.length}
      />
      {result.data?.length ? (
        <Chart option={rankingOption(result.data.slice(0, 10), field, color)} />
      ) : null}
    </ChartCard>
  );

  return (
    <main className="page analytics-page">
      <div className="page-title">
        <div>
          <span className="kicker">{t('interactive')}</span>
          <h1>{t('findShape')}</h1>
          <p>{t('exploreText')}</p>
        </div>
        <div className="query-status">
          <span className="status-dot" /> {t('localQuery')}
        </div>
      </div>

      <FilterBar
        filters={filters}
        setFilters={setFilters}
        years={options.data?.years ?? []}
        devices={options.data?.devices ?? []}
      />
      <StatePanel loading={summary.loading} error={summary.error} />
      <MetricStrip summary={summary.data} />
      <InsightStrip year={filters.year} device={filters.device} />
      <DiscoverySection year={filters.year} device={filters.device} />
      <ConsistencySection year={filters.year} device={filters.device} />

      <div className="chart-grid">
        <ChartCard
          title={t('timeOverYears')}
          description={t('timeOverYearsText')}
          sql={SQL.years(filters.year, filters.device)}
        >
          <StatePanel
            loading={years.loading}
            error={years.error}
            empty={!years.loading && !years.error && !years.data?.length}
          />
          {years.data?.length ? <Chart option={yearOption} /> : null}
        </ChartCard>

        {rankedCard(
          t('topArtists'),
          t('topArtistsText'),
          SQL.artists(filters.year, filters.device),
          artists,
          'events',
          '#4d8df7',
        )}
        {rankedCard(
          t('topArtistsTime'),
          t('topArtistsTimeText'),
          SQL.artistsByTime(filters.year, filters.device),
          artistsByTime,
          'hours',
          '#175cd3',
        )}

        <ChartCard
          title={t('rhythm')}
          description={t('rhythmText')}
          sql={SQL.heatmap(filters.year, filters.device)}
        >
          <StatePanel
            loading={heatmap.loading}
            error={heatmap.error}
            empty={!heatmap.loading && !heatmap.error && !heatmap.data?.length}
          />
          {heatmap.data?.length ? <Chart option={heatmapOption} height={340} /> : null}
        </ChartCard>

        <ChartCard
          title={t('topTracks')}
          description={t('topTracksText')}
          sql={SQL.tracks(filters.year, filters.device)}
        >
          <StatePanel
            loading={tracks.loading}
            error={tracks.error}
            empty={!tracks.loading && !tracks.error && !tracks.data?.length}
          />
          {tracks.data?.length ? <Chart option={trackOption} /> : null}
        </ChartCard>

        <ChartCard
          title={t('deviceProfile')}
          description={t('deviceProfileText')}
          sql={SQL.devices(filters.year)}
        >
          <StatePanel
            loading={devices.loading}
            error={devices.error}
            empty={!devices.loading && !devices.error && !devices.data?.length}
          />
          {devices.data?.length ? <Chart option={deviceOption} /> : null}
        </ChartCard>

        <ChartCard
          title={t('geography')}
          description={t('geographyText')}
          sql={SQL.countries(filters.year, filters.device)}
        >
          <StatePanel
            loading={countries.loading}
            error={countries.error}
            empty={!countries.loading && !countries.error && !countries.data?.length}
          />
          {countries.data?.length ? <Chart option={countryOption(countries.data)} /> : null}
        </ChartCard>

        <ChartCard
          title={t('cities')}
          description={t('citiesText')}
          sql={SQL.cities(filters.year, filters.device)}
        >
          <StatePanel
            loading={cities.loading}
            error={cities.error}
            empty={!cities.loading && !cities.error && !cities.data?.length}
          />
          {cities.data?.length ? <Chart option={countryOption(cities.data)} /> : null}
        </ChartCard>
      </div>

      <ArtistExplorer rows={artists.data} year={filters.year} device={filters.device} />
      <QualityStrip year={filters.year} device={filters.device} />
    </main>
  );
}
