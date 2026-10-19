import { Chart } from '../Chart';
import { StatePanel } from '../StatePanel';
import { useCopy } from '../../hooks/useCopy';
import { useQuery } from '../../hooks/useQuery';
import {
  queryConsistency,
  queryDiscovery,
  queryGrowth,
  queryInsights,
} from '../../data/queries';
import { useLanguage } from '../../i18n';

const weekdays = [
  'Monday',
  'Tuesday',
  'Wednesday',
  'Thursday',
  'Friday',
  'Saturday',
  'Sunday',
];

export const weekdayEs: Record<string, string> = {
  Monday: 'Lunes',
  Tuesday: 'Martes',
  Wednesday: 'Miércoles',
  Thursday: 'Jueves',
  Friday: 'Viernes',
  Saturday: 'Sábado',
  Sunday: 'Domingo',
};

interface FilterValues {
  year: string;
  device: string;
}

export function InsightStrip({ year, device }: FilterValues) {
  const t = useCopy();
  const { language } = useLanguage();
  const result = useQuery(() => queryInsights(year, device), [year, device]);
  const data = result.data;
  const weekday = data
    ? language === 'es'
      ? weekdayEs[data.activeWeekday]
      : data.activeWeekday
    : '—';

  const cards: [string, string, string][] = [
    [
      t('peakHour'),
      data ? `${String(data.peakHour).padStart(2, '0')}:00` : '—',
      data ? `${Number(data.peakHourEvents).toLocaleString()} ${t('eventsAt')}` : '',
    ],
    [
      t('activeWeekday'),
      weekday,
      data ? `${Number(data.activeWeekdayEvents).toLocaleString()} ${t('eventsAt')}` : '',
    ],
    [
      t('averageEvent'),
      data ? `${Number(data.averageMinutes).toFixed(1)} ${t('minutes')}` : '—',
      'SUM(ms_played) / COUNT(*)',
    ],
    [t('skipRate'), data ? `${Number(data.skipRate).toFixed(1)}%` : '—', 'skipped = true'],
    [
      t('shuffleRate'),
      data ? `${Number(data.shuffleRate).toFixed(1)}%` : '—',
      'shuffle = true',
    ],
  ];

  return (
    <>
      {result.error && <StatePanel loading={false} error={result.error} />}
      <div className="insight-grid">
        {cards.map(([label, value, note]) => (
          <div className="insight-card" key={label}>
            <span>{label}</span>
            <strong>{value}</strong>
            <small>{note}</small>
          </div>
        ))}
      </div>
    </>
  );
}

export function DiscoverySection({ year, device }: FilterValues) {
  const t = useCopy();
  const discovery = useQuery(() => queryDiscovery(year, device), [year, device]);
  const growth = useQuery(() => queryGrowth(year, device), [year, device]);
  const data = discovery.data;
  const cards: [string, string, string][] = [
    [
      t('topTrack'),
      data?.topTrack ?? '—',
      data ? `${Number(data.topTrackEvents).toLocaleString()} ${t('eventsAt')}` : '',
    ],
    [
      t('topAlbum'),
      data?.topAlbum ?? '—',
      data ? `${Number(data.topAlbumEvents).toLocaleString()} ${t('eventsAt')}` : '',
    ],
    [
      t('uniqueAlbums'),
      data ? Number(data.uniqueAlbums).toLocaleString() : '—',
      'album_name distinct',
    ],
    [
      t('diversity'),
      data ? `${(Number(data.diversity) * 100).toFixed(2)}%` : '—',
      'unique artists / events',
    ],
  ];

  const option = {
    tooltip: { trigger: 'axis' as const },
    legend: {
      bottom: 0,
      textStyle: { color: '#667085', fontFamily: 'DM Sans' },
    },
    grid: { left: 42, right: 18, top: 24, bottom: 45, containLabel: true },
    xAxis: {
      type: 'category' as const,
      data: growth.data?.map((row) => String(row.year)),
    },
    yAxis: {
      type: 'value' as const,
      splitLine: { lineStyle: { color: '#e9edf2' } },
    },
    series: [
      {
        name: t('newArtists'),
        type: 'line' as const,
        data: growth.data?.map((row) => row.newArtists),
        itemStyle: { color: '#175cd3' },
      },
      {
        name: t('newTracks'),
        type: 'line' as const,
        data: growth.data?.map((row) => row.newTracks),
        itemStyle: { color: '#4d8df7' },
      },
      {
        name: t('newAlbums'),
        type: 'line' as const,
        data: growth.data?.map((row) => row.newAlbums),
        itemStyle: { color: '#98a2b3' },
      },
    ],
  };

  return (
    <section className="insight-section">
      <div className="section-intro compact">
        <div>
          <span className="eyebrow">{t('discovery')}</span>
          <h2>{t('discovery')}</h2>
        </div>
        <p>{t('discoveryText')}</p>
      </div>

      <div className="highlight-grid">
        {cards.map(([label, value, note]) => (
          <div className="highlight-card" key={label}>
            <span>{label}</span>
            <strong title={value}>{value}</strong>
            <small>{note}</small>
          </div>
        ))}
      </div>

      <div className="growth-card">
        <div className="card-heading">
          <div>
            <span className="eyebrow">{t('artistExplorer')}</span>
            <h3>
              {t('newArtists')} / {t('newTracks')} / {t('newAlbums')}
            </h3>
            <p>{t('growthText')}</p>
          </div>
          <span className="data-badge">{growth.data?.length ?? 0} years</span>
        </div>
        <StatePanel
          loading={growth.loading}
          error={growth.error}
          empty={!growth.loading && !growth.error && !growth.data?.length}
        />
        {growth.data?.length ? <Chart option={option} height={300} /> : null}
      </div>
    </section>
  );
}

export function ConsistencySection({ year, device }: FilterValues) {
  const t = useCopy();
  const result = useQuery(() => queryConsistency(year, device), [year, device]);
  const data = result.data;

  const formatDay = (value: string) =>
    value === '—'
      ? value
      : new Date(`${value}T00:00:00`).toLocaleDateString(undefined, {
          year: 'numeric',
          month: 'short',
          day: 'numeric',
        });

  const cards: [string, string, string][] = [
    [
      t('peakDay'),
      data ? formatDay(data.peakDay) : '—',
      data ? `${Number(data.peakDayHours).toFixed(1)} h` : '',
    ],
    [
      t('peakMonth'),
      data?.peakMonth ?? '—',
      data ? `${Number(data.peakMonthHours).toFixed(1)} h` : '',
    ],
    [
      t('activeDays'),
      data ? Number(data.activeDays).toLocaleString() : '—',
      'unique calendar days',
    ],
    [
      t('inactiveDays'),
      data ? Number(data.inactiveDays).toLocaleString() : '—',
      'within observed range',
    ],
    [
      t('maxStreak'),
      data ? `${Number(data.maxStreak)} days` : '—',
      'consecutive active days',
    ],
  ];

  return (
    <section className="insight-section consistency-section">
      <div className="section-intro compact">
        <div>
          <span className="eyebrow">{t('consistency')}</span>
          <h2>{t('consistency')}</h2>
        </div>
        <p>{t('consistencyText')}</p>
      </div>
      <StatePanel loading={result.loading} error={result.error} />
      <div className="highlight-grid five">
        {cards.map(([label, value, note]) => (
          <div className="highlight-card" key={label}>
            <span>{label}</span>
            <strong title={value}>{value}</strong>
            <small>{note}</small>
          </div>
        ))}
      </div>
    </section>
  );
}

export { weekdays };
