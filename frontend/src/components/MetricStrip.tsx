import type { MetricSummary } from '../types';
import { useCopy } from '../hooks/useCopy';

interface MetricStripProps {
  summary: MetricSummary | null;
}

export function MetricStrip({ summary }: MetricStripProps) {
  const t = useCopy();
  const metrics: [string, string, string][] = [
    [t('events'), summary ? summary.events.toLocaleString() : '—', t('rowsFact')],
    [
      t('listeningTime'),
      summary ? `${Number(summary.hours).toFixed(1)} h` : '—',
      t('derivedMs'),
    ],
    [t('tracks'), summary ? summary.tracks.toLocaleString() : '—', t('excludesUnknown')],
    [t('artists'), summary ? summary.artists.toLocaleString() : '—', t('distinctArtists')],
    [t('years'), summary ? summary.years.toLocaleString() : '—', t('hourlyDimension')],
  ];

  return (
    <div className="metric-grid">
      {metrics.map(([label, value, note]) => (
        <div className="metric" key={label}>
          <span className="metric-label">{label}</span>
          <strong>{value}</strong>
          <span className="metric-note">{note}</span>
        </div>
      ))}
    </div>
  );
}
