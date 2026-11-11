import { queryQuality } from '../../data/queries';
import { useCopy } from '../../hooks/useCopy';
import { useQuery } from '../../hooks/useQuery';

interface QualityStripProps {
  year: string;
  device: string;
}

export function QualityStrip({ year, device }: QualityStripProps) {
  const t = useCopy();
  const result = useQuery(() => queryQuality(year, device), [year, device]);
  const data = result.data;

  const cards: [string, string, string][] = [
    [
      t('unknownTracks'),
      data ? Number(data.unknownTracks).toLocaleString() : '—',
      data ? `${(100 - Number(data.trackCoverage)).toFixed(2)}% ${t('percentOfEvents')}` : '',
    ],
    [
      t('unknownLocations'),
      data ? Number(data.unknownLocations).toLocaleString() : '—',
      data
        ? `${(100 - Number(data.locationCoverage)).toFixed(2)}% ${t('percentWithoutCountry')}`
        : '',
    ],
    [
      t('trackCoverage'),
      data ? `${Number(data.trackCoverage).toFixed(1)}%` : '—',
      t('trackMapped'),
    ],
    [
      t('locationCoverage'),
      data ? `${Number(data.locationCoverage).toFixed(1)}%` : '—',
      t('countryEnriched'),
    ],
  ];

  return (
    <section className="quality-section">
      <div className="section-label">
        <span className="eyebrow">{t('quality')}</span>
      </div>
      <div className="quality-grid">
        {cards.map(([label, value, note]) => (
          <div className="quality-card" key={label}>
            <span>{label}</span>
            <strong>{value}</strong>
            <small>{note}</small>
          </div>
        ))}
      </div>
    </section>
  );
}
