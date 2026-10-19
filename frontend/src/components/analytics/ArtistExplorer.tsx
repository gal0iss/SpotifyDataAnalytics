import { useState } from 'react';
import { queryArtistsAll } from '../../data/queries';
import { useCopy } from '../../hooks/useCopy';
import { useQuery } from '../../hooks/useQuery';
import { StatePanel } from '../StatePanel';

interface ArtistRow {
  name: string;
  events: number;
  hours: number;
}

interface ArtistExplorerProps {
  rows: ArtistRow[] | null;
  year: string;
  device: string;
}

export function ArtistExplorer({ rows, year, device }: ArtistExplorerProps) {
  const t = useCopy();
  const [expanded, setExpanded] = useState(false);
  const [search, setSearch] = useState('');
  const allArtists = useQuery(
    () => (expanded ? queryArtistsAll(year, device) : Promise.resolve([])),
    [expanded, year, device],
  );
  const sourceRows = expanded && allArtists.data?.length ? allArtists.data : rows ?? [];
  const filtered = sourceRows.filter((row) =>
    row.name.toLowerCase().includes(search.toLowerCase()),
  );

  if (!sourceRows.length && !allArtists.loading) {
    return null;
  }

  return (
    <section className="artist-explorer">
      <div className="section-intro compact">
        <div>
          <span className="eyebrow">{t('artistExplorer')}</span>
          <h2>{t('artistExplorer')}</h2>
        </div>
        <p>{t('artistExplorerText')}</p>
      </div>

      <div className="explorer-controls">
        <input
          value={search}
          onChange={(event) => setSearch(event.target.value)}
          placeholder={t('searchArtists')}
          aria-label={t('searchArtists')}
        />
        <button
          className="secondary-button"
          onClick={() => setExpanded((value) => !value)}
        >
          {expanded ? t('hideTable') : t('showTable')}
        </button>
        <span className="data-badge">
          {expanded ? `${allArtists.data?.length ?? 0} artists` : 'Top 10'}
        </span>
      </div>

      {expanded && allArtists.loading ? (
        <StatePanel loading error={null} />
      ) : (
        <div className="artist-table-wrap">
          <table>
            <thead>
              <tr>
                <th>{t('rank')}</th>
                <th>Artist</th>
                <th>{t('events')}</th>
                <th>{t('hours')}</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((row, index) => (
                <tr key={row.name}>
                  <td>{index + 1}</td>
                  <td>{row.name}</td>
                  <td>{Number(row.events).toLocaleString()}</td>
                  <td>{Number(row.hours).toFixed(1)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
