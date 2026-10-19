import type { Filters } from '../../types';
import { useCopy } from '../../hooks/useCopy';

interface FilterBarProps {
  filters: Filters;
  setFilters: (filters: Filters) => void;
  years: number[];
  devices: string[];
}

export function FilterBar({ filters, setFilters, years, devices }: FilterBarProps) {
  const t = useCopy();

  return (
    <div className="filters-bar">
      <div>
        <span className="eyebrow">{t('exploreSlice')}</span>
        <strong>{t('filterModel')}</strong>
      </div>
      <label>
        {t('year')}
        <select
          value={filters.year}
          onChange={(event) => setFilters({ ...filters, year: event.target.value })}
        >
          <option value="all">{t('allYears')}</option>
          {years.map((year) => (
            <option key={year} value={year}>
              {year}
            </option>
          ))}
        </select>
      </label>
      <label>
        {t('device')}
        <select
          value={filters.device}
          onChange={(event) => setFilters({ ...filters, device: event.target.value })}
        >
          <option value="all">{t('allDevices')}</option>
          {devices.map((device) => (
            <option key={device} value={device}>
              {device}
            </option>
          ))}
        </select>
      </label>
    </div>
  );
}
