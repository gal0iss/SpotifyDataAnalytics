import { useCopy } from '../hooks/useCopy';

interface StatePanelProps {
  loading: boolean;
  error: string | null;
  empty?: boolean;
}

export function StatePanel({ loading, error, empty = false }: StatePanelProps) {
  const t = useCopy();

  if (loading) {
    return (
      <div className="state-panel">
        <span className="loader" /> {t('querying')}
      </div>
    );
  }

  if (error) {
    return (
      <div className="state-panel state-error">
        <strong>{t('unavailable')}</strong>
        <span>{error}</span>
      </div>
    );
  }

  if (empty) {
    return <div className="state-panel">{t('noData')}</div>;
  }

  return null;
}
