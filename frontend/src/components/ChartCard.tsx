import { useState, type ReactNode } from 'react';
import { SQLViewer } from './SQLViewer';
import { useCopy } from '../hooks/useCopy';

interface ChartCardProps {
  title: string;
  description: string;
  sql: string;
  children: ReactNode;
}

export function ChartCard({ title, description, sql, children }: ChartCardProps) {
  const [showSql, setShowSql] = useState(false);
  const t = useCopy();

  return (
    <section className="chart-card">
      <div className="card-heading">
        <div>
          <span className="eyebrow">{t('analysis')}</span>
          <h3>{title}</h3>
          <p>{description}</p>
        </div>
        <button className="text-button" onClick={() => setShowSql(true)}>
          {t('viewSql')}
        </button>
      </div>
      {children}
      {showSql && (
        <SQLViewer title={title} sql={sql} onClose={() => setShowSql(false)} />
      )}
    </section>
  );
}
