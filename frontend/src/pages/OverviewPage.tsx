import { Database, GitBranch, Play } from 'lucide-react';
import { queryMetrics } from '../data/queries';
import { useCopy } from '../hooks/useCopy';
import { useQuery } from '../hooks/useQuery';
import { MetricStrip } from '../components/MetricStrip';
import { StatePanel } from '../components/StatePanel';
import type { Page } from '../types';

interface OverviewPageProps {
  goTo: (page: Page) => void;
}

export function OverviewPage({ goTo }: OverviewPageProps) {
  const t = useCopy();
  const summary = useQuery(() => queryMetrics('all', 'all'), []);

  return (
    <main className="page">
      <section className="hero">
        <div className="hero-copy">
          <span className="kicker">{t('beta')}</span>
          <h1>{t('heroTitle')}</h1>
          <p>{t('heroText')}</p>
          <div className="hero-actions">
            <button className="primary-button" onClick={() => goTo('analytics')}>
              <Play size={16} /> {t('explore')}
            </button>
            <button className="secondary-button" onClick={() => goTo('pipeline')}>
              {t('seePipeline')} <GitBranch size={16} />
            </button>
          </div>
        </div>

        <div className="hero-architecture">
          <div className="architecture-label">{t('path')}</div>
          {['Spotify JSON', 'Python ETL', 'Star schema', 'Parquet + SQL', 'React + ECharts'].map(
            (item, index) => (
              <div className="path-step" key={item}>
                <span>0{index + 1}</span>
                <strong>{item}</strong>
                {index < 4 && <i>↓</i>}
              </div>
            ),
          )}
        </div>
      </section>

      <section className="section-block">
        <div className="section-intro">
          <div>
            <span className="eyebrow">{t('currentDataset')}</span>
            <h2>{t('queryable')}</h2>
          </div>
          <p>{t('queryableText')}</p>
        </div>
        <StatePanel loading={summary.loading} error={summary.error} />
        <MetricStrip summary={summary.data} />
      </section>

      <section className="split-callout">
        <div>
          <span className="eyebrow">{t('technicalBeta')}</span>
          <h2>{t('inspectModel')}</h2>
          <p>{t('inspectText')}</p>
        </div>
        <button className="secondary-button" onClick={() => goTo('model')}>
          {t('openModel')} <Database size={16} />
        </button>
      </section>
    </main>
  );
}
