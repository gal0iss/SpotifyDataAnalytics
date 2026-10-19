import { StarSchemaDiagram } from '../components/schema/StarSchemaDiagram';
import { useCopy } from '../hooks/useCopy';
import type { Page } from '../types';

interface TechnicalPageProps {
  page: Extract<Page, 'model' | 'pipeline'>;
}

export function TechnicalPage({ page }: TechnicalPageProps) {
  const t = useCopy();

  if (page === 'model') {
    return (
      <main className="page technical-page">
        <span className="kicker">{t('modelKicker')}</span>
        <h1>{t('modelTitle')}</h1>
        <p className="lede">{t('modelText')}</p>
        <StarSchemaDiagram />
      </main>
    );
  }

  const steps = [
    ['01', 'Spotify JSON', t('rawStep')],
    ['02', 'Pre-validation', t('validationStep')],
    ['03', 'Extract + clean', t('cleanStep')],
    ['04', 'Transform', t('transformStep')],
    ['05', 'Parquet', t('parquetStep')],
    ['06', 'GeoLite2 enrichment', t('geoStep')],
    ['07', 'Browser insight', t('browserStep')],
  ];

  return (
    <main className="page technical-page">
      <span className="kicker">{t('pipelineKicker')}</span>
      <h1>{t('pipelineTitle')}</h1>
      <p className="lede">{t('pipelineText')}</p>

      <div className="pipeline-list">
        {steps.map(([number, title, description]) => (
          <div className="pipeline-step" key={number}>
            <span>{number}</span>
            <div>
              <h3>{title}</h3>
              <p>{description}</p>
            </div>
          </div>
        ))}
      </div>

      <div className="error-model">
        <div>
          <strong>{t('fatal')}</strong>
          <p>{t('fatalText')}</p>
        </div>
        <div>
          <strong>{t('recoverable')}</strong>
          <p>{t('recoverableText')}</p>
        </div>
      </div>
    </main>
  );
}
