import { useState } from 'react';

interface SchemaField {
  name: string;
  type: 'PK' | 'FK' | 'MEASURE' | 'ATTRIBUTE' | 'SENSITIVE';
}

interface SchemaDimension {
  id: string;
  title: string;
  kind: string;
  fields: SchemaField[];
  relation?: string;
}

interface SchemaCardProps extends SchemaDimension {
  hovered: string | null;
  onHover: (value: string | null) => void;
}

const schemaRelations = {
  date: 'fact_table.date_id -> dim_date.date_id',
  device: 'fact_table.device_id -> dim_device.device_id',
  episode: 'fact_table.episode_id -> dim_episode.episode_id',
  track: 'fact_table.track_id -> dim_track.track_id',
  location: 'fact_table.location_id -> dim_location.location_id',
} as const;

function SchemaCard({
  id,
  title,
  kind,
  fields,
  relation,
  hovered,
  onHover,
}: SchemaCardProps) {
  const isActive = hovered === id || (relation ? hovered === relation : false);

  return (
    <article
      className={`schema-card schema-card-${id} ${isActive ? 'is-active' : ''}`}
      onMouseEnter={() => onHover(id)}
      onMouseLeave={() => onHover(null)}
    >
      <header className="schema-card-header">
        <div>
          <span className="schema-card-kind">{kind}</span>
          <h3>{title}</h3>
        </div>
        <span className="schema-card-dot" />
      </header>

      <div className="schema-fields">
        {fields.map((field) => (
          <div
            className={`schema-field schema-field-${field.type.toLowerCase()}`}
            key={field.name}
            onMouseEnter={(event) => {
              event.stopPropagation();
              onHover(id);
            }}
            onMouseLeave={(event) => {
              event.stopPropagation();
              onHover(id);
            }}
          >
            <span className="schema-field-marker">
              {field.type === 'PK'
                ? 'KEY'
                : field.type === 'FK'
                  ? 'FK'
                  : field.type === 'MEASURE'
                    ? 'MEAS'
                    : field.type === 'SENSITIVE'
                      ? 'LOCK'
                      : 'ATTR'}
            </span>
            <code>{field.name}</code>
            <span className="schema-field-type">{field.type}</span>
          </div>
        ))}
      </div>

      {relation && (
        <footer className="schema-card-relation">
          {relation.replace(' -> ', '  ->  ')}
        </footer>
      )}
    </article>
  );
}

const factFields: SchemaField[] = [
  { name: 'date_id', type: 'FK' },
  { name: 'device_id', type: 'FK' },
  { name: 'episode_id', type: 'FK' },
  { name: 'event_id', type: 'PK' },
  { name: 'incognito_mode', type: 'ATTRIBUTE' },
  { name: 'location_id', type: 'FK' },
  { name: 'ms_played', type: 'MEASURE' },
  { name: 'offline', type: 'ATTRIBUTE' },
  { name: 'shuffle', type: 'ATTRIBUTE' },
];

const dimensions: SchemaDimension[] = [
  {
    id: 'date',
    title: 'DIM_DATE',
    kind: 'DIMENSION',
    fields: [
      { name: 'date_id', type: 'PK' },
      { name: 'day', type: 'ATTRIBUTE' },
      { name: 'hour', type: 'ATTRIBUTE' },
      { name: 'month', type: 'ATTRIBUTE' },
      { name: 'weekday', type: 'ATTRIBUTE' },
      { name: 'year', type: 'ATTRIBUTE' },
    ],
    relation: schemaRelations.date,
  },
  {
    id: 'track',
    title: 'DIM_TRACK',
    kind: 'DIMENSION',
    fields: [
      { name: 'track_id', type: 'PK' },
      { name: 'Album_name', type: 'ATTRIBUTE' },
      { name: 'Artist_name', type: 'ATTRIBUTE' },
      { name: 'Spotify_track_uri', type: 'ATTRIBUTE' },
      { name: 'track_name', type: 'ATTRIBUTE' },
    ],
    relation: schemaRelations.track,
  },
  {
    id: 'device',
    title: 'DIM_DEVICE',
    kind: 'DIMENSION',
    fields: [
      { name: 'device_id', type: 'PK' },
      { name: 'device_type', type: 'ATTRIBUTE' },
      { name: 'plataform', type: 'ATTRIBUTE' },
    ],
    relation: schemaRelations.device,
  },
  {
    id: 'episode',
    title: 'DIM_EPISODE',
    kind: 'DIMENSION',
    fields: [
      { name: 'episode_id', type: 'PK' },
      { name: 'episode_name', type: 'ATTRIBUTE' },
      { name: 'show_name', type: 'ATTRIBUTE' },
      { name: 'spotify_episode_uri', type: 'ATTRIBUTE' },
    ],
    relation: schemaRelations.episode,
  },
  {
    id: 'location',
    title: 'DIM_LOCATION',
    kind: 'DIMENSION',
    fields: [
      { name: 'location_id', type: 'PK' },
      { name: 'conn_country', type: 'ATTRIBUTE' },
      { name: 'ip_addr', type: 'SENSITIVE' },
    ],
    relation: schemaRelations.location,
  },
  {
    id: 'enriched',
    title: 'DIM_LOCATION / ENRICHED',
    kind: 'ENRICHED DIMENSION',
    fields: [
      { name: 'location_id', type: 'PK' },
      { name: 'city', type: 'ATTRIBUTE' },
      { name: 'conn_country', type: 'ATTRIBUTE' },
      { name: 'country', type: 'ATTRIBUTE' },
      { name: 'ip_addr', type: 'SENSITIVE' },
      { name: 'isp', type: 'SENSITIVE' },
      { name: 'latitude', type: 'ATTRIBUTE' },
      { name: 'region', type: 'ATTRIBUTE' },
      { name: 'longitude', type: 'ATTRIBUTE' },
    ],
    relation: schemaRelations.location,
  },
];

export function StarSchemaDiagram() {
  const [hovered, setHovered] = useState<string | null>(null);

  return (
    <div className="star-schema-wrap">
      <div className="schema-legend">
        <span>
          <b className="legend-pk">KEY</b> Primary Key
        </span>
        <span>
          <b className="legend-fk">FK</b> Foreign Key
        </span>
        <span>
          <b className="legend-measure">MEAS</b> Measure
        </span>
        <span>
          <b className="legend-sensitive">LOCK</b> Sensitive / hidden
        </span>
      </div>

      <div className="star-schema-board" onMouseLeave={() => setHovered(null)}>
        <svg
          className="schema-connections"
          viewBox="0 0 1000 1180"
          preserveAspectRatio="none"
          aria-hidden="true"
        >
          <line className={hovered === 'date' ? 'is-active' : ''} x1="500" y1="340" x2="500" y2="302" />
          <line className={hovered === 'track' ? 'is-active' : ''} x1="365" y1="430" x2="170" y2="430" />
          <line className={hovered === 'device' ? 'is-active' : ''} x1="635" y1="430" x2="830" y2="430" />
          <line className={hovered === 'episode' ? 'is-active' : ''} x1="500" y1="658" x2="350" y2="620" />
          <line className={hovered === 'location' ? 'is-active' : ''} x1="500" y1="658" x2="680" y2="620" />
        </svg>

        <div className="schema-node schema-node-date">
          <SchemaCard {...dimensions[0]} hovered={hovered} onHover={setHovered} />
        </div>
        <div className="schema-node schema-node-track">
          <SchemaCard {...dimensions[1]} hovered={hovered} onHover={setHovered} />
        </div>
        <div className="schema-node schema-node-fact">
          <SchemaCard
            id="fact"
            title="FACT_TABLE"
            kind="FACT"
            fields={factFields}
            hovered={hovered}
            onHover={setHovered}
          />
        </div>
        <div className="schema-node schema-node-device">
          <SchemaCard {...dimensions[2]} hovered={hovered} onHover={setHovered} />
        </div>
        <div className="schema-node schema-node-episode">
          <SchemaCard {...dimensions[3]} hovered={hovered} onHover={setHovered} />
        </div>
        <div className="schema-node schema-node-location">
          <SchemaCard {...dimensions[4]} hovered={hovered} onHover={setHovered} />
        </div>
        <div className="schema-node schema-node-enriched">
          <SchemaCard {...dimensions[5]} hovered={hovered} onHover={setHovered} />
        </div>
      </div>

      <div className="schema-explanation">
        <strong>Star Schema</strong>
        <p>
          <code>fact_table</code> contiene los eventos de escucha y las métricas asociadas. Las
          dimensiones proporcionan el contexto temporal, musical, de dispositivo, podcast y
          geográfico necesario para realizar consultas analíticas.
        </p>
        <p>
          <b>Grain:</b> una fila representa un evento de escucha registrado.
        </p>
      </div>

      <div className="schema-privacy">
        <strong>Privacy boundary</strong>
        <span>
          <code>ip_addr</code>, <code>isp</code> y las coordenadas exactas existen en el modelo
          físico, pero permanecen ocultos en la interfaz.
        </span>
      </div>
    </div>
  );
}
