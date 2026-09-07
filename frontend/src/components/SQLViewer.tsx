import { X } from 'lucide-react';

interface SQLViewerProps { title: string; sql: string; onClose: () => void; }

export function SQLViewer({ title, sql, onClose }: SQLViewerProps) {
  return <div className="modal-backdrop" role="presentation" onClick={onClose}>
    <div className="sql-modal" role="dialog" aria-modal="true" aria-labelledby="sql-title" onClick={(event) => event.stopPropagation()}>
      <div className="modal-head"><div><span className="eyebrow">Query definition</span><h2 id="sql-title">{title}</h2></div><button className="icon-button" onClick={onClose} aria-label="Close SQL viewer"><X size={18} /></button></div>
      <pre><code>{sql}</code></pre>
      <p className="modal-note">Read-only SQL. The application controls the query; visitors cannot execute arbitrary statements.</p>
    </div>
  </div>;
}
