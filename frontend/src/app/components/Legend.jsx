'use client'

export const NODE_COLORS = {
  Customer:        '#f97316',
  SalesOrder:      '#3b82f6',
  OrderItem:       '#facc15',
  Product:         '#10b981',
  Delivery:        '#a855f7',
  BillingDocument: '#ef4444',
  JournalEntry:    '#06b6d4',
  Address:         '#94a3b8',
}

export default function Legend({ visibleTypes, onToggle }) {
  return (
    <div className="absolute bottom-4 left-4 z-10"
      style={{ background: '#1a1a2e', border: '1px solid rgba(255,255,255,0.08)', borderRadius: '12px', padding: '12px 14px' }}>
      <p style={{ fontSize: '10px', fontWeight: 600, color: '#64748b', textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: '10px' }}>
        Entity Types
      </p>
      <div className="flex flex-col gap-1.5">
        {Object.entries(NODE_COLORS).map(([type, color]) => {
          const active = visibleTypes.has(type)
          return (
            <button
              key={type}
              onClick={() => onToggle(type)}
              style={{
                display: 'flex', alignItems: 'center', gap: '8px',
                padding: '4px 6px', borderRadius: '8px', border: 'none',
                background: 'transparent', cursor: 'pointer',
                opacity: active ? 1 : 0.3,
                transition: 'opacity 0.15s',
              }}
            >
              <span style={{ width: 10, height: 10, borderRadius: '50%', background: color, flexShrink: 0 }} />
              <span style={{ fontSize: '12px', color: '#cbd5e1', fontWeight: 500 }}>{type}</span>
            </button>
          )
        })}
      </div>
    </div>
  )
}