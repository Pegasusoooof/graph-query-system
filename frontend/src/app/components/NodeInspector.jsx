'use client'
import { NODE_COLORS } from './Legend'

const HIDDEN_KEYS = ['id', 'entity_type', 'label', '__indexColor', 'index', 'vx', 'vy', 'x', 'y', 'fx', 'fy']

export default function NodeInspector({ node, connections, onClose }) {
  if (!node) return null

  const color = NODE_COLORS[node.entity_type] || '#94a3b8'
  const fields = Object.entries(node).filter(
    ([k, v]) => !HIDDEN_KEYS.includes(k) && v !== null && v !== '' && v !== undefined
  )

  const incoming = connections?.filter(c => c.direction === 'incoming') || []
  const outgoing = connections?.filter(c => c.direction === 'outgoing') || []

  return (
    <div className="absolute top-4 right-4 w-72 bg-[#111111] border border-[#222222] rounded-xl shadow-2xl z-20 overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-[#1e1e1e]">
        <div className="flex items-center gap-2">
          <span className="w-3 h-3 rounded-full" style={{ background: color }} />
          <div>
            <p className="text-xs text-[#555] font-medium">{node.entity_type}</p>
            <p className="text-sm font-semibold text-[#e2e8f0] leading-tight">{node.label}</p>
          </div>
        </div>
        <button onClick={onClose} className="text-[#444] hover:text-[#aaa] text-lg leading-none transition-colors">×</button>
      </div>

      {/* Fields */}
      <div className="px-4 py-3 max-h-52 overflow-y-auto">
        {fields.length === 0 && <p className="text-xs text-[#444]">No additional fields</p>}
        {fields.map(([k, v]) => (
          <div key={k} className="flex justify-between gap-2 py-0.5">
            <span className="text-xs text-[#555] capitalize flex-shrink-0">{k.replace(/_/g, ' ')}</span>
            <span className="text-xs text-[#ccc] font-medium text-right truncate max-w-[140px]">{String(v)}</span>
          </div>
        ))}
      </div>

      {/* Connections */}
      {(incoming.length > 0 || outgoing.length > 0) && (
        <div className="border-t border-[#1e1e1e] px-4 py-3">
          <p className="text-xs font-semibold text-[#444] uppercase tracking-wide mb-2">
            Connections ({incoming.length + outgoing.length})
          </p>
          <div className="max-h-36 overflow-y-auto flex flex-col gap-1">
            {outgoing.map((c, i) => (
              <div key={i} className="flex items-center gap-2">
                <span className="w-1.5 h-1.5 rounded-full flex-shrink-0" style={{ background: NODE_COLORS[c.entity_type] || '#94a3b8' }} />
                <span className="text-xs text-[#555]">→ {c.relationship}</span>
                <span className="text-xs text-[#aaa] truncate">{c.label}</span>
              </div>
            ))}
            {incoming.map((c, i) => (
              <div key={i} className="flex items-center gap-2">
                <span className="w-1.5 h-1.5 rounded-full flex-shrink-0" style={{ background: NODE_COLORS[c.entity_type] || '#94a3b8' }} />
                <span className="text-xs text-[#555]">← {c.relationship}</span>
                <span className="text-xs text-[#aaa] truncate">{c.label}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      <div className="px-4 pb-3">
        <p className="text-xs text-[#333] italic">
          Additional fields hidden for readability
        </p>
      </div>
    </div>
  )
}