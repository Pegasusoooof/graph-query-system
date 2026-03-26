'use client'
import { useEffect, useRef, useState, useCallback } from 'react'
import dynamic from 'next/dynamic'
import { NODE_COLORS } from './Legend'

const ForceGraph2D = dynamic(() => import('react-force-graph-2d'), { ssr: false })

const API = 'https://graph-query-system-x9mb.onrender.com/'

export default function GraphCanvas({ highlightedIds, visibleTypes, onNodeClick, onReady }) {
  const [graphData, setGraphData] = useState({ nodes: [], links: [] })
  const [loading, setLoading] = useState(true)
  const fgRef = useRef()
  const onReadyRef = useRef(onReady)
  onReadyRef.current = onReady

  useEffect(() => {
    fetch(`${API}/graph/`)
      .then(r => r.json())
      .then(data => { setGraphData(data); setLoading(false) })
      .catch(() => setLoading(false))
  }, [])

  // Fire onReady after graph mounts — pass a stable proxy that always reads fgRef.current
  useEffect(() => {
    if (loading) return
    const interval = setInterval(() => {
      if (fgRef.current) {
        clearInterval(interval)
        onReadyRef.current && onReadyRef.current({
          zoomToFit: (duration, padding) => {
            if (fgRef.current) fgRef.current.zoomToFit(duration, padding)
          },
          centerAt: (x, y, duration) => {
            if (fgRef.current) fgRef.current.centerAt(x, y, duration)
          },
          zoom: (scale, duration) => {
            if (fgRef.current) fgRef.current.zoom(scale, duration)
          },
        })
      }
    }, 100)
    return () => clearInterval(interval)
  }, [loading])

  const filteredData = {
    nodes: graphData.nodes.filter(n => visibleTypes.has(n.entity_type)),
    links: graphData.links.filter(l => {
      const src = typeof l.source === 'object' ? l.source?.id : l.source
      const tgt = typeof l.target === 'object' ? l.target?.id : l.target
      const srcNode = graphData.nodes.find(n => n.id === src)
      const tgtNode = graphData.nodes.find(n => n.id === tgt)
      return srcNode && tgtNode && visibleTypes.has(srcNode.entity_type) && visibleTypes.has(tgtNode.entity_type)
    }),
  }

  const highlightSet = new Set(highlightedIds)

  const drawBadge = (ctx, text, x, y) => {
    const fontSize = 4
    ctx.font = `bold ${fontSize}px -apple-system, sans-serif`
    const textWidth = ctx.measureText(text).width
    const padX = 3
    const padY = 2
    const bw = textWidth + padX * 2
    const bh = fontSize + padY * 2
    const bx = x - bw / 2
    const by = y - bh / 2
    const radius = 1.5

    ctx.beginPath()
    ctx.moveTo(bx + radius, by)
    ctx.lineTo(bx + bw - radius, by)
    ctx.quadraticCurveTo(bx + bw, by, bx + bw, by + radius)
    ctx.lineTo(bx + bw, by + bh - radius)
    ctx.quadraticCurveTo(bx + bw, by + bh, bx + bw - radius, by + bh)
    ctx.lineTo(bx + radius, by + bh)
    ctx.quadraticCurveTo(bx, by + bh, bx, by + bh - radius)
    ctx.lineTo(bx, by + radius)
    ctx.quadraticCurveTo(bx, by, bx + radius, by)
    ctx.closePath()
    ctx.fillStyle = '#1a1a2e'
    ctx.fill()

    ctx.fillStyle = '#ffffff'
    ctx.textAlign = 'center'
    ctx.textBaseline = 'middle'
    ctx.fillText(text, x, y + 0.3)
    ctx.textBaseline = 'alphabetic'
  }

  const nodeCanvasObject = useCallback((node, ctx, globalScale) => {
    const isHighlighted = highlightSet.has(node.id)
    const color = NODE_COLORS[node.entity_type] || '#94a3b8'
    const r = isHighlighted ? 7 : 4

    if (isHighlighted) {
      ctx.beginPath()
      ctx.arc(node.x, node.y, r + 4, 0, 2 * Math.PI)
      ctx.fillStyle = color + '33'
      ctx.fill()
      ctx.beginPath()
      ctx.arc(node.x, node.y, r + 2, 0, 2 * Math.PI)
      ctx.fillStyle = color + '66'
      ctx.fill()
    }

    ctx.beginPath()
    ctx.arc(node.x, node.y, r, 0, 2 * Math.PI)
    ctx.fillStyle = color
    ctx.fill()

    ctx.beginPath()
    ctx.arc(node.x, node.y, r, 0, 2 * Math.PI)
    ctx.strokeStyle = 'rgba(255,255,255,0.15)'
    ctx.lineWidth = 1
    ctx.stroke()

    if (isHighlighted) {
      drawBadge(ctx, node.entity_type, node.x, node.y + r + 6)
    } else if (globalScale > 2.5) {
      const label = node.label || ''
      ctx.font = `3px -apple-system, sans-serif`
      ctx.fillStyle = '#64748b'
      ctx.textAlign = 'center'
      ctx.fillText(label.length > 18 ? label.slice(0, 18) + '…' : label, node.x, node.y + r + 4)
    }
  }, [highlightedIds, visibleTypes])

  const linkColor = useCallback((link) => {
    const src = typeof link.source === 'object' ? link.source?.id : link.source
    const tgt = typeof link.target === 'object' ? link.target?.id : link.target
    if (highlightSet.has(src) && highlightSet.has(tgt)) return '#3b82f6'
    return link.edge_type === 'primary' ? 'rgba(148,163,184,0.25)' : 'rgba(148,163,184,0.1)'
  }, [highlightedIds])

  const linkWidth = useCallback((link) => {
    const src = typeof link.source === 'object' ? link.source?.id : link.source
    const tgt = typeof link.target === 'object' ? link.target?.id : link.target
    if (highlightSet.has(src) && highlightSet.has(tgt)) return 2.5
    return link.edge_type === 'primary' ? 0.8 : 0.4
  }, [highlightedIds])

  // Animated particles only on highlighted path edges — shows flow direction
  const linkDirectionalParticles = useCallback((link) => {
    const src = typeof link.source === 'object' ? link.source?.id : link.source
    const tgt = typeof link.target === 'object' ? link.target?.id : link.target
    return (highlightSet.has(src) && highlightSet.has(tgt)) ? 5 : 0
  }, [highlightedIds])

  // Particle color — bright white so they're visible against the blue edge
  const linkDirectionalParticleColor = useCallback((link) => {
    const src = typeof link.source === 'object' ? link.source?.id : link.source
    const tgt = typeof link.target === 'object' ? link.target?.id : link.target
    return (highlightSet.has(src) && highlightSet.has(tgt)) ? '#ffffff' : 'rgba(148,163,184,0.3)'
  }, [highlightedIds])

  // Larger arrow on highlighted edges so direction is unmistakable
  const linkDirectionalArrowLength = useCallback((link) => {
    const src = typeof link.source === 'object' ? link.source?.id : link.source
    const tgt = typeof link.target === 'object' ? link.target?.id : link.target
    return (highlightSet.has(src) && highlightSet.has(tgt)) ? 8 : 3
  }, [highlightedIds])

  const handleNodeClick = useCallback((node) => {
    fetch(`${API}/graph/node/${node.id}`)
      .then(r => r.json())
      .then(data => onNodeClick(node, data.connections))
      .catch(() => onNodeClick(node, []))
    if (fgRef.current) {
      fgRef.current.centerAt(node.x, node.y, 600)
      fgRef.current.zoom(3, 600)
    }
  }, [onNodeClick])

  if (loading) {
    return (
      <div className="w-full h-full flex items-center justify-center" style={{ background: '#0a0a0a' }}>
        <div className="flex flex-col items-center gap-3">
          <div className="w-8 h-8 border-2 border-blue-400 border-t-transparent rounded-full animate-spin" />
          <p className="text-sm text-slate-400">Loading graph...</p>
        </div>
      </div>
    )
  }

  return (
    <div className="w-full h-full" style={{ background: '#0a0a0a' }}>
      <ForceGraph2D
        ref={fgRef}
        graphData={filteredData}
        nodeCanvasObject={nodeCanvasObject}
        nodeCanvasObjectMode={() => 'replace'}
        linkColor={linkColor}
        linkWidth={linkWidth}
        linkDirectionalArrowLength={linkDirectionalArrowLength}
        linkDirectionalArrowRelPos={1}
        linkDirectionalArrowColor={linkColor}
        linkDirectionalParticles={linkDirectionalParticles}
        linkDirectionalParticleSpeed={0.006}
        linkDirectionalParticleWidth={3}
        linkDirectionalParticleColor={linkDirectionalParticleColor}
        onNodeClick={handleNodeClick}
        cooldownTicks={80}
        nodeRelSize={4}
        backgroundColor="#0a0a0a"
        linkCurvature={0.1}
        d3AlphaDecay={0.03}
        d3VelocityDecay={0.3}
      />
    </div>
  )
}