'use client'
import { useState, useCallback, useRef, useEffect } from 'react'
import dynamic from 'next/dynamic'
import NodeInspector from './components/NodeInspector'
import ChatPanel from './components/ChatPanel'
import Legend, { NODE_COLORS } from './components/Legend'

const GraphCanvas = dynamic(() => import('./components/GraphCanvas'), { ssr: false })

export default function Home() {
  const [highlightedIds, setHighlightedIds] = useState([])
  const [selectedNode, setSelectedNode] = useState(null)
  const [selectedConnections, setSelectedConnections] = useState([])
  const [visibleTypes, setVisibleTypes] = useState(new Set(Object.keys(NODE_COLORS)))
  const [chatOpen, setChatOpen] = useState(true)
  const [legendOpen, setLegendOpen] = useState(true)
  const [graphInstance, setGraphInstance] = useState(null)
  const [chatWidth, setChatWidth] = useState(320)
  const isDragging = useRef(false)
  const startX = useRef(0)
  const startWidth = useRef(0)

  const handleNodeClick = useCallback((node, connections) => {
    setSelectedNode(node)
    setSelectedConnections(connections)
  }, [])

  const handleHighlight = useCallback((ids) => {
    setHighlightedIds(ids)
  }, [])

  const toggleType = useCallback((type) => {
    setVisibleTypes(prev => {
      const next = new Set(prev)
      if (next.has(type)) next.delete(type)
      else next.add(type)
      return next
    })
  }, [])

  const onMouseDown = (e) => {
    if (!chatOpen) return
    isDragging.current = true
    startX.current = e.clientX
    startWidth.current = chatWidth
    document.body.style.cursor = 'col-resize'
    document.body.style.userSelect = 'none'
  }

  useEffect(() => {
    const onMouseMove = (e) => {
      if (!isDragging.current) return
      const delta = startX.current - e.clientX
      const newWidth = Math.min(600, Math.max(260, startWidth.current + delta))
      setChatWidth(newWidth)
    }
    const onMouseUp = () => {
      isDragging.current = false
      document.body.style.cursor = ''
      document.body.style.userSelect = ''
    }
    window.addEventListener('mousemove', onMouseMove)
    window.addEventListener('mouseup', onMouseUp)
    return () => {
      window.removeEventListener('mousemove', onMouseMove)
      window.removeEventListener('mouseup', onMouseUp)
    }
  }, [])

  return (
    <div className="flex flex-col h-screen overflow-hidden bg-[#0a0a0a]">
      {/* Top bar */}
      <div className="h-11 bg-[#111111] border-b border-[#222222] flex items-center justify-between px-4 flex-shrink-0 z-30">
        <span className="text-xs font-semibold text-[#ccc]">Order to Cash</span>
        <div className="flex items-center gap-2">
          {highlightedIds.length > 0 && (
            <div className="flex items-center gap-1.5 bg-blue-950/60 border border-blue-800/50 rounded-full px-3 py-1">
              <span className="w-1.5 h-1.5 rounded-full bg-blue-400" />
              <span className="text-xs text-blue-400 font-medium">{highlightedIds.length} nodes highlighted</span>
              <button onClick={() => setHighlightedIds([])} className="text-blue-500 hover:text-blue-300 ml-1 text-xs">×</button>
            </div>
          )}
          <button
            onClick={() => setLegendOpen(p => !p)}
            className="text-xs px-3 py-1.5 border border-[#2a2a2a] rounded-lg text-[#999] hover:bg-[#1a1a1a] hover:text-[#ccc] transition-colors"
          >
            {legendOpen ? 'Hide Legend' : 'Show Legend'}
          </button>
          <button
            onClick={() => setChatOpen(p => !p)}
            className="text-xs px-3 py-1.5 border border-[#2a2a2a] rounded-lg text-[#999] hover:bg-[#1a1a1a] hover:text-[#ccc] transition-colors"
          >
            {chatOpen ? 'Hide Chat' : 'Chat with Graph'}
          </button>
        </div>
      </div>

      <div className="flex flex-1 overflow-hidden">
        <div className="flex-1 relative overflow-hidden">
          <GraphCanvas
            onReady={setGraphInstance}
            highlightedIds={highlightedIds}
            visibleTypes={visibleTypes}
            onNodeClick={handleNodeClick}
          />

          {legendOpen && <Legend visibleTypes={visibleTypes} onToggle={toggleType} />}

          {selectedNode && (
            <NodeInspector
              node={selectedNode}
              connections={selectedConnections}
              onClose={() => setSelectedNode(null)}
            />
          )}
        </div>

        {/* ✅ Always mounted — hidden with CSS only so chat history is preserved */}
        <div
          className="flex flex-shrink-0 transition-all duration-300"
          style={{
            width: chatOpen ? chatWidth : 0,
            overflow: 'hidden',
            opacity: chatOpen ? 1 : 0,
            pointerEvents: chatOpen ? 'auto' : 'none',
          }}
        >
          {/* Drag handle */}
          <div
            onMouseDown={onMouseDown}
            className="w-1 cursor-col-resize flex-shrink-0 hover:bg-[#3a5a8a] transition-colors"
            style={{ minWidth: '4px', background: '#1e1e1e' }}
          />
          <div
            className="flex-1 border-l border-[#1e1e1e] bg-[#0f0f0f] flex flex-col overflow-hidden"
            style={{ width: chatWidth - 4 }}
          >
            <ChatPanel onHighlight={handleHighlight} />
          </div>
        </div>
      </div>
    </div>
  )
}