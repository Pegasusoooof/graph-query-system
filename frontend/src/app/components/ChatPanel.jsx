'use client'
import { useState, useRef, useEffect } from 'react'

const API = process.env.NEXT_PUBLIC_API_URL

export default function ChatPanel({ onHighlight }) {
  const [messages, setMessages] = useState([
    { role: 'assistant', text: 'Hi! I can help you analyze the **Order to Cash** process.' }
  ])
  const [input, setInput] = useState('')
  const [loading, setLoading] = useState(false)
  const [history, setHistory] = useState([])
  const scrollRef = useRef(null)

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight
    }
  }, [messages])

  const send = async () => {
    const msg = input.trim()
    if (!msg || loading) return
    setInput('')
    setLoading(true)

    setMessages(prev => [...prev, { role: 'user', text: msg }])
    setMessages(prev => [...prev, { role: 'assistant', text: '', streaming: true }])

    try {
      const res = await fetch(`${API}/query/stream`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          message: msg,
          conversation_history: history,
          highlighted_nodes: [],
        }),
      })

      const reader = res.body.getReader()
      const decoder = new TextDecoder()
      let fullText = ''
      let highlighted = []

      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        const chunk = decoder.decode(value)
        const lines = chunk.split('\n').filter(l => l.startsWith('data: '))
        for (const line of lines) {
          try {
            const data = JSON.parse(line.slice(6))
            if (data.token) {
              fullText += data.token
              setMessages(prev => {
                const updated = [...prev]
                updated[updated.length - 1] = { role: 'assistant', text: fullText, streaming: true }
                return updated
              })
            }
            if (data.done) highlighted = data.highlighted_nodes || []
          } catch {}
        }
      }

      const cleanText = fullText.replace(/<highlight_nodes>.*?<\/highlight_nodes>/gs, '').trim()
      setMessages(prev => {
        const updated = [...prev]
        updated[updated.length - 1] = { role: 'assistant', text: cleanText, streaming: false }
        return updated
      })
      if (highlighted.length > 0) onHighlight(highlighted)
      setHistory(prev => [
        ...prev,
        { role: 'user', content: msg },
        { role: 'assistant', content: cleanText },
      ])
    } catch {
      setMessages(prev => {
        const updated = [...prev]
        updated[updated.length - 1] = {
          role: 'assistant',
          text: 'Sorry, something went wrong. Make sure the backend is running on port 8000.',
          streaming: false,
        }
        return updated
      })
    }
    setLoading(false)
  }

  const handleKey = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); send() }
  }

  const formatText = (text) => {
    const lines = text.split('\n')
    let html = ''
    let i = 0

    while (i < lines.length) {
      const line = lines[i]

      if (line.trim().startsWith('|')) {
        const tableLines = []
        while (i < lines.length && lines[i].trim().startsWith('|')) {
          tableLines.push(lines[i])
          i++
        }
        const rows = tableLines.filter(l => !/^\s*\|[\s\-|:]+\|\s*$/.test(l))
        if (rows.length > 0) {
          html += '<table style="border-collapse:collapse;width:100%;margin:6px 0;font-size:12px">'
          rows.forEach((row, rowIdx) => {
            const cells = row.split('|').map(c => c.trim()).filter((_, ci, arr) => ci > 0 && ci < arr.length - 1)
            const tag = rowIdx === 0 ? 'th' : 'td'
            const rowStyle = rowIdx === 0 ? 'background:#1a1a1a' : rowIdx % 2 === 0 ? 'background:#111' : 'background:#0e0e0e'
            html += `<tr style="${rowStyle}">`
            cells.forEach(cell => {
              const cellHtml = cell.replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
              html += `<${tag} style="padding:4px 8px;border:1px solid #2a2a2a;color:${tag === 'th' ? '#aaa' : '#c8d0d8'};text-align:left;white-space:nowrap">${cellHtml}</${tag}>`
            })
            html += '</tr>'
          })
          html += '</table>'
        }
        continue
      }

      const formatted = line
        .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
        .replace(/\*(.*?)\*/g, '<em>$1</em>')
      html += `<div class="${line.startsWith('•') ? 'ml-2' : ''}">${formatted || '&nbsp;'}</div>`
      i++
    }

    return html
  }

  return (
    <div className="flex flex-col h-full bg-[#0f0f0f]">
      <div className="px-5 py-4 border-b border-[#1e1e1e]">
        <p className="text-sm font-semibold text-[#e2e8f0]">Chat with Graph</p>
        <p className="text-xs text-[#555]">Order to Cash</p>
      </div>

      <div ref={scrollRef} className="flex-1 overflow-y-auto chat-scroll px-4 py-3 flex flex-col gap-3">
        {messages.map((msg, i) => (
          <div key={i} className={`flex gap-2.5 ${msg.role === 'user' ? 'flex-row-reverse' : 'flex-row'}`}>
            {msg.role === 'assistant' && (
              <div className="w-7 h-7 rounded-full bg-[#1e1e1e] border border-[#2a2a2a] flex items-center justify-center flex-shrink-0 mt-0.5">
                <span className="text-[#ccc] text-xs font-bold">G</span>
              </div>
            )}
            {msg.role === 'user' && (
              <div className="w-7 h-7 rounded-full bg-[#2a2a2a] flex items-center justify-center flex-shrink-0 mt-0.5">
                <span className="text-[#999] text-xs">U</span>
              </div>
            )}
            <div className={`max-w-[80%] ${msg.role === 'user' ? 'items-end' : 'items-start'} flex flex-col`}>
              {msg.role === 'assistant' && (
                <p className="text-xs text-[#555] mb-1 font-medium">Graph AI <span className="font-normal">Graph Agent</span></p>
              )}
              <div
                className={`rounded-2xl px-3.5 py-2.5 text-sm leading-relaxed ${
                  msg.role === 'user'
                    ? 'bg-[#1e2a3a] text-[#e2e8f0] border border-[#1e3a5f] rounded-tr-sm'
                    : 'bg-[#161616] text-[#c8d0d8] border border-[#222] rounded-tl-sm'
                }`}
                dangerouslySetInnerHTML={{ __html: formatText(msg.text) }}
              />
              {msg.streaming && (
                <div className="flex gap-1 mt-1 ml-1">
                  <span className="w-1 h-1 bg-[#444] rounded-full animate-bounce" style={{ animationDelay: '0ms' }} />
                  <span className="w-1 h-1 bg-[#444] rounded-full animate-bounce" style={{ animationDelay: '150ms' }} />
                  <span className="w-1 h-1 bg-[#444] rounded-full animate-bounce" style={{ animationDelay: '300ms' }} />
                </div>
              )}
            </div>
          </div>
        ))}
      </div>

      <div className="px-4 py-3 border-t border-[#1e1e1e]">
        <div className="flex items-center gap-1 mb-2">
          <span className={`w-1.5 h-1.5 rounded-full ${loading ? 'bg-yellow-500 animate-pulse' : 'bg-emerald-500'}`} />
          <span className="text-xs text-[#555]">
            {loading ? 'Analyzing your query...' : 'Ready to analyze — ask anything about the dataset'}
          </span>
        </div>
        <div className="flex gap-2">
          <textarea
            value={input}
            onChange={e => setInput(e.target.value)}
            onKeyDown={handleKey}
            placeholder="Analyze anything"
            rows={1}
            className="flex-1 resize-none text-sm border border-[#2a2a2a] rounded-xl px-3 py-2 focus:outline-none focus:border-[#3a5a8a] text-[#ccc] placeholder-[#3a3a3a] bg-[#161616] transition-colors"
            style={{ minHeight: '40px', maxHeight: '80px' }}
          />
          <button
            onClick={send}
            disabled={loading || !input.trim()}
            className="px-4 py-2 bg-[#1e3a5f] text-[#90c0f0] text-sm rounded-xl hover:bg-[#1e4a7f] disabled:opacity-30 disabled:cursor-not-allowed transition-colors flex-shrink-0 border border-[#1e4a7f]"
          >
            Send
          </button>
        </div>
      </div>
    </div>
  )
}