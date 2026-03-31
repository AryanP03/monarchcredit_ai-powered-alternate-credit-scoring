import { useState, useRef, useEffect } from 'react'
import './Chatbot.css'

const CHATBOT_API = 'http://localhost:8999'

const SendIcon = () => (
  <svg viewBox="0 0 24 24" fill="none" className="chatbot-icon-sm" stroke="currentColor" strokeWidth="2">
    <path d="M22 2L11 13" strokeLinecap="round" strokeLinejoin="round" />
    <path d="M22 2L15 22L11 13L2 9L22 2Z" strokeLinecap="round" strokeLinejoin="round" />
  </svg>
)

const BotIcon = () => (
  <svg viewBox="0 0 24 24" fill="none" className="chatbot-icon-md" stroke="currentColor" strokeWidth="1.8">
    <rect x="3" y="8" width="18" height="13" rx="2" />
    <path d="M9 8V6a3 3 0 016 0v2" />
    <circle cx="9" cy="14" r="1.5" fill="currentColor" stroke="none" />
    <circle cx="15" cy="14" r="1.5" fill="currentColor" stroke="none" />
    <path d="M9 18h6" strokeLinecap="round" />
  </svg>
)

const CloseIcon = () => (
  <svg viewBox="0 0 24 24" fill="none" className="chatbot-icon-md" stroke="currentColor" strokeWidth="2">
    <path d="M18 6L6 18M6 6l12 12" strokeLinecap="round" />
  </svg>
)

const ChevronIcon = () => (
  <svg viewBox="0 0 24 24" fill="none" className="chatbot-icon-md" stroke="currentColor" strokeWidth="2">
    <path d="M19 9l-7 7-7-7" strokeLinecap="round" strokeLinejoin="round" />
  </svg>
)

const SparkleIcon = () => (
  <svg viewBox="0 0 24 24" fill="currentColor" className="chatbot-icon-sm">
    <path d="M12 2l2.4 7.4H22l-6.2 4.5 2.4 7.4L12 17l-6.2 4.3 2.4-7.4L2 9.4h7.6z" />
  </svg>
)

const TypingDots = () => (
  <div className="chatbot-typing">
    <span />
    <span />
    <span />
  </div>
)

function MessageBubble({ msg }) {
  const isUser = msg.role === 'user'

  return (
    <div className={`chatbot-message-row ${isUser ? 'user' : 'assistant'}`}>
      {!isUser && (
        <div className="chatbot-avatar">
          <BotIcon />
        </div>
      )}

      <div className="chatbot-message-col">
        {msg.image && (
          <div className="chatbot-image-wrap">
            <img
              src={`data:image/png;base64,${msg.image}`}
              alt="SHAP explanation"
              className="chatbot-image"
            />
          </div>
        )}

        {msg.content && (
          <div className={`chatbot-bubble ${isUser ? 'chatbot-bubble-user' : 'chatbot-bubble-bot'}`}>
            {msg.content}
          </div>
        )}

        <span className="chatbot-time">
          {new Date(msg.ts).toLocaleTimeString([], {
            hour: '2-digit',
            minute: '2-digit',
          })}
        </span>
      </div>
    </div>
  )
}

export default function Chatbot({ applicationId }) {
  const [isOpen, setIsOpen] = useState(false)
  const [isMinimized, setIsMinimized] = useState(false)
  const [messages, setMessages] = useState([
    {
      role: 'assistant',
      content:
        "Hi! I'm your AI credit assistant 👋\n\nI can explain your credit score, SHAP breakdown, and answer finance-related questions.",
      ts: Date.now(),
    },
  ])
  const [input, setInput] = useState('')
  const [isStreaming, setIsStreaming] = useState(false)
  const [unread, setUnread] = useState(0)

  const bottomRef = useRef(null)
  const inputRef = useRef(null)

  useEffect(() => {
    if (isOpen && !isMinimized) {
      bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
    }
  }, [messages, isOpen, isMinimized])

  useEffect(() => {
    if (isOpen && !isMinimized) {
      setTimeout(() => inputRef.current?.focus(), 200)
      setUnread(0)
    }
  }, [isOpen, isMinimized])

  const buildHistory = () =>
    messages
      .filter((m) => m.content)
      .slice(-8)
      .map((m) => ({ role: m.role, content: m.content }))

  const sendMessage = async () => {
    const text = input.trim()
    if (!text || isStreaming) return

    const userMsg = { role: 'user', content: text, ts: Date.now() }
    setMessages((prev) => [...prev, userMsg])
    setInput('')
    setIsStreaming(true)

    const assistantId = Date.now() + 1
    setMessages((prev) => [
      ...prev,
      { id: assistantId, role: 'assistant', content: '', image: null, ts: assistantId },
    ])

    try {
      const response = await fetch(`${CHATBOT_API}/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          message: text,
          history: buildHistory(),
          application_id: applicationId || null,
        }),
      })

      if (!response.ok || !response.body) {
        throw new Error('Server error')
      }

      const reader = response.body.getReader()
      const decoder = new TextDecoder()
      let buffer = ''

      while (true) {
        const { done, value } = await reader.read()
        if (done) break

        buffer += decoder.decode(value, { stream: true })
        const lines = buffer.split('\n')
        buffer = lines.pop() || ''

        for (const line of lines) {
          if (!line.startsWith('data: ')) continue
          const raw = line.slice(6).trim()

          if (raw === '[DONE]') {
            setIsStreaming(false)
            if (!isOpen || isMinimized) setUnread((u) => u + 1)
            break
          }

          try {
            const parsed = JSON.parse(raw)

            if (parsed.image) {
              setMessages((prev) =>
                prev.map((m) => (m.id === assistantId ? { ...m, image: parsed.image } : m))
              )
            }

            if (parsed.token) {
              setMessages((prev) =>
                prev.map((m) =>
                  m.id === assistantId ? { ...m, content: m.content + parsed.token } : m
                )
              )
            }
          } catch {
            // ignore malformed chunk
          }
        }
      }
    } catch (err) {
      setMessages((prev) =>
        prev.map((m) =>
          m.id === assistantId
            ? { ...m, content: "Sorry, I couldn't connect to the chatbot server." }
            : m
        )
      )
      setIsStreaming(false)
    }
  }

  const handleKeyDown = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      sendMessage()
    }
  }

  return (
    <>
      {!isOpen && (
        <button
          className="chatbot-fab"
          onClick={() => {
            setIsOpen(true)
            setIsMinimized(false)
            setUnread(0)
          }}
          aria-label="Open chatbot"
        >
          <BotIcon />
          {unread > 0 && <span className="chatbot-badge">{unread}</span>}
        </button>
      )}

      {isOpen && (
        <div className="chatbot-window" style={{ height: isMinimized ? 'auto' : '580px' }}>
          <div className="chatbot-header">
            <div className="chatbot-header-left">
              <div className="chatbot-header-avatar">
                <BotIcon />
              </div>

              <div>
                <div className="chatbot-title-row">
                  <span className="chatbot-title">Credit Assistant</span>
                  <SparkleIcon />
                </div>
                <div className="chatbot-status-row">
                  <span className="chatbot-status-dot" />
                  <span className="chatbot-status-text">
                    {isStreaming ? 'Typing...' : 'Online'}
                  </span>
                </div>
              </div>
            </div>

            <div className="chatbot-header-actions">
              <button
                className="chatbot-header-btn"
                onClick={() => setIsMinimized((v) => !v)}
                aria-label="Minimize chatbot"
              >
                <ChevronIcon />
              </button>

              <button
                className="chatbot-header-btn"
                onClick={() => setIsOpen(false)}
                aria-label="Close chatbot"
              >
                <CloseIcon />
              </button>
            </div>
          </div>

          {!isMinimized && (
            <>
              {applicationId && (
                <div className="chatbot-session-strip">
                  Analysing current application
                </div>
              )}

              <div className="chatbot-messages">
                {messages.map((msg, i) => (
                  <MessageBubble key={msg.id || i} msg={msg} />
                ))}

                {isStreaming &&
                  messages[messages.length - 1]?.content === '' &&
                  !messages[messages.length - 1]?.image && <TypingDots />}

                <div ref={bottomRef} />
              </div>

              {messages.length === 1 && (
                <div className="chatbot-suggestions">
                  {[
                    'Why was this applicant rejected?',
                    'Explain SHAP chart',
                    'Compare risk factors',
                    'What could improve approval chances?',
                  ].map((s) => (
                    <button
                      key={s}
                      className="chatbot-suggestion-btn"
                      onClick={() => {
                        setInput(s)
                        setTimeout(() => inputRef.current?.focus(), 50)
                      }}
                    >
                      {s}
                    </button>
                  ))}
                </div>
              )}

              <div className="chatbot-input-wrap">
                <textarea
                  ref={inputRef}
                  value={input}
                  onChange={(e) => setInput(e.target.value)}
                  onKeyDown={handleKeyDown}
                  placeholder="Ask about this application..."
                  className="chatbot-input"
                  rows={1}
                  disabled={isStreaming}
                  onInput={(e) => {
                    e.target.style.height = 'auto'
                    e.target.style.height = Math.min(e.target.scrollHeight, 112) + 'px'
                  }}
                />

                <button
                  className="chatbot-send-btn"
                  onClick={sendMessage}
                  disabled={!input.trim() || isStreaming}
                  aria-label="Send message"
                >
                  <SendIcon />
                </button>
              </div>

              <div className="chatbot-footer">
                Powered by <span>MonarchCredit AI</span>
              </div>
            </>
          )}
        </div>
      )}
    </>
  )
}