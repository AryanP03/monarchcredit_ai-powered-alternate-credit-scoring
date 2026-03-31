import { useState, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import axios from 'axios'
import ManagerSidebar from '../../components/ManagerSidebar'
import styles from './ManagerDashboard.module.css'
import Chatbot from '../../components/Chatbot'
export default function ManagerDashboard() {
  const [tab, setTab] = useState('active')
  const [modelHealthy] = useState(true)
  const [customers, setCustomers] = useState([])
  const [loading, setLoading] = useState(true)
  const [downloading, setDownloading] = useState(false)
  const navigate = useNavigate()

  const fetchActiveCustomers = async () => {
  try {
    setLoading(true)

    const [msmeRes, individualRes] = await Promise.allSettled([
      axios.get('http://localhost:8000/api/manager/dashboard/active'),
      axios.get('http://localhost:8000/api/manager/dashboard/individual'),
    ])

    const msmeData =
      msmeRes.status === 'fulfilled'
        ? Array.isArray(msmeRes.value.data)
          ? msmeRes.value.data.map((item) => ({
              ...item,
              application_type: item.application_type || 'msme',
            }))
          : []
        : []

    const individualRaw =
      individualRes.status === 'fulfilled'
        ? individualRes.value.data?.items || []
        : []

    const individualData = Array.isArray(individualRaw)
      ? individualRaw.map((item) => ({
          ...item,
          application_type: item.application_type || 'individual',
        }))
      : []

      console.log('MSME data:', msmeData)
console.log('Individual data:', individualData)
console.log('Combined data:', [...msmeData, ...individualData])

    setCustomers([...msmeData, ...individualData])
  } catch (err) {
    console.error('Error fetching applications:', err)
    setCustomers([])
  } finally {
    setLoading(false)
  }
}

  useEffect(() => {
  fetchActiveCustomers()
  const interval = setInterval(fetchActiveCustomers, 5000)
  return () => clearInterval(interval)
}, [])

  const activeStatuses = new Set([
  'pending',
  'scored',
  'explained',
  'ready_for_manager',
])

const active = customers.filter((c) =>
  activeStatuses.has((c.status || '').toLowerCase())
)

const previous = customers.filter(
  (c) => (c.status || '').toLowerCase() === 'reviewed'
)

  const getDisplayName = (c) => {
  return c.company_name || c.applicant_name || c.full_name || 'Unknown Entity'
}

  const getAvatarText = (c) => {
    const name = getDisplayName(c)
    return name
      .split(' ')
      .filter(Boolean)
      .map(part => part[0])
      .join('')
      .slice(0, 2)
      .toUpperCase() || 'NA'
  }

  const getMetaText = (c) => {
  if (c.application_type === 'individual') {
    const employment = c.employment_status || 'Individual'
    const phone = c.phone || c.primary_phone || 'No phone'
    return `${employment} · ${phone}`
  }

  const entity = c.entity_type || 'Unknown type'
  const sector =
    c.sector_id !== null && c.sector_id !== undefined
      ? `Sector ${c.sector_id}`
      : 'No sector'

  return `${entity} · ${sector}`
}

  const formatCurrencyINR = (value) => {
    return `₹${Number(value || 0).toLocaleString('en-IN')}`
  }

  const formatDateTime = (value) => {
    if (!value) return '—'
    const date = new Date(value)
    if (Number.isNaN(date.getTime())) return '—'
    return date.toLocaleString('en-IN', {
      day: 'numeric',
      month: 'short',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    })
  }

  const getPredictionLabel = (riskBand) => {
  if (!riskBand) return 'Review'
  const rb = riskBand.toUpperCase()

  if (rb === 'LOW') return 'Approve'
  if (rb === 'MODERATE') return 'Review'
  if (rb === 'HIGH') return 'High Risk'
  if (rb === 'VERY HIGH') return 'Reject'

  return 'Review'
}

const getPredictionChipClass = (riskBand) => {
  if (!riskBand) return 'chip-red'
  const rb = riskBand.toUpperCase()

  if (rb === 'LOW') return 'chip-green'
  if (rb === 'MODERATE') return 'chip-yellow'
  if (rb === 'HIGH') return 'chip-red'
  if (rb === 'VERY HIGH') return 'chip-red'

  return 'chip-red'
}

  const handleDownloadAudit = async () => {
  try {
    setDownloading(true)

    const response = await axios.get(
      'http://localhost:8000/api/manager/dashboard/audit/download',
      { responseType: 'blob' }
    )

    const blob = new Blob([response.data], { type: 'text/csv' })
    const url = window.URL.createObjectURL(blob)

    const link = document.createElement('a')
    link.href = url
    link.download = 'msme_manager_final_decision.csv'
    document.body.appendChild(link)
    link.click()
    link.remove()

    window.URL.revokeObjectURL(url)
  } catch (err) {
    console.error('Error downloading audit CSV:', err)
    alert('Failed to download audit CSV.')
  } finally {
    setDownloading(false)
  }
}

  return (
    <div className={styles.page}>
      <ManagerSidebar modelHealthy={modelHealthy} />
      
      <div className={styles.main}>
        <div className={styles.topBar}>
          <div>
            <h1 className={styles.topTitle}>Dashboard</h1>
            <p className={styles.topSub}>
              {new Date().toLocaleDateString('en-IN', {
                weekday: 'long',
                day: 'numeric',
                month: 'long',
                year: 'numeric'
              })}
            </p>
          </div>

          <div className={styles.topActions}>
            <div className={`${styles.modelPill} ${modelHealthy ? styles.modelPillGreen : styles.modelPillRed}`}>
              <div className={`${styles.modelPillDot} ${modelHealthy ? styles.dotGreen : styles.dotRed}`} />
              {modelHealthy ? 'Model healthy' : 'Anomaly detected'}
            </div>

            <button
              className="btn btn-outline btn-sm"
              onClick={handleDownloadAudit}
              disabled={downloading}
            >
              {downloading ? (
                <><span className={styles.spinner} /> Downloading…</>
              ) : (
                <>
                  <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
                    <path d="M7 1v8M4 6l3 3 3-3M2 11h10" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/>
                  </svg>
                  Download audit
                </>
              )}
            </button>
          </div>
        </div>

        <div className={styles.body}>
          <div className={styles.kpiRow}>
            {[
              { label: 'Awaiting decision', value: active.length, sub: 'Active applications', color: 'var(--blue)' },
              { label: 'Reviewed', value: previous.length, sub: 'Completed reviews', color: 'var(--success)' },
              { label: 'Pending', value: customers.filter(c => c.status === 'pending').length, sub: 'Waiting in queue', color: 'var(--danger)' },
              { label: 'Total records', value: customers.length, sub: 'All applications', color: 'var(--ink)' },
            ].map((k, i) => (
              <div key={i} className={styles.kpiCard}>
                <span className={styles.kpiLabel}>{k.label}</span>
                <span className={styles.kpiValue} style={{ color: k.color }}>{k.value}</span>
                <span className={styles.kpiSub}>{k.sub}</span>
              </div>
            ))}
          </div>

          <div className={styles.tabRow}>
            <div className={styles.tabs}>
              <button
                className={`${styles.tab} ${tab === 'active' ? styles.tabActive : ''}`}
                onClick={() => setTab('active')}
              >
                Active customers
                {active.length > 0 && <span className={styles.tabBadge}>{active.length}</span>}
              </button>

              <button
                className={`${styles.tab} ${tab === 'previous' ? styles.tabActive : ''}`}
                onClick={() => setTab('previous')}
              >
                Previous customers
              </button>
            </div>
          </div>

          {tab === 'active' && (
            <div className={styles.tableWrap}>
              <div className={styles.tableHead}>
                <span>Customer</span>
                <span>App ID</span>
                <span>Loan ask</span>
                <span>Model prediction</span>
                <span>Submitted</span>
                <span></span>
              </div>

              {loading ? (
                <div className={styles.emptyState}><p>Loading applications...</p></div>
              ) : active.length === 0 ? (
                <div className={styles.emptyState}>
                  <p>No active applications</p>
                </div>
              ) : (
                active.map((c) => (
                  <div key={c.application_id} className={styles.tableRow}>
                    <div className={styles.customerCell}>
                      <div className={styles.customerAvatar}>{getAvatarText(c)}</div>
                      <div>
                        <div className={styles.customerName}>{getDisplayName(c)}</div>
                        <div className={styles.customerMeta}>{getMetaText(c)}</div>
                      </div>
                    </div>

                    <div className={styles.appId}>{c.application_id}</div>
                    <div className={styles.amount}>{formatCurrencyINR(c.loan_ask)}</div>

                    <div>
                      <span className={`chip ${getPredictionChipClass(c.risk_band)}`}>
                        {getPredictionLabel(c.risk_band)}
                      </span>
                    </div>

                    <div className={styles.timeCell}>{formatDateTime(c.created_at)}</div>

                    <div>
                      <button
                        className={styles.reviewBtn}
                        onClick={() =>
  navigate(`/manager/review/${c.application_id}`, {
    state: { applicationType: c.application_type },
  })
}
                      >
                        Review →
                      </button>
                    </div>
                  </div>
                ))
              )}
            </div>
          )}

          {tab === 'previous' && (
            <div className={styles.tableWrap}>
              <div className={styles.tableHeadPrev}>
                <span>Customer</span>
                <span>App ID</span>
                <span>Loan ask</span>
                <span>Decision</span>
                <span>Rate</span>
                <span>Date</span>
              </div>

              {previous.length === 0 ? (
                <div className={styles.emptyState}>
                  <p>No reviewed applications</p>
                </div>
              ) : (
                previous.map((c) => (
                  <div key={c.application_id} className={`${styles.tableRow} ${styles.tableRowPrev}`}>
                    <div className={styles.customerCell}>
                      <div className={styles.customerAvatar}>{getAvatarText(c)}</div>
                      <div>
                        <div className={styles.customerName}>{getDisplayName(c)}</div>
                        <div className={styles.customerMeta}>{getMetaText(c)}</div>
                      </div>
                    </div>

                    <div className={styles.appId}>{c.application_id}</div>
                    <div className={styles.amount}>{formatCurrencyINR(c.loan_ask)}</div>

                    <div>
                      <span className="chip chip-green">
  {c.manager_decision || 'Reviewed'}
</span>
                    </div>

                    <div className={styles.rateCell}>
                      {c.suggested_interest_rate ? `${c.suggested_interest_rate}% p.a.` : '—'}
                    </div>

                    <div className={styles.timeCell}>{formatDateTime(c.updated_at)}</div>
                  </div>
                ))
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  )
} 