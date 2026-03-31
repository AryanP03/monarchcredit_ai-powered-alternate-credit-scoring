import { useEffect, useMemo, useState } from 'react'
import { useParams, useNavigate, useLocation } from 'react-router-dom'
import ManagerSidebar from '../../components/ManagerSidebar'
import styles from './DecisionPage.module.css'
import Chatbot from '../../components/Chatbot';

export default function DecisionPage() {
  
  const { id } = useParams()
  const navigate = useNavigate()
const location = useLocation()

  const [application, setApplication] = useState(null)
  const [loadingData, setLoadingData] = useState(true)
  const [serverError, setServerError] = useState('')
  const [notes, setNotes] = useState('')
  const [decision, setDecision] = useState(null) // 'approve' | 'reject'
  const [principal, setPrincipal] = useState(0)
  const [rate, setRate] = useState(0)
  const [submitting, setSubmitting] = useState(false)
  const [submitted, setSubmitted] = useState(false)
  const [overriding, setOverriding] = useState(false)

  const customer = application

  const isMSME = useMemo(() => {
    return !!(customer?.company_name || customer?.registration_number || customer?.entity_type)
  }, [customer])

  const getDisplayName = () => {
  if (customer?.company_name) return customer.company_name
  if (customer?.applicant_name) return customer.applicant_name
  if (customer?.full_name) return customer.full_name
  if (customer?.name) return customer.name
  return 'Applicant'
}

  const getInitials = () => {
    return getDisplayName()
      .split(' ')
      .filter(Boolean)
      .map((n) => n[0])
      .join('')
      .slice(0, 2)
      .toUpperCase()
  }

  const getEntityTypeLabel = () => {
    if (isMSME) return customer?.entity_type || 'MSME'
    return 'Individual'
  }

  const getMetaLine = () => {
  if (isMSME) {
    return `Reg No: ${customer?.registration_number || '—'}`
  }
  return customer?.email || customer?.phone || customer?.primary_phone || 'Individual applicant'
}

  const getSubmittedAtText = () => {
    if (!customer?.created_at) return '—'
    const dt = new Date(customer.created_at)
    if (Number.isNaN(dt.getTime())) return '—'
    return dt.toLocaleString('en-IN')
  }

  const getModelDecisionText = () => {
    if (customer?.decision) return customer.decision
    if (customer?.risk_band) return customer.risk_band
    return 'REVIEW'
  }

  const getModelDecisionChipClass = () => {
    if (customer?.decision === 'APPROVED') return 'chip-green'
    if (customer?.decision === 'REJECTED') return 'chip-red'
    return 'chip-neutral'
  }

  const getModelRecommendationClass = () => {
    if (customer?.decision === 'APPROVED') return styles.modelRecGreen
    if (customer?.decision === 'REJECTED') return styles.modelRecRed
    return ''
  }

  const getRecommendationSubtext = () => {
    if (customer?.decision === 'APPROVED') {
      return `Suggested principal ₹${Number(customer?.suggested_principal || 0).toLocaleString(
        'en-IN'
      )} at ${customer?.suggested_interest_rate || 0}% p.a.`
    }
    if (customer?.decision === 'REJECTED') {
      return 'Model found insufficient creditworthiness signals.'
    }
    return 'Borderline case requiring manual review.'
  }

  const getDecisionEndpoint = () => {
    if (customer?.application_type === 'individual') {
      return `http://127.0.0.1:8000/manager/individual/${id}/final-decision`
    }
    if (customer?.application_type === 'msme') {
      return `http://127.0.0.1:8000/manager/msme/${id}/final-decision`
    }
    return isMSME
      ? `http://127.0.0.1:8000/manager/msme/${id}/final-decision`
      : `http://127.0.0.1:8000/manager/individual/${id}/final-decision`
  }

  const emiMonths = useMemo(() => {
  if (customer?.application_type === 'individual') {
    return (
      Number(customer?.final_tenure) ||
      Number(customer?.approved_tenure) ||
      Number(customer?.loan_tenure_months) ||
      24
    )
  }

  const tenureYears =
    Number(customer?.tenure) ||
    Number(customer?.requested_tenure) ||
    Number(customer?.approved_tenure) ||
    2

  return Math.max(1, Math.round(tenureYears * 12))
}, [customer])

  const estimatedEmi = useMemo(() => {
    if (!(principal > 0 && rate > 0)) return null

    const monthlyRate = rate / 1200
    if (monthlyRate === 0) return Math.round(principal / emiMonths)

    const emi =
      (principal * monthlyRate * Math.pow(1 + monthlyRate, emiMonths)) /
      (Math.pow(1 + monthlyRate, emiMonths) - 1)

    if (!Number.isFinite(emi)) return null
    return Math.round(emi)
  }, [principal, rate, emiMonths])

  useEffect(() => {
  const fetchApplication = async () => {
    try {
      setLoadingData(true)
      setServerError('')

      const hintedType = location.state?.applicationType

      const tryFetch = async (url, forcedType) => {
        const res = await fetch(url)
        const data = await res.json()

        if (!res.ok) {
          throw new Error(data.detail || 'Failed to load application')
        }

        return {
          ...data,
          application_type: data.application_type || forcedType,
        }
      }

      let data

      if (hintedType === 'individual') {
        data = await tryFetch(
          `http://127.0.0.1:8000/api/manager/review/individual/${id}`,
          'individual'
        )
      } else if (hintedType === 'msme') {
        data = await tryFetch(
          `http://127.0.0.1:8000/api/manager/review/${id}`,
          'msme'
        )
      } else {
        try {
          data = await tryFetch(
            `http://127.0.0.1:8000/api/manager/review/individual/${id}`,
            'individual'
          )
        } catch {
          data = await tryFetch(
            `http://127.0.0.1:8000/api/manager/review/${id}`,
            'msme'
          )
        }
      }

      setApplication(data)
      setPrincipal(Number(data.suggested_principal || 0))
      setRate(Number(data.suggested_interest_rate || 0))
    } catch (err) {
      console.error(err)
      setServerError(err.message || 'Failed to load application')
    } finally {
      setLoadingData(false)
    }
  }

  fetchApplication()
}, [id, location.state])

  const handleDownloadPdf = () => {
  setServerError('')

  const pdfUrl =
    customer?.application_type === 'individual'
      ? `http://127.0.0.1:8000/api/manager/individual/${id}/pdf`
      : `http://127.0.0.1:8000/api/manager/review/${id}/pdf`

  window.open(pdfUrl, '_blank', 'noopener,noreferrer')
};

  const handleSubmit = async () => {
    if (!decision || !application) return

    setSubmitting(true)
    setServerError('')

    try {
      const manager = JSON.parse(localStorage.getItem('user') || '{}')

      const res = await fetch(getDecisionEndpoint(), {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
  manager_id: manager?.public_user_id || manager?.id,
  decision,
  principal: decision === 'approve' ? principal : null,
  interest_rate: decision === 'approve' ? rate : null,
  tenure:
    decision === 'approve'
      ? customer?.application_type === 'individual'
        ? Number(customer?.loan_tenure_months || customer?.approved_tenure || 0)
        : null
      : null,
  notes,
}),
      })

      const data = await res.json()

      if (!res.ok) {
        throw new Error(data.detail || 'Failed to submit decision')
      }

      navigate('/manager/dashboard', { replace: true })
    } catch (err) {
      console.error(err)
      setServerError(err.message || 'Failed to submit decision')
    } finally {
      setSubmitting(false)
    }
  }

  if (loadingData) {
    return (
      <div className={styles.page}>
        <ManagerSidebar />
        <Chatbot  />
        <div className={styles.main}>
          <div className={styles.submittedWrap}>
            <div className={styles.submittedCard}>
              <h2 className={styles.submittedTitle}>Loading application...</h2>
            </div>
          </div>
        </div>
      </div>
    )
  }

  if (serverError && !application) {
    return (
      <div className={styles.page}>
        <ManagerSidebar />
        <div className={styles.main}>
          <div className={styles.submittedWrap}>
            <div className={styles.submittedCard}>
              <h2 className={styles.submittedTitle}>Failed to load application</h2>
              <p className={styles.submittedSub}>{serverError}</p>
            </div>
          </div>
        </div>
      </div>
    )
  }

  if (submitted) {
    return (
      <div className={styles.page}>
        <ManagerSidebar />
        <div className={styles.submittedWrap}>
          <div className={styles.submittedCard}>
            <div
              className={`${styles.submittedIcon} ${
                decision === 'approve' ? styles.submittedIconGreen : styles.submittedIconRed
              }`}
            >
              {decision === 'approve' ? (
                <svg width="28" height="28" viewBox="0 0 28 28" fill="none">
                  <path
                    d="M6 14l5 5 11-11"
                    stroke="currentColor"
                    strokeWidth="2.5"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  />
                </svg>
              ) : (
                <svg width="28" height="28" viewBox="0 0 28 28" fill="none">
                  <path
                    d="M7 7l14 14M21 7L7 21"
                    stroke="currentColor"
                    strokeWidth="2.5"
                    strokeLinecap="round"
                  />
                </svg>
              )}
            </div>
            <h2 className={styles.submittedTitle}>Decision submitted</h2>
            <p className={styles.submittedSub}>
              {getDisplayName()} has been {decision === 'approve' ? 'approved' : 'rejected'}.
              Moving to previous customers…
            </p>
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className={styles.page}>
      <ManagerSidebar />
     <Chatbot applicationId={customer?.application_id || id} />
      
      <div className={styles.main}>
        <div className={styles.topBar}>
          <button className={styles.backBtn} onClick={() => navigate('/manager/dashboard')}>
            <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
              <path
                d="M10 3L5 8l5 5"
                stroke="currentColor"
                strokeWidth="1.6"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
            </svg>
            Back to dashboard
          </button>

          <div className={styles.topRight}>
            <span className={styles.topAppId}>{customer?.application_id || id}</span>

            <button className={styles.pdfButton} onClick={handleDownloadPdf}>
              <svg
                className={styles.pdfIcon}
                width="16"
                height="16"
                viewBox="0 0 24 24"
                fill="none"
              >
                <path
                  d="M7 3h7l5 5v13a1 1 0 0 1-1 1H7a1 1 0 0 1-1-1V4a1 1 0 0 1 1-1z"
                  stroke="currentColor"
                  strokeWidth="1.6"
                />
                <path d="M14 3v5h5" stroke="currentColor" strokeWidth="1.6" />
                <path
                  d="M12 17v-6m0 6l-3-3m3 3l3-3"
                  stroke="currentColor"
                  strokeWidth="1.6"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                />
              </svg>
              <span>PDF</span>
            </button>
          </div>
        </div>

        <div className={styles.body}>
          {serverError && (
            <div
              style={{
                marginBottom: '12px',
                padding: '10px 12px',
                borderRadius: '12px',
                background: 'rgba(220, 38, 38, 0.08)',
                color: '#b91c1c',
                fontSize: '14px',
              }}
            >
              {serverError}
            </div>
          )}

          <div className={styles.bodyLeft}>
            <div className={styles.customerHeader}>
              <div className={styles.customerAvatar}>{getInitials()}</div>

              <div className={styles.customerInfo}>
                <h1 className={styles.customerName}>{getDisplayName()}</h1>
                <div className={styles.customerMeta}>
                  <span className="chip chip-neutral" style={{ fontSize: 11 }}>
                    {getEntityTypeLabel()}
                  </span>
                  <span className={styles.metaDot} />
                  <span className={styles.metaText}>{getMetaLine()}</span>
                  <span className={styles.metaDot} />
                  <span className={styles.metaText}>Submitted {getSubmittedAtText()}</span>
                </div>
              </div>

              <div className={styles.loanAskBadge}>
                <span className={styles.loanAskLabel}>Loan requested</span>
                <span className={styles.loanAskVal}>
                  ₹{Number(customer?.loan_ask || 0).toLocaleString('en-IN')}
                </span>
              </div>
            </div>

            <div className={`${styles.modelRec} ${getModelRecommendationClass()}`}>
              <div className={styles.modelRecIcon}>
                <svg width="18" height="18" viewBox="0 0 18 18" fill="none">
                  <circle cx="9" cy="9" r="7" stroke="currentColor" strokeWidth="1.5" />
                  <path
                    d="M6 9.5l2 2 4-4"
                    stroke="currentColor"
                    strokeWidth="1.5"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  />
                </svg>
              </div>

              <div>
                <div className={styles.modelRecTitle}>
                  AI recommendation: <strong>{getModelDecisionText()}</strong>
                </div>
                <div className={styles.modelRecSub}>{getRecommendationSubtext()}</div>
              </div>

              <div className={styles.modelRecRight}>
                <span className={`chip ${getModelDecisionChipClass()}`}>
                  Model: {getModelDecisionText()}
                </span>
              </div>
            </div>

            <div className={styles.section}>
              <h2 className={styles.sectionTitle}>SHAP explanation</h2>
              <p className={styles.sectionSub}>
                Feature importance contributing to this prediction
              </p>

              <div className={styles.shapList}>
                {customer?.shap_plot_base64 ? (
                  <img
                    src={`data:image/png;base64,${customer.shap_plot_base64}`}
                    alt="SHAP explanation"
                    style={{ width: '100%', borderRadius: '12px' }}
                  />
                ) : (
                  <p>No SHAP plot available yet.</p>
                )}
              </div>
            </div>

            <div className={styles.overrideToggleRow}>
              <span className={styles.overrideToggleLabel}>Override suggested values</span>
              <button
                className={`${styles.toggleSwitch} ${overriding ? styles.toggleSwitchOn : ''}`}
                onClick={() => setOverriding((o) => !o)}
                aria-pressed={overriding}
              >
                <div className={styles.toggleThumb} />
              </button>
            </div>
          </div>

          <div className={styles.decisionPanel}>
            <h2 className={styles.panelTitle}>Your decision</h2>
            <p className={styles.panelSub}>
              Review the AI recommendation and make a final call. You can override the suggested
              terms.
            </p>

            <div className={styles.decisionBtns}>
              <button
                className={`${styles.decisionBtn} ${styles.decisionBtnApprove} ${
                  decision === 'approve' ? styles.decisionBtnSelected : ''
                }`}
                onClick={() => setDecision('approve')}
              >
                <div className={styles.decisionBtnIcon}>
                  <svg width="18" height="18" viewBox="0 0 18 18" fill="none">
                    <path
                      d="M4 9l3.5 3.5 6.5-7"
                      stroke="currentColor"
                      strokeWidth="2"
                      strokeLinecap="round"
                      strokeLinejoin="round"
                    />
                  </svg>
                </div>
                <span className={styles.decisionBtnLabel}>Approve</span>
                <span className={styles.decisionBtnSub}>Grant the loan</span>
              </button>

              <button
                className={`${styles.decisionBtn} ${styles.decisionBtnReject} ${
                  decision === 'reject' ? styles.decisionBtnSelected : ''
                }`}
                onClick={() => setDecision('reject')}
              >
                <div className={styles.decisionBtnIcon}>
                  <svg width="18" height="18" viewBox="0 0 18 18" fill="none">
                    <path
                      d="M5 5l8 8M13 5l-8 8"
                      stroke="currentColor"
                      strokeWidth="2"
                      strokeLinecap="round"
                    />
                  </svg>
                </div>
                <span className={styles.decisionBtnLabel}>Reject</span>
                <span className={styles.decisionBtnSub}>Decline application</span>
              </button>
            </div>

            {decision === 'approve' && (
              <div className={styles.approveInputs}>
                <div className={styles.inputGroup}>
                  <label htmlFor="principal">
                    Principal amount (₹)
                    {!overriding && <span className={styles.suggestedTag}>suggested</span>}
                  </label>
                  <input
                    id="principal"
                    className="input"
                    type="number"
                    value={principal}
                    onChange={(e) => setPrincipal(Number(e.target.value))}
                    disabled={!overriding}
                    min={10000}
                    max={5000000}
                  />
                </div>

                <div className={styles.inputGroup}>
                  <label htmlFor="rate">
                    Interest rate (% p.a.)
                    {!overriding && <span className={styles.suggestedTag}>suggested</span>}
                  </label>
                  <input
                    id="rate"
                    className="input"
                    type="number"
                    step="0.1"
                    value={rate}
                    onChange={(e) => setRate(Number(e.target.value))}
                    disabled={!overriding}
                    min={6}
                    max={36}
                  />
                </div>

                {estimatedEmi !== null && (
                  <div className={styles.emiPreview}>
                    <span className={styles.emiPreviewLabel}>
                      Estimated monthly EMI ({emiMonths} months)
                    </span>
                    <span className={styles.emiPreviewVal}>
                      ₹{estimatedEmi.toLocaleString('en-IN')}
                    </span>
                  </div>
                )}
              </div>
            )}

            <div className={styles.notesField}>
              <label htmlFor="notes">Notes (optional)</label>
              <textarea
                id="notes"
                className={`input ${styles.textarea}`}
                placeholder="Add any internal notes about this decision…"
                rows={3}
                value={notes}
                onChange={(e) => setNotes(e.target.value)}
              />
            </div>

            <button
              className={`btn btn-primary ${styles.submitBtn}`}
              onClick={handleSubmit}
              disabled={!decision || submitting}
            >
              {submitting ? (
                <>
                  <span className={styles.spinner} /> Submitting…
                </>
              ) : (
                <>Submit decision →</>
              )}
            </button>

            {!decision && (
              <p className={styles.decisionHint}>Select approve or reject to continue</p>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}