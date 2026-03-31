import { useEffect, useState } from 'react'
import { useNavigate, useLocation } from 'react-router-dom'
import styles from './ProcessingPage.module.css'
import axios from 'axios'

export default function ProcessingPage() {
  const navigate = useNavigate()
  const location = useLocation()

  const applicationId = location.state?.applicationId
  const loanType = location.state?.loanType || 'msme'
  const preview = location.state?.preview

  const [statusText, setStatusText] = useState('Submitting application...')
  const [error, setError] = useState('')
  const [progress, setProgress] = useState(5)
  const [done, setDone] = useState(false)

  useEffect(() => {
    if (!applicationId) {
      setError('Missing application ID.')
      return
    }

    let stopped = false
    let timer = null

    const getResultUrl = () => {
      if (loanType === 'individual') {
        return `http://127.0.0.1:8000/api/customer/individual/${applicationId}/result`
      }
      return `http://127.0.0.1:8000/api/customer/msme/${applicationId}/result`
    }

    const poll = async () => {
      try {
        const res = await axios.get(getResultUrl())
        const data = res.data
        const status = data?.status
        const decision = data?.manager_decision

        if (status === 'pending') {
          setStatusText('Parsing financial data...')
          setProgress(20)
          setDone(false)
        } else if (status === 'scored') {
          setStatusText('Running ML risk scoring...')
          setProgress(40)
          setDone(false)
        } else if (status === 'explained') {
          setStatusText('Generating AI explanations...')
          setProgress(60)
          setDone(false)
        } else if (status === 'ready_for_manager') {
          setStatusText('Waiting for manager review...')
          setProgress(80)
          setDone(false)
        } else if (status === 'reviewed' && decision) {
          setStatusText('Final decision ready. Redirecting to your result...')
          setProgress(100)
          setDone(true)

          if (!stopped) {
            setTimeout(() => {
              navigate('/result', {
                replace: true,
                state: {
                  applicationId,
                  loanType,
                  preview,
                },
              })
            }, 1200)
          }
          return
        } else {
          setStatusText('Processing your application...')
          setDone(false)
        }

        timer = setTimeout(poll, 3000)
      } catch (err) {
        setError(err?.response?.data?.detail || 'Failed to fetch application status.')
      }
    }

    poll()

    return () => {
      stopped = true
      if (timer) clearTimeout(timer)
    }
  }, [applicationId, loanType, navigate, preview])

  return (
    <div className={styles.page}>
      <div className={styles.topBar}>
        <div className={styles.topLogo}>
          <span className={styles.logoMark}>
            <svg width="13" height="13" viewBox="0 0 14 14" fill="none">
              <path d="M2 11L4.5 5L7 8.5L9.5 5L12 11H2Z" fill="white" />
              <rect x="2" y="11.2" width="10" height="1.8" rx="0.9" fill="white" />
            </svg>
          </span>
          MonarchCredit
        </div>

        <div className={styles.appIdBadge}>
          App ID: <strong>{applicationId || 'N/A'}</strong>
        </div>
      </div>

      <div className={styles.center}>
        <div className={styles.card}>
          <div className={`${styles.iconRing} ${done ? styles.iconRingDone : ''}`}>
            {done ? (
              <svg width="32" height="32" viewBox="0 0 32 32" fill="none" className={styles.checkIcon}>
                <path
                  d="M8 16l5 5 10-10"
                  stroke="currentColor"
                  strokeWidth="2.5"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                />
              </svg>
            ) : (
              <div className={styles.pulseRing} />
            )}
          </div>

          <h1 className={styles.title}>
            {done ? 'Decision ready' : 'Processing your application'}
          </h1>

          <p className={styles.sub}>
            {done
              ? 'Your application has completed review and your result is ready.'
              : statusText}
          </p>

          {preview?.name && (
  <p className={styles.sub}>
    {loanType === 'individual' ? 'Applicant' : 'Company'}: {preview.name}
  </p>
)}

          <div className={styles.appIdBox}>
            <span className={styles.appIdLabel}>Application ID</span>
            <span className={styles.appIdValue}>{applicationId || 'N/A'}</span>
            <span className={styles.appIdHint}>Save this for your records</span>
          </div>

          <div className={styles.progressWrap}>
            <div className={styles.progressBar}>
              <div
                className={`${styles.progressFill} ${done ? styles.progressFillDone : ''}`}
                style={{ width: `${progress}%` }}
              />
            </div>
            <span className={styles.progressPct}>{progress}%</span>
          </div>

          <div className={styles.steps}>
            <div className={styles.step}>
              <div className={styles.stepIndicator}>
                {progress >= 20 ? (
                  <svg width="12" height="12" viewBox="0 0 12 12" fill="none">
                    <path
                      d="M2.5 6l2.5 2.5 5-5"
                      stroke="currentColor"
                      strokeWidth="1.5"
                      strokeLinecap="round"
                      strokeLinejoin="round"
                    />
                  </svg>
                ) : (
                  <div className={styles.stepDot} />
                )}
              </div>
              <span className={styles.stepLabel}>Parsing financial data</span>
            </div>

            <div className={styles.step}>
              <div className={styles.stepIndicator}>
                {progress >= 40 ? (
                  <svg width="12" height="12" viewBox="0 0 12 12" fill="none">
                    <path
                      d="M2.5 6l2.5 2.5 5-5"
                      stroke="currentColor"
                      strokeWidth="1.5"
                      strokeLinecap="round"
                      strokeLinejoin="round"
                    />
                  </svg>
                ) : progress >= 20 ? (
                  <div className={styles.stepSpinner} />
                ) : (
                  <div className={styles.stepDot} />
                )}
              </div>
              <span className={styles.stepLabel}>Running ML risk scoring</span>
            </div>

            <div className={styles.step}>
              <div className={styles.stepIndicator}>
                {progress >= 60 ? (
                  <svg width="12" height="12" viewBox="0 0 12 12" fill="none">
                    <path
                      d="M2.5 6l2.5 2.5 5-5"
                      stroke="currentColor"
                      strokeWidth="1.5"
                      strokeLinecap="round"
                      strokeLinejoin="round"
                    />
                  </svg>
                ) : progress >= 40 ? (
                  <div className={styles.stepSpinner} />
                ) : (
                  <div className={styles.stepDot} />
                )}
              </div>
              <span className={styles.stepLabel}>Generating AI explanations</span>
            </div>

            <div className={styles.step}>
              <div className={styles.stepIndicator}>
                {progress >= 80 ? (
                  <svg width="12" height="12" viewBox="0 0 12 12" fill="none">
                    <path
                      d="M2.5 6l2.5 2.5 5-5"
                      stroke="currentColor"
                      strokeWidth="1.5"
                      strokeLinecap="round"
                      strokeLinejoin="round"
                    />
                  </svg>
                ) : progress >= 60 ? (
                  <div className={styles.stepSpinner} />
                ) : (
                  <div className={styles.stepDot} />
                )}
              </div>
              <span className={styles.stepLabel}>Preparing review package</span>
            </div>

            <div className={styles.step}>
              <div className={styles.stepIndicator}>
                {progress >= 100 ? (
                  <svg width="12" height="12" viewBox="0 0 12 12" fill="none">
                    <path
                      d="M2.5 6l2.5 2.5 5-5"
                      stroke="currentColor"
                      strokeWidth="1.5"
                      strokeLinecap="round"
                      strokeLinejoin="round"
                    />
                  </svg>
                ) : progress >= 80 ? (
                  <div className={styles.stepSpinner} />
                ) : (
                  <div className={styles.stepDot} />
                )}
              </div>
              <span className={styles.stepLabel}>Final decision and result</span>
            </div>
          </div>

          {done && (
            <div className={styles.doneNote}>
              Redirecting to your result…
            </div>
          )}

          {error && (
            <div style={{ color: 'red', marginTop: '12px', textAlign: 'center' }}>
              {error}
            </div>
          )}
        </div>
      </div>
    </div>
  )
}