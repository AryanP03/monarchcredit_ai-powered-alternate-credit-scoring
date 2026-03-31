import { useNavigate } from 'react-router-dom'
import styles from './CustomerDashboard.module.css'
import { useState, useRef, useCallback, useEffect } from 'react'
import axios from 'axios'
import { AlertCircle } from 'lucide-react'

const LOAN_TYPES = [
  {
    id: 'individual',
    label: 'Individual loan',
    desc: 'For salaried or self-employed individuals seeking personal credit.',
    model: 'Individual credit model',
    icon: (
      <svg width="26" height="26" viewBox="0 0 26 26" fill="none">
        <circle cx="13" cy="9" r="5" stroke="currentColor" strokeWidth="1.7" />
        <path d="M4 24c0-4.418 4.03-7 9-7s9 2.582 9 7" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" />
      </svg>
    ),
    checks: ['Income regularity & stability', 'Savings & spending patterns', 'Existing obligations', 'Transaction history depth', 'Cash-flow consistency'],
  },
  {
    id: 'msme',
    label: 'MSME loan',
    desc: 'For micro, small & medium enterprises seeking business credit.',
    model: 'MSME credit model',
    icon: (
      <svg width="26" height="26" viewBox="0 0 26 26" fill="none">
        <rect x="2" y="11" width="22" height="13" rx="2" stroke="currentColor" strokeWidth="1.7" />
        <path d="M8 11V8a5 5 0 0110 0v3" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" />
        <path d="M9 18h8M13 15v6" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" />
      </svg>
    ),
    checks: ['Business revenue trends', 'GST & turnover data', 'Vendor payment history', 'Working capital flow', 'Cash-flow consistency'],
  },
]

const steps = [
  { id: 1, label: 'Choose loan type', desc: 'Individual or MSME' },
  { id: 2, label: 'Upload data', desc: 'Share your AA JSON file' },
  { id: 3, label: 'AI processes', desc: 'Model runs in seconds' },
  { id: 4, label: 'Manager review', desc: 'Human-in-the-loop decision' },
  { id: 5, label: 'Final result', desc: 'Approval or rejection report' },
]

export default function CustomerDashboard() {
  const [stage, setStage] = useState('select')
  const [loanType, setLoanType] = useState(null)
  const [file, setFile] = useState(null)
  const [dragging, setDragging] = useState(false)
  const [userName, setUserName] = useState('')
  const [parseErr, setParseErr] = useState('')
  const [preview, setPreview] = useState(null)
  const [jsonData, setJsonData] = useState(null)
  const [submitting, setSubmitting] = useState(false)

  const fileRef = useRef()
  const navigate = useNavigate()

  const selected = LOAN_TYPES.find((t) => t.id === loanType)
  const activeStepIndex = stage === 'select' ? 0 : 1

  const handleSignOut = () => {
    localStorage.removeItem('user')
    localStorage.removeItem('token')
    navigate('/auth?mode=signin')
  }

  const handleChoose = (id) => {
    setLoanType(id)
    setStage('upload')
    setFile(null)
    setPreview(null)
    setJsonData(null)
    setParseErr('')
  }

  const processFile = useCallback((f) => {
    setParseErr('')
    setPreview(null)

    if (!f) return

    if (!f.name.endsWith('.json')) {
      setParseErr('Please upload a valid .json file.')
      return
    }

    const reader = new FileReader()

    reader.onload = (e) => {
      try {
        const data = JSON.parse(e.target.result)

        if (loanType === 'individual') {
          if (
            !data?.application_meta ||
            !data?.customer_profile ||
            !data?.contact_details ||
            !data?.address_details ||
            !data?.loan_details ||
            !data?.documents_submitted ||
            !data?.credit_history ||
            !data?.external_credit_scores ||
            !data?.raw_model_input_fields ||
            !data?.internal_derived_fields
          ) {
            throw new Error('Invalid individual schema')
          }
        }

        console.log('loanType at parse:', loanType)
        console.log('parsed data:', data)
        console.log('msme company:', data?.business_identity?.company_name)
        console.log('msme reg no:', data?.business_identity?.registration_number)
        console.log('msme loan_amt:', data?.loan_requirements?.loan_amt)
        console.log('msme tenure:', data?.loan_requirements?.tenure)

        setJsonData(data)
        setFile(f)

        if (loanType === 'msme') {
          setUserName(data?.business_identity?.company_name || 'User')
          setPreview({
            name: data?.business_identity?.company_name || 'Not found',
            pan: data?.business_identity?.registration_number || '—',
            income: data?.loan_requirements?.loan_amt ?? '—',
            txnCount: data?.loan_requirements?.tenure ?? '—',
          })
        } else {
          setUserName(data?.customer_profile?.personal_details?.full_name || 'User')
          setPreview({
            name: data?.customer_profile?.personal_details?.full_name || 'Not found',
            pan:
              data?.customer_profile?.personal_details?.pan_number ||
              data?.customer_profile?.personal_details?.government_id_number ||
              '—',
            annualIncome: data?.customer_profile?.employment_details?.annual_income_total ?? '—',
            loanAmount: data?.loan_details?.loan_amount ?? '—',
          })
        }

        setParseErr('')
        setStage('upload')
      } catch {
        setParseErr('Could not parse JSON. Please check the file and try again.')
        setJsonData(null)
        setFile(null)
        setPreview(null)
      }
    }

    reader.readAsText(f)
  }, [loanType])

  const onFileChange = (e) => processFile(e.target.files[0])

  const onDrop = (e) => {
    e.preventDefault()
    setDragging(false)
    const f = e.dataTransfer.files?.[0]
    processFile(f)
  }

  useEffect(() => {
    const raw = localStorage.getItem('user')
    if (!raw) return

    try {
      const parsed = JSON.parse(raw)
      const signedInName = parsed?.name || parsed?.user?.name || ''
      setUserName(signedInName)
    } catch {
      setUserName('')
    }
  }, [])

  const handleSubmit = async () => {
    if (!file || !jsonData || !loanType) return

    try {
      setSubmitting(true)

      const endpoint =
        loanType === 'msme'
          ? 'http://localhost:8000/apply/msme'
          : 'http://localhost:8000/apply/individual'

      const response = await axios.post(endpoint, jsonData, {
        headers: {
          'Content-Type': 'application/json',
        },
      })

      console.log('Submission successful:', response.data)

      navigate('/processing', {
        state: {
          loanType,
          applicationId: response.data?.application_id || null,
          preview,
        },
      })
    } catch (err) {
      console.error('Submit failed:', err)

      const detail = err?.response?.data?.detail
      let message = 'Failed to submit application.'

      if (Array.isArray(detail)) {
        message = detail
          .map((d) => `${(d.loc || []).join(' > ')}: ${d.msg}`)
          .join('\n')
      } else if (typeof detail === 'string') {
        message = detail
      }

      setParseErr(message)
    } finally {
      setSubmitting(false)
    }
  }

  const removeFile = () => {
    setFile(null)
    setPreview(null)
    setJsonData(null)
    setParseErr('')
  }

  const getPreviewConfig = () => {
    if (loanType === 'msme') {
      return [
        { label: 'Company Name', value: preview?.name },
        { label: 'Registration No.', value: preview?.pan },
        {
          label: 'Loan Amount',
          value:
            preview?.income !== '—' && preview?.income !== undefined && preview?.income !== null
              ? `₹${Number(preview.income).toLocaleString('en-IN')}`
              : '—',
        },
        {
          label: 'Tenure',
          value: preview?.txnCount !== '—' ? String(preview?.txnCount) : '—',
        },
      ]
    }

    return [
      { label: 'Applicant Name', value: preview?.name },
      { label: 'PAN', value: preview?.pan },
      {
        label: 'Annual Income',
        value:
          preview?.annualIncome !== '—' &&
          preview?.annualIncome !== undefined &&
          preview?.annualIncome !== null
            ? `₹${Number(preview.annualIncome).toLocaleString('en-IN')}`
            : '—',
      },
      {
        label: 'Loan Amount',
        value:
          preview?.loanAmount !== '—' &&
          preview?.loanAmount !== undefined &&
          preview?.loanAmount !== null
            ? `₹${Number(preview.loanAmount).toLocaleString('en-IN')}`
            : '—',
      },
    ]
  }

  return (
    <div className={styles.page}>
      <button className={styles.signOutBtn} onClick={handleSignOut}>
        Sign out
      </button>

      <div className={`container ${styles.layout}`}>
        <main className={styles.main}>
          <div className={styles.greeting}>
            <div className={styles.greetingAvatar}>
              {userName ? userName.split(' ').map((w) => w[0]).join('').slice(0, 2).toUpperCase() : 'U'}
            </div>
            <div>
              <h1 className={styles.greetingTitle}>
                Hello, {userName ? userName.split(' ')[0] : 'User'}
              </h1>
              <p className={styles.greetingSub}>
                {stage === 'select'
                  ? 'Choose your loan type to get started.'
                  : `Applying as: ${selected?.label} — upload your Account Aggregator file.`}
              </p>
            </div>
          </div>

          {stage === 'select' && (
            <div className={styles.selectStage}>
              <div className={styles.selectHead}>
                <h2 className={styles.selectTitle}>What type of loan are you applying for?</h2>
                <p className={styles.selectSub}>
                  We use two separate AI models optimised for each applicant type.
                </p>
              </div>

              <div className={styles.typeGrid}>
                {LOAN_TYPES.map((t) => (
                  <button key={t.id} className={styles.typeCard} onClick={() => handleChoose(t.id)}>
                    <div className={styles.typeCardIcon}>{t.icon}</div>
                    <div className={styles.typeCardBody}>
                      <h3 className={styles.typeCardLabel}>{t.label}</h3>
                      <p className={styles.typeCardDesc}>{t.desc}</p>
                      <div className={styles.typeCardModel}>
                        <svg width="11" height="11" viewBox="0 0 11 11" fill="none">
                          <circle cx="5.5" cy="5.5" r="4" stroke="currentColor" strokeWidth="1.2" />
                          <path d="M3.5 5.5l1.5 1.5 3-3" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round" strokeLinejoin="round" />
                        </svg>
                        {t.model}
                      </div>
                    </div>
                    <div className={styles.typeCardArrow}>
                      <svg width="18" height="18" viewBox="0 0 18 18" fill="none">
                        <path d="M4 9h10M10 5l4 4-4 4" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" />
                      </svg>
                    </div>
                  </button>
                ))}
              </div>

              <div className={styles.selectNote}>
                <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
                  <circle cx="7" cy="7" r="5.5" stroke="currentColor" strokeWidth="1.2" />
                  <path d="M7 4.5v.5M7 7v3" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round" />
                </svg>
                Not sure which to pick? Individual loans are for personal use. MSME loans are for registered businesses.
              </div>
            </div>
          )}

          {stage === 'upload' && (
            <>
              <div className={styles.loanTypeBanner}>
                <div className={styles.loanTypeBannerLeft}>
                  <div className={styles.loanTypeBannerIcon}>{selected?.icon}</div>
                  <div>
                    <div className={styles.loanTypeBannerLabel}>{selected?.label}</div>
                    <div className={styles.loanTypeBannerModel}>Using: {selected?.model}</div>
                  </div>
                </div>

                <button
                  className={styles.changeLoanType}
                  onClick={() => {
                    setStage('select')
                    setLoanType(null)
                    setFile(null)
                    setPreview(null)
                    setJsonData(null)
                    setParseErr('')
                  }}
                >
                  Change type
                </button>
              </div>

              <div className={styles.uploadCard}>
                <div className={styles.uploadCardHead}>
                  <h2 className={styles.uploadCardTitle}>Upload AA JSON file</h2>
                  <p className={styles.uploadCardSub}>
                    Your data is processed securely under the Account Aggregator framework.
                  </p>
                </div>

                {!file && (
                  <div
                    className={`${styles.dropZone} ${dragging ? styles.dropZoneDragging : ''}`}
                    onDragOver={(e) => {
                      e.preventDefault()
                      setDragging(true)
                    }}
                    onDragLeave={() => setDragging(false)}
                    onDrop={onDrop}
                    onClick={() => fileRef.current.click()}
                    role="button"
                    tabIndex={0}
                    onKeyDown={(e) => e.key === 'Enter' && fileRef.current.click()}
                  >
                    <input
                      ref={fileRef}
                      type="file"
                      accept=".json"
                      className={styles.fileInput}
                      onChange={onFileChange}
                    />
                    <div className={styles.dropIcon}>
                      <svg width="28" height="28" viewBox="0 0 28 28" fill="none">
                        <path d="M14 4v14M8 10l6-6 6 6" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
                        <path d="M4 20v2a2 2 0 002 2h16a2 2 0 002-2v-2" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
                      </svg>
                    </div>
                    <p className={styles.dropPrimary}>
                      {dragging ? 'Drop it here' : 'Drop your JSON file here'}
                    </p>
                    <p className={styles.dropSec}>
                      or <span className={styles.dropLink}>browse to upload</span>
                    </p>
                    <p className={styles.dropHint}>Only .json files · Max 10 MB</p>
                  </div>
                )}

                {parseErr && (
                  <div className={styles.errorBox}>
                    <AlertCircle size={18} />
                    <span style={{ whiteSpace: 'pre-line' }}>{parseErr}</span>
                  </div>
                )}

                {file && preview && (
                  <div className={styles.filePreview}>
                    <div className={styles.filePreviewHeader}>
                      <div className={styles.fileIcon}>
                        <svg width="18" height="18" viewBox="0 0 18 18" fill="none">
                          <rect x="2" y="1" width="14" height="16" rx="2.5" stroke="currentColor" strokeWidth="1.5" />
                          <path d="M5 6h8M5 9h8M5 12h5" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
                        </svg>
                      </div>
                      <div className={styles.fileInfo}>
                        <span className={styles.fileName}>{file.name}</span>
                        <span className={styles.fileSize}>{(file.size / 1024).toFixed(1)} KB</span>
                      </div>
                      <button className={styles.removeFile} onClick={removeFile} aria-label="Remove file">
                        <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
                          <path d="M4 4l8 8M12 4l-8 8" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
                        </svg>
                      </button>
                    </div>

                    <div className={styles.previewGrid}>
                      {getPreviewConfig().map((r, i) => (
                        <div key={i} className={styles.previewRow}>
                          <span className={styles.previewLabel}>{r.label}</span>
                          <span className={styles.previewValue}>{String(r.value)}</span>
                        </div>
                      ))}
                    </div>

                    <div className={styles.previewReady}>
                      <svg width="15" height="15" viewBox="0 0 15 15" fill="none">
                        <circle cx="7.5" cy="7.5" r="6" stroke="currentColor" strokeWidth="1.4" />
                        <path d="M4.5 7.5l2 2 4-4" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round" />
                      </svg>
                      File looks good — ready to submit
                    </div>
                  </div>
                )}

                <button
                  className={`btn btn-primary ${styles.submitBtn}`}
                  onClick={handleSubmit}
                  disabled={!file || submitting}
                >
                  {submitting ? (
                    <>
                      <span className={styles.spinner} /> Submitting…
                    </>
                  ) : (
                    <>
                      Submit application
                      <svg width="15" height="15" viewBox="0 0 15 15" fill="none">
                        <path d="M3 7.5h9M8 3.5l4 4-4 4" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                      </svg>
                    </>
                  )}
                </button>
              </div>

              <div className={styles.infoNote}>
                <svg width="15" height="15" viewBox="0 0 15 15" fill="none">
                  <circle cx="7.5" cy="7.5" r="6" stroke="currentColor" strokeWidth="1.4" />
                  <path d="M7.5 5v.5M7.5 7.5V10" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" />
                </svg>
                <span>Don't have a JSON file? Export it from any AA-certified app like Finvu, CAMS Finserv, or OneMoney.</span>
              </div>
            </>
          )}
        </main>

        <aside className={styles.sidebar}>
          <div className={styles.stepper}>
            <h3 className={styles.stepperTitle}>Application steps</h3>
            <div className={styles.stepperList}>
              {steps.map((s, i) => (
                <div key={s.id} className={styles.stepperItem}>
                  <div className={styles.stepperLeft}>
                    <div
                      className={`${styles.stepperDot} ${
                        i === activeStepIndex
                          ? styles.stepperDotActive
                          : i < activeStepIndex
                            ? styles.stepperDotDone
                            : styles.stepperDotIdle
                      }`}
                    >
                      {i < activeStepIndex ? (
                        <svg width="10" height="10" viewBox="0 0 10 10" fill="none">
                          <path d="M2 5l2.5 2.5 4-4" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round" />
                        </svg>
                      ) : i === activeStepIndex ? (
                        <svg width="10" height="10" viewBox="0 0 10 10" fill="none">
                          <circle cx="5" cy="5" r="3" fill="currentColor" />
                        </svg>
                      ) : (
                        <span>{s.id}</span>
                      )}
                    </div>
                    {i < steps.length - 1 && <div className={styles.stepperLine} />}
                  </div>
                  <div className={styles.stepperText}>
                    <span className={`${styles.stepperLabel} ${i === activeStepIndex ? styles.stepperLabelActive : ''}`}>
                      {s.label}
                    </span>
                    <span className={styles.stepperDesc}>{s.desc}</span>
                  </div>
                </div>
              ))}
            </div>
          </div>

          <div className={styles.checkCard}>
            <h3 className={styles.checkTitle}>What we analyse</h3>
            <ul className={styles.checkList}>
              {(selected?.checks || [
                'Income regularity & stability',
                'Savings & spending patterns',
                'Existing obligations',
                'Transaction history depth',
                'Cash-flow consistency',
              ]).map((item, i) => (
                <li key={i} className={styles.checkItem}>
                  <div className={styles.checkDot} />
                  {item}
                </li>
              ))}
            </ul>
          </div>

          <div className={styles.privacyNote}>
            <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
              <path d="M7 1L2 3v4c0 3 2.5 5.5 5 6 2.5-.5 5-3 5-6V3L7 1z" stroke="currentColor" strokeWidth="1.3" strokeLinejoin="round" />
              <path d="M5 7l1.5 1.5L9 5" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round" strokeLinejoin="round" />
            </svg>
            <span>Your data is encrypted and never shared outside the loan assessment process.</span>
          </div>
        </aside>
      </div>
    </div>
  )
}