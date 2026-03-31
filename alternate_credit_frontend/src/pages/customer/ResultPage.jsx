import { useEffect, useMemo, useState } from 'react'
import { useNavigate, useLocation, Link } from 'react-router-dom'

import styles from './ResultPage.module.css'
import axios from 'axios'
import Chatbot from '../../components/Chatbot'
function ShapImage({ imageBase64 }) {
  if (!imageBase64) return null

  return (
    <div className={styles.shapImageWrap}>
      <img
        src={`data:image/png;base64,${imageBase64}`}
        alt="SHAP explanation"
        className={styles.shapImage}
      />
    </div>
  )
}

function safeNumber(value) {
  const n = Number(value)
  return Number.isFinite(n) ? n : null
}

function formatCurrency(value) {
  const n = safeNumber(value)
  if (n === null) return 'N/A'
  return `₹${n.toLocaleString('en-IN', {
    minimumFractionDigits: 0,
    maximumFractionDigits: 2,
  })}`
}

function formatPercent(value) {
  const n = safeNumber(value)
  if (n === null) return 'N/A'
  return `${n.toFixed(2)}%`
}

function formatPlain(value) {
  if (value === null || value === undefined || value === '') return 'N/A'
  return String(value)
}

function convertRiskToCibil(riskScore) {
  const rs = safeNumber(riskScore)
  if (rs === null) return 'N/A'
  const bounded = Math.max(0, Math.min(1, rs))
  const cibil = 900 - Math.round(bounded * 600)
  return Math.max(300, Math.min(900, cibil))
}

function computeEmi(principal, annualRatePct, tenureValue, applicationType) {
  const p = safeNumber(principal)
  const annual = safeNumber(annualRatePct)
  const tenure = safeNumber(tenureValue)

  if (p === null || annual === null || tenure === null) return null
  if (p <= 0 || annual <= 0 || tenure <= 0) return null

  const n =
    applicationType === 'individual'
      ? Math.round(tenure)
      : Math.round(tenure * 12)

  if (n <= 0) return null

  const r = annual / 12 / 100
  const emi = (p * r * Math.pow(1 + r, n)) / (Math.pow(1 + r, n) - 1)

  if (!Number.isFinite(emi)) return null
  return emi
}

export default function ResultPage() {
  const navigate = useNavigate()
  const location = useLocation()

  const applicationId = location.state?.applicationId
  const loanType = location.state?.loanType || 'msme'

  const [result, setResult] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [openingPdf, setOpeningPdf] = useState(false)

  useEffect(() => {
    const fetchResult = async () => {
      try {
        if (!applicationId) {
          setError('Missing application ID.')
          setLoading(false)
          return
        }

        const resultUrl =
          loanType === 'individual'
            ? `http://127.0.0.1:8000/api/customer/individual/${applicationId}/result`
            : `http://127.0.0.1:8000/api/customer/msme/${applicationId}/result`

        const res = await axios.get(resultUrl)
        setResult(res.data)
      } catch (err) {
        setError(err?.response?.data?.detail || 'Failed to load application result.')
      } finally {
        setLoading(false)
      }
    }

    fetchResult()
  }, [applicationId, loanType])

  const applicationType = result?.application_type || loanType || 'msme'

  const pdfUrl =
    applicationType === 'individual'
      ? `http://127.0.0.1:8000/api/customer/individual/${applicationId}/pdf`
      : `http://127.0.0.1:8000/api/customer/msme/${applicationId}/pdf`

  const isApproved = useMemo(() => {
    return String(result?.manager_decision || '').toUpperCase() === 'APPROVED'
  }, [result])

  const displayName =
  result?.company_name ||
  result?.applicant_name ||
  result?.full_name ||
  'Applicant'
  const cibilScore = convertRiskToCibil(result?.risk_score)
  const emi = computeEmi(
  result?.final_principal,
  result?.final_interest_rate,
  result?.final_tenure,
  applicationType
)

  const handleOpenPdf = () => {
    if (!applicationId) return
    setOpeningPdf(true)
    window.open(pdfUrl, '_blank', 'noopener,noreferrer')
    setTimeout(() => setOpeningPdf(false), 1000)
  }

  if (loading) {
    return (
      <div className={styles.page}>
        
        <div className={`container ${styles.layout}`}>
          <main className={styles.main}>
            <div className={styles.statusBanner}>
              <div className={styles.statusText}>
                <h1 className={styles.statusTitle}>Loading your application result...</h1>
                <p className={styles.statusSub}>
                  Please wait while we fetch the latest decision details.
                </p>
              </div>
            </div>
          </main>
        </div>
      </div>
    )
  }

  if (error || !result) {
    return (
      <div className={styles.page}>
        
        <div className={`container ${styles.layout}`}>
          <main className={styles.main}>
            <div className={styles.statusBannerRejected}>
              <div className={styles.statusText}>
                <h1 className={styles.statusTitle}>Unable to load result</h1>
                <p className={styles.statusSub}>
                  {error || 'No application result was found.'}
                </p>
              </div>
            </div>

            <div style={{ marginTop: '20px' }}>
              <button
                className="btn btn-outline"
                onClick={() => navigate('/dashboard')}
              >
                Back to dashboard
              </button>
            </div>
          </main>
        </div>
      </div>
    )
  }

  return (
    <div className={styles.page}>
      
      <Chatbot/>
      <div className={`container ${styles.layout}`}>
        <main className={styles.main}>
          <div
            className={`${styles.statusBanner} ${
              isApproved ? styles.statusBannerApproved : styles.statusBannerRejected
            }`}
          >
            <div className={styles.statusText}>
              <h1 className={styles.statusTitle}>
                {isApproved ? 'Loan Approved' : 'Application Not Approved'}
              </h1>
              <p className={styles.statusSub}>
                {isApproved
  ? applicationType === 'individual'
    ? 'Your individual loan application has been approved after model evaluation and manager review.'
    : 'Your MSME loan application has been approved after model evaluation and manager review.'
  : 'Your application could not be approved at this stage after review.'}
              </p>
            </div>

            <div className={styles.statusAppId}>
              <span className={styles.statusAppIdLabel}>Application ID</span>
              <span className={styles.statusAppIdVal}>{result.application_id}</span>
            </div>
          </div>

          <div className={styles.loanCard}>
            <h2 className={styles.cardTitle}>Final application summary</h2>

            <div className={styles.loanGrid}>
              <div className={styles.loanRow}>
  <span className={styles.loanLabel}>
    {applicationType === 'individual' ? 'Applicant' : 'Company / Applicant'}
  </span>
  <span className={styles.loanValue}>{displayName}</span>
</div>

{applicationType === 'individual' ? (
  <>
    <div className={styles.loanRow}>
      <span className={styles.loanLabel}>Employment Status</span>
      <span className={styles.loanValue}>
        {formatPlain(result.employment_status)}
      </span>
    </div>

    <div className={styles.loanRow}>
      <span className={styles.loanLabel}>PAN Number</span>
      <span className={styles.loanValue}>
        {formatPlain(result.pan_number)}
      </span>
    </div>
  </>
) : (
  <>
    <div className={styles.loanRow}>
      <span className={styles.loanLabel}>Entity Type</span>
      <span className={styles.loanValue}>
        {formatPlain(result.entity_type)}
      </span>
    </div>

    <div className={styles.loanRow}>
      <span className={styles.loanLabel}>Registration Number</span>
      <span className={styles.loanValue}>
        {formatPlain(result.registration_number)}
      </span>
    </div>
  </>
)}

              <div className={styles.loanRow}>
                <span className={styles.loanLabel}>Requested Loan Amount</span>
                <span className={styles.loanValue}>
                  {formatCurrency(result.loan_ask)}
                </span>
              </div>

              <div className={styles.loanRow}>
                <span className={styles.loanLabel}>Final Principal</span>
                <span className={styles.loanValue}>
                  {formatCurrency(result.final_principal)}
                </span>
              </div>

              <div className={styles.loanRow}>
                <span className={styles.loanLabel}>Final Interest Rate</span>
                <span className={styles.loanValue}>
                  {formatPercent(result.final_interest_rate)}
                </span>
              </div>

              <div className={styles.loanRow}>
                <span className={styles.loanLabel}>Tenure</span>
                <span className={styles.loanValue}>
                  {result.final_tenure
  ? applicationType === 'individual'
    ? `${result.final_tenure} months`
    : `${result.final_tenure} years`
  : 'N/A'}
                </span>
              </div>

              <div className={styles.loanRow}>
                <span className={styles.loanLabel}>Estimated EMI</span>
                <span className={styles.loanValue}>
                  {emi !== null ? formatCurrency(emi) : 'N/A'}
                </span>
              </div>

              <div className={styles.loanRow}>
                <span className={styles.loanLabel}>Risk Score</span>
                <span className={styles.loanValue}>
                  {safeNumber(result.risk_score) !== null
                    ? Number(result.risk_score).toFixed(4)
                    : 'N/A'}
                </span>
              </div>

              <div className={styles.loanRow}>
                <span className={styles.loanLabel}>Risk Band</span>
                <span className={styles.loanValue}>
                  {formatPlain(result.risk_band)}
                </span>
              </div>

              <div className={styles.loanRow}>
                <span className={styles.loanLabel}>Approx. CIBIL Score</span>
                <span className={styles.loanValue}>{cibilScore}</span>
              </div>

              <div className={styles.loanRow}>
                <span className={styles.loanLabel}>Manager Decision</span>
                <span className={styles.loanValue}>
                  {formatPlain(result.manager_decision)}
                </span>
              </div>
            </div>
          </div>

          <div className={styles.rejectedCard}>
            <h2 className={styles.cardTitle}>Manager Notes</h2>
            <p>{result.manager_notes || 'No additional notes were provided.'}</p>
          </div>

          <div className={styles.shapCard}>
            <div className={styles.shapCardHead}>
              <div>
                <h2 className={styles.cardTitle}>AI explainability</h2>
                <p className={styles.shapCardSub}>
                  {applicationType === 'individual'
  ? 'This SHAP chart shows which applicant features influenced the model toward lower or higher personal credit risk.'
  : 'This SHAP chart shows which business features influenced the model toward lower or higher credit risk.'}
                </p>
              </div>
            </div>

            <ShapImage imageBase64={result.shap_plot_base64} />

            {!result.shap_plot_base64 && (
              <p style={{ marginTop: '12px' }}>
                Explainability chart is not available for this application.
              </p>
            )}
          </div>
        </main>

        <aside className={styles.sidebar}>
          <div className={styles.downloadCard}>
            <h3 className={styles.downloadTitle}>View customer PDF report</h3>
            <p className={styles.downloadSub}>
              Open the final customer-facing application report with loan terms,
              EMI, score summary, and explanation details.
            </p>

            <button
              className={`btn btn-primary ${styles.downloadBtn}`}
              onClick={handleOpenPdf}
              disabled={openingPdf}
            >
              {openingPdf ? 'Opening…' : 'Open PDF Report'}
            </button>
          </div>

          <div className={styles.detailsCard}>
            <h3 className={styles.detailsTitle}>Application details</h3>

            <div className={styles.detailRow}>
              <span className={styles.detailLabel}>Application ID</span>
              <span className={styles.detailValue}>{result.application_id}</span>
            </div>

            <div className={styles.detailRow}>
  <span className={styles.detailLabel}>
    {applicationType === 'individual' ? 'Applicant' : 'Company'}
  </span>
  <span className={styles.detailValue}>{displayName}</span>
</div>
{applicationType === 'individual' && (
  <div className={styles.detailRow}>
    <span className={styles.detailLabel}>PAN</span>
    <span className={styles.detailValue}>
      {formatPlain(result.pan_number)}
    </span>
  </div>
)}

            <div className={styles.detailRow}>
              <span className={styles.detailLabel}>Status</span>
              <span className={styles.detailValue}>{formatPlain(result.status)}</span>
            </div>

            <div className={styles.detailRow}>
              <span className={styles.detailLabel}>Decision</span>
              <span className={styles.detailValue}>
                {formatPlain(result.manager_decision)}
              </span>
            </div>
          </div>

          <div className={styles.actionsCard}>
            <button
              className="btn btn-outline"
              style={{ width: '100%', justifyContent: 'center' }}
              onClick={() => navigate('/dashboard')}
            >
              Back to dashboard
            </button>

            <Link to="/" className={styles.homeLink}>
              ← Back to home
            </Link>
          </div>
        </aside>
      </div>
    </div>
  )
}