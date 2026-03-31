import { useState } from 'react'
import { useNavigate, Link } from 'react-router-dom'
import styles from './ManagerLogin.module.css'

export default function ManagerLogin() {
  const [form, setForm]       = useState({ email: '', password: '' })
  const [errors, setErrors]   = useState({})
  const [serverError, setServerError] = useState("")
  const [loading, setLoading] = useState(false)
  const navigate = useNavigate()

  const set = (f) => (e) => {
  setServerError("")
  setForm(p => ({ ...p, [f]: e.target.value }))
}

  const handleSubmit = async (e) => {
  e.preventDefault()
  setServerError("")

  const errs = {}
  if (!form.email.includes('@')) errs.email = 'Enter a valid email'
  if (form.password.length < 4) errs.password = 'Enter your password'

  if (Object.keys(errs).length) {
    setErrors(errs)
    return
  }

  setLoading(true)

  try {
    const res = await fetch("http://127.0.0.1:8000/auth/manager/signin", {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        email: form.email,
        password: form.password
      })
    })

    let data = null
    try {
      data = await res.json()
    } catch {
      data = null
    }

    if (!res.ok) {
      setServerError(data?.detail || "Something went wrong")
      return
    }

    localStorage.setItem("user", JSON.stringify(data))
    navigate("/manager/dashboard")
  } catch (err) {
    console.error(err)
    setServerError("Unable to connect to server")
  } finally {
    setLoading(false)
  }
}

  return (
    <div className={styles.page}>

      {/* Top bar */}
      <div className={styles.topBar}>
        <div className={styles.logo}>
          <span className={styles.logoMark}>
            <svg width="13" height="13" viewBox="0 0 14 14" fill="none">
              <path d="M2 11L4.5 5L7 8.5L9.5 5L12 11H2Z" fill="white"/>
              <rect x="2" y="11.2" width="10" height="1.8" rx="0.9" fill="white"/>
            </svg>
          </span>
          MonarchCredit
        </div>
        <span className={styles.portalBadge}>Manager Portal</span>
      </div>

      <div className={styles.center}>
        <div className={styles.card}>

          {/* Icon */}
          <div className={styles.cardIcon}>
            <svg width="24" height="24" viewBox="0 0 24 24" fill="none">
              <circle cx="12" cy="8" r="4" stroke="currentColor" strokeWidth="1.8"/>
              <path d="M4 20c0-4.418 3.582-7 8-7s8 2.582 8 7" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round"/>
            </svg>
          </div>

          <h1 className={styles.title}>Manager sign in</h1>
          <p className={styles.sub}>Access the loan review portal. Authorised personnel only.</p>

          <form className={styles.form} onSubmit={handleSubmit} noValidate>
            <div className={styles.field}>
              <label htmlFor="email">Work email</label>
              <input
                id="email"
                className={`input ${errors.email ? styles.inputErr : ''}`}
                type="email"
                placeholder="priya.mehta@barclays.com"
                value={form.email}
                onChange={set('email')}
              />
              {errors.email && <span className={styles.err}>{errors.email}</span>}
            </div>

            <div className={styles.field}>
              <label htmlFor="password">Password</label>
              <input
                id="password"
                className={`input ${errors.password ? styles.inputErr : ''}`}
                type="password"
                placeholder="••••••••"
                value={form.password}
                onChange={set('password')}
              />
              {errors.password && <span className={styles.err}>{errors.password}</span>}
            </div>

            <div className={styles.buttonWrapper}>
  <button
    type="submit"
    className={`btn btn-primary ${styles.submitBtn}`}
    disabled={loading}
  >
    {loading ? <span className={styles.spinner} /> : 'Sign in to portal'}
  </button>
</div>

{serverError && (
  <p className={styles.serverError}>
    {serverError}
  </p>
)}
          </form>

          <p className={styles.customerLink}>
            Not a manager? <Link to="/auth">Go to customer portal →</Link>
          </p>
        </div>

        <p className={styles.secureNote}>
          <svg width="13" height="13" viewBox="0 0 13 13" fill="none">
            <path d="M6.5 1L2 3v3.5C2 9.5 4 11.5 6.5 12.5 9 11.5 11 9.5 11 6.5V3L6.5 1z" stroke="currentColor" strokeWidth="1.2" strokeLinejoin="round"/>
          </svg>
          Secure, encrypted connection · Barclays fintech infrastructure
        </p>
      </div>
    </div>
  )
}
