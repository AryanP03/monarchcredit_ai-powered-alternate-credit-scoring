import { useState, useEffect } from 'react'
import { useNavigate, useSearchParams, Link } from 'react-router-dom'
import styles from './AuthPage.module.css'

export default function AuthPage() {
  const [searchParams] = useSearchParams()
  const [mode, setMode] = useState(searchParams.get('mode') === 'signup' ? 'signup' : 'signin')
  const [loading, setLoading] = useState(false)
  const [form, setForm] = useState({ name: '', email: '', phone: '', password: '' })
  const [errors, setErrors] = useState({})
  const [serverError, setServerError] = useState("")
  const navigate = useNavigate()

  useEffect(() => {
  setErrors({})
  setServerError("")
}, [mode])

  const validate = () => {
    const e = {}
    if (mode === 'signup' && !form.name.trim()) e.name = 'Full name is required'
    if (!form.email.includes('@')) e.email = 'Enter a valid email'
    if (mode === 'signup' && form.phone.replace(/\D/g,'').length < 10) e.phone = 'Enter a valid phone number'
    if (form.password.length < 6) e.password = 'At least 6 characters'
    return e
  }

  const handleSubmit = async (e) => {
  e.preventDefault()
  setServerError("")

  const errs = validate()
  if (Object.keys(errs).length) {
    setErrors(errs)
    return
  }

  setLoading(true)

  try {
    let url = ""
    let body = {}

    if (mode === "signup") {
      url = "http://127.0.0.1:8000/auth/customer/signup"
      body = {
        name: form.name,
        email: form.email,
        password: form.password,
        phone: form.phone || null
      }
    } else {
      url = "http://127.0.0.1:8000/auth/customer/signin"
      body = {
        email: form.email,
        password: form.password
      }
    }

    const res = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify(body)
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
    navigate("/dashboard")
  } catch (err) {
    console.error(err)
    setServerError("Unable to connect to server")
  } finally {
    setLoading(false)
  }
}

  const set = (field) => (e) => {
  setServerError("")   // 🔥 clears error while typing
  setForm(f => ({ ...f, [field]: e.target.value }))
}

  return (
    <div className={styles.page}>

      {/* ── Left brand panel ─────────────────────────── */}
      <div className={styles.left}>
        <Link to="/" className={styles.backLogo}>
        
          MonarchCredit
        </Link>

        <div className={styles.leftContent}>
          <div className={styles.leftQuote}>
            <div className={styles.leftQuoteBar} />
            <div>
              <p className={styles.leftQuoteText}>
                "Credit access for those the system forgot - powered by how you actually manage money."
              </p>
              <span className={styles.leftQuoteSrc}>MonarchCredit · Mission</span>
            </div>
          </div>

          <div className={styles.leftStats}>
            {[
              { n: '1.4B', l: 'Applicants served' },
              { n: '85+',  l: 'AUC of the model'},
              { n: 'Full Automated Report',l: 'PDF report of the results'      },
            ].map((s, i) => (
              <div key={i} className={styles.leftStat}>
                <span className={styles.leftStatN}>{s.n}</span>
                <span className={styles.leftStatL}>{s.l}</span>
              </div>
            ))}
          </div>
        </div>

        <div className={styles.leftBg} aria-hidden="true">
          <div className={styles.leftBgCircle} />
        </div>
      </div>

      {/* ── Right form panel ─────────────────────────── */}
      <div className={styles.right}>
        <div className={styles.formWrap}>

          <div className={styles.modeTabs}>
            <button
              className={`${styles.modeTab} ${mode === 'signin' ? styles.modeTabActive : ''}`}
              onClick={() => setMode('signin')}
            >Sign in</button>
            <button
              className={`${styles.modeTab} ${mode === 'signup' ? styles.modeTabActive : ''}`}
              onClick={() => setMode('signup')}
            >Create account</button>
          </div>

          <div className={styles.formHead}>
            <h1 className={styles.formTitle}>
              {mode === 'signin' ? 'Welcome back' : 'Get started'}
            </h1>
            <p className={styles.formSub}>
              {mode === 'signin'
                ? 'Sign in to view your application status.'
                : 'Apply for a loan in under 5 minutes.'}
            </p>
          </div>

          <form className={styles.form} onSubmit={handleSubmit} noValidate>
  {mode === 'signup' && (
    <div className={styles.field}>
      <label htmlFor="name">Full name</label>
      <input
        id="name"
        className={`input ${errors.name ? styles.inputError : ''}`}
        type="text"
        placeholder="Arjun Sharma"
        value={form.name}
        onChange={set('name')}
      />
      {errors.name && <span className={styles.err}>{errors.name}</span>}
    </div>
  )}

  <div className={styles.field}>
    <label htmlFor="email">Email address</label>
    <input
      id="email"
      className={`input ${errors.email ? styles.inputError : ''}`}
      type="email"
      placeholder="you@example.com"
      value={form.email}
      onChange={set('email')}
    />
    {errors.email && <span className={styles.err}>{errors.email}</span>}
  </div>

  {mode === 'signup' && (
    <div className={styles.field}>
      <label htmlFor="phone">Phone number</label>
      <input
        id="phone"
        className={`input ${errors.phone ? styles.inputError : ''}`}
        type="tel"
        placeholder="+91 98765 43210"
        value={form.phone}
        onChange={set('phone')}
      />
      {errors.phone && <span className={styles.err}>{errors.phone}</span>}
    </div>
  )}

  <div className={styles.field}>
    <div className={styles.pwRow}>
      <label htmlFor="password">Password</label>
      {mode === 'signin' && <a href="#" className={styles.forgot}>Forgot password?</a>}
    </div>
    <input
      id="password"
      className={`input ${errors.password ? styles.inputError : ''}`}
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
      {loading
        ? <span className={styles.spinner} />
        : mode === 'signin' ? 'Sign in' : 'Create account'
      }
    </button>
  </div>

  {serverError && (
    <p className={styles.serverError}>
      {serverError}
    </p>
  )}
</form>

          <div className={styles.orRow}>
            <div className={styles.orLine} /><span className={styles.orText}>or</span><div className={styles.orLine} />
          </div>

          <p className={styles.switchText}>
            {mode === 'signin' ? "Don't have an account? " : 'Already have an account? '}
            <button className={styles.switchBtn} onClick={() => setMode(mode === 'signin' ? 'signup' : 'signin')}>
              {mode === 'signin' ? 'Create one' : 'Sign in'}
            </button>
          </p>

          <p className={styles.managerLink}>
            Loan Manager? <Link to="/manager/login">Access the manager portal →</Link>
          </p>
        </div>
      </div>
    </div>
  )
}
