import { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import Navbar from '../../components/Navbar'
import styles from './LandingPage.module.css'
import Chatbot from '../../components/Chatbot'
const stats = [
  { value: '1.4B+', label: 'Unbanked Customers' },
  
  { value: 'AUC 85+', label: 'Model Metrics' },
  { value: '60-80%', label: 'Cost reduction' },
  { value: 'Full Financial Report', label: 'PDF report of final results' }
]

const steps = [
  {
    num: '01',
    title: 'Upload your data',
    desc: 'Share your Account Aggregator JSON - your real financial story, not just a credit score.',
    icon: (
      <svg width="22" height="22" viewBox="0 0 22 22" fill="none">
        <rect x="3" y="2" width="16" height="18" rx="3" stroke="currentColor" strokeWidth="1.6" />
        <path d="M7 7h8M7 11h8M7 15h5" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" />
      </svg>
    ),
  },
  {
    num: '02',
    title: 'AI assessment',
    desc: 'Our ML pipeline analyses spending patterns, income stability, and savings behaviour in seconds.',
    icon: (
      <svg width="22" height="22" viewBox="0 0 22 22" fill="none">
        <circle cx="11" cy="11" r="8" stroke="currentColor" strokeWidth="1.6" />
        <path d="M7.5 11l2.5 2.5 5-5" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" />
      </svg>
    ),
  },
  {
    num: '03',
    title: 'Human review',
    desc: 'A loan manager reviews the AI recommendation and makes the final call - full transparency.',
    icon: (
      <svg width="22" height="22" viewBox="0 0 22 22" fill="none">
        <circle cx="11" cy="8" r="4" stroke="currentColor" strokeWidth="1.6" />
        <path d="M3 20c0-4.418 3.582-7 8-7s8 2.582 8 7" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" />
      </svg>
    ),
  },
]

const features = [
  {
    title: 'No credit history required',
    desc: "We assess your real financial behaviour — not a score you've never had a chance to build.",
  },
  {
    title: 'Built for MSMEs',
    desc: 'Tailored for small and micro businesses that traditional banks overlook.',
  },
  {
    title: 'Explainable decisions',
    desc: 'Every approval or rejection comes with a clear explanation of the reasoning behind it.',
  },
  {
    title: 'Fast & secure',
    desc: 'Account Aggregator framework ensures your data is shared safely and with your consent.',
  },
]

export default function LandingPage() {
  const navigate = useNavigate()

  const words = ['everyone.', 'MSMEs.', 'founders.', 'dreamers.']
  const [typedText, setTypedText] = useState('')
  const [wordIndex, setWordIndex] = useState(0)
  const [isDeleting, setIsDeleting] = useState(false)

  useEffect(() => {
    const currentWord = words[wordIndex]
    const typingSpeed = isDeleting ? 60 : 110
    const pauseTime = 1200

    const timeout = setTimeout(() => {
      if (!isDeleting) {
        const nextText = currentWord.slice(0, typedText.length + 1)
        setTypedText(nextText)

        if (nextText === currentWord) {
          setTimeout(() => setIsDeleting(true), pauseTime)
        }
      } else {
        const nextText = currentWord.slice(0, typedText.length - 1)
        setTypedText(nextText)

        if (nextText === '') {
          setIsDeleting(false)
          setWordIndex((prev) => (prev + 1) % words.length)
        }
      }
    }, typingSpeed)

    return () => clearTimeout(timeout)
  }, [typedText, isDeleting, wordIndex, words])

  return (
    <div className={styles.page}>
      <Navbar />
      
      <section className={styles.hero}>
        <div className={`container ${styles.heroInner}`}>
          <div className={`${styles.heroPill} fade-up`}>
            <span className={styles.pillDot} />
            AI-powered credit decisions
          </div>

          <h1 className={`${styles.heroHeading} fade-up fade-up-1`}>
            Credit access for
            <br />
            <em className={styles.heroAccent}>
              <span className={styles.typewriterWord}>{typedText}</span>
              <span className={styles.typewriterCursor}>|</span>
            </em>
          </h1>

          <p className={`${styles.heroSub} fade-up fade-up-2`}>
            Loans for unbanked individuals and MSMEs - assessed on how you
            actually manage money, not a number you were never given a
            chance to build.
          </p>

          <div className={`${styles.heroActions} fade-up fade-up-3`}>
            <button
              className="btn btn-primary btn-lg"
              onClick={() => navigate('/auth?mode=signup')}
            >
              Apply for a loan
              <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
                <path
                  d="M3 8h10M9 4l4 4-4 4"
                  stroke="currentColor"
                  strokeWidth="1.6"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                />
              </svg>
            </button>

            <button className="btn btn-outline btn-lg">
              See how it works
            </button>
          </div>

          <div className={`${styles.heroCard} fade-up fade-up-4`}>
            <div className={styles.heroCardRow}>
              <div className={styles.heroCardAvatar}>AS</div>
              <div>
                <div className={styles.heroCardName}>Arjun Sharma · MSME</div>
                <div className={styles.heroCardSub}>Loan approved — ₹4,20,000 @ 11.5%</div>
              </div>
              <span className="chip chip-green" style={{ marginLeft: 'auto' }}>
                Approved
              </span>
            </div>

            <div className={styles.heroCardBar}>
              <div className={styles.heroCardProgress} />
            </div>

            <div className={styles.heroCardMeta}>
              <span>Decision in 3 min</span>
              <span>SHAP explained</span>
            </div>
          </div>
        </div>

        <div className={styles.heroBg} aria-hidden="true">
          <div className={styles.heroBgCircle1} />
          <div className={styles.heroBgCircle2} />
        </div>
      </section>

      <section className={styles.statsBar} id="about">
        <div className="container">
          <div className={styles.statsGrid}>
            {stats.map((s, i) => (
              <div key={i} className={styles.stat}>
                <span className={styles.statNum}>{s.value}</span>
                <span className={styles.statLabel}>{s.label}</span>
              </div>
            ))}
          </div>
        </div>
      </section>

      <section className={styles.spotlight}>
        <div className="container">
          <div className={styles.spotlightInner}>
            <div className={styles.spotlightBar} />
            <div>
              <blockquote className={styles.spotlightQuote}>
                "We provide loan access to unbanked & underbanked individuals and
                MSMEs without credit history - using your real financial behaviour,
                not a number."
              </blockquote>
              <cite className={styles.spotlightCite}>
                MonarchCredit · Mission Statement
              </cite>
            </div>
          </div>
        </div>
      </section>

      <section className={styles.howSection} id="how">
        <div className="container">
          <div className={styles.sectionLabel}>How it works</div>
          <h2 className={styles.sectionHeading}>Three steps to a decision</h2>

          <div className={styles.stepsGrid}>
            {steps.map((s, i) => (
              <div key={i} className={styles.stepCard}>
                <div className={styles.stepTop}>
                  <div className={styles.stepIconWrap}>{s.icon}</div>
                  <span className={styles.stepNum}>{s.num}</span>
                </div>
                <h3 className={styles.stepTitle}>{s.title}</h3>
                <p className={styles.stepDesc}>{s.desc}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      <section className={styles.featuresSection} id="solutions">
        <div className="container">
          <div className={styles.featuresLayout}>
            <div className={styles.featuresSide}>
              <div className={styles.sectionLabel}>Our solutions</div>
              <h2 className={styles.sectionHeading}>
                Built for those
                <br />
                the system forgot
              </h2>
              <p className={styles.featuresSub}>
                Traditional credit scoring excludes millions. MonarchCredit uses
                Account Aggregator data to build a fairer picture of financial health.
              </p>
              <button
                className="btn btn-primary"
                onClick={() => navigate('/auth?mode=signup')}
              >
                Get started
              </button>
            </div>

            <div className={styles.featuresGrid}>
              {features.map((f, i) => (
                <div key={i} className={styles.featureCard}>
                  <div className={styles.featureDot} />
                  <h3 className={styles.featureTitle}>{f.title}</h3>
                  <p className={styles.featureDesc}>{f.desc}</p>
                </div>
              ))}
            </div>
          </div>
        </div>
      </section>

      <section className={styles.ctaSection}>
        <div className="container">
          <div className={styles.ctaBox}>
            <div className={styles.ctaLeft}>
              <h2 className={styles.ctaHeading}>Ready to apply?</h2>
              <p className={styles.ctaSub}>
                Takes under 5 minutes. No paperwork. No branch visit.
              </p>
            </div>
            <button
              className="btn btn-primary btn-lg"
              onClick={() => navigate('/auth?mode=signup')}
            >
              Apply for a loan
            </button>
          </div>
        </div>
      </section>

      <footer className={styles.footer}>
        <div className="container">
          <div className={styles.footerInner}>
            <div className={styles.footerLogo}>
              <span>MonarchCredit</span>
            </div>
            <p className={styles.footerCopy}>
              © 2026 MonarchCredit. Barclays, Hack-O-Hire.
            </p>
          </div>
        </div>
      </footer>
    </div>
  )
}