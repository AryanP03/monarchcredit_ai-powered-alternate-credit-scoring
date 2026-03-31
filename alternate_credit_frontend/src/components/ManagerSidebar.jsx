import { NavLink, useNavigate } from 'react-router-dom'
import styles from './ManagerSidebar.module.css'

const NAV = [
  {
    to: '/manager/dashboard',
    label: 'Dashboard',
    icon: (
      <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
        <rect x="1" y="1" width="6" height="6" rx="1.5" stroke="currentColor" strokeWidth="1.4"/>
        <rect x="9" y="1" width="6" height="6" rx="1.5" stroke="currentColor" strokeWidth="1.4"/>
        <rect x="1" y="9" width="6" height="6" rx="1.5" stroke="currentColor" strokeWidth="1.4"/>
        <rect x="9" y="9" width="6" height="6" rx="1.5" stroke="currentColor" strokeWidth="1.4"/>
      </svg>
    ),
  },
  {
    to: '/manager/dashboard#active',
    label: 'Active customers',
    icon: (
      <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
        <circle cx="8" cy="6" r="3" stroke="currentColor" strokeWidth="1.4"/>
        <path d="M2 14c0-3.314 2.686-5 6-5s6 1.686 6 5" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round"/>
      </svg>
    ),
  },
  {
    to: '/manager/dashboard#previous',
    label: 'Previous customers',
    icon: (
      <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
        <path d="M2 4h12M2 8h9M2 12h11" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round"/>
      </svg>
    ),
  },
  {
    to: '/manager/dashboard#audit',
    label: 'Audit trail',
    icon: (
      <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
        <rect x="2" y="1" width="12" height="14" rx="2" stroke="currentColor" strokeWidth="1.4"/>
        <path d="M5 5h6M5 8h6M5 11h3" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round"/>
      </svg>
    ),
  },
  {
    to: '/manager/dashboard#model',
    label: 'Model settings',
    icon: (
      <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
        <circle cx="8" cy="8" r="2.5" stroke="currentColor" strokeWidth="1.4"/>
        <path d="M8 1v2M8 13v2M1 8h2M13 8h2M3.05 3.05l1.41 1.41M11.54 11.54l1.41 1.41M3.05 12.95l1.41-1.41M11.54 4.46l1.41-1.41" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round"/>
      </svg>
    ),
  },
]

export default function ManagerSidebar({ modelHealthy = true }) {
  const navigate = useNavigate()

  return (
    <aside className={styles.sidebar}>
      {/* Logo */}
      <div className={styles.logo}>
        <div className={styles.logoMark}>
          <svg width="13" height="13" viewBox="0 0 14 14" fill="none">
            <path d="M2 11L4.5 5L7 8.5L9.5 5L12 11H2Z" fill="white"/>
            <rect x="2" y="11.2" width="10" height="1.8" rx="0.9" fill="white"/>
          </svg>
        </div>
        <div>
          <div className={styles.logoText}>MonarchCredit</div>
          <div className={styles.logoSub}>Manager Portal</div>
        </div>
      </div>

      {/* Nav */}
      <nav className={styles.nav}>
        {NAV.map((item) => (
          <NavLink
            key={item.label}
            to={item.to}
            className={({ isActive }) =>
              `${styles.navItem} ${isActive ? styles.navItemActive : ''}`
            }
          >
            <span className={styles.navIcon}>{item.icon}</span>
            {item.label}
          </NavLink>
        ))}
      </nav>

      {/* Model status */}
      <div className={styles.modelStatus}>
        <div className={styles.modelStatusRow}>
          <div className={`${styles.modelDot} ${modelHealthy ? styles.modelDotGreen : styles.modelDotRed}`} />
          <span className={styles.modelStatusLabel}>
            {modelHealthy ? 'Model healthy' : 'Anomaly detected'}
          </span>
        </div>
        {!modelHealthy && (
          <button className={`btn btn-sm ${styles.retrain}`}>Trigger retraining</button>
        )}
      </div>

      {/* User */}
      <div className={styles.user}>
        <div className={styles.userAvatar}>PM</div>
        <div className={styles.userInfo}>
          <div className={styles.userName}>Priya Mehta</div>
          <div className={styles.userRole}>Loan Manager</div>
        </div>
        <button
          className={styles.signOut}
          onClick={() => navigate('/manager/login')}
          title="Sign out"
        >
          <svg width="15" height="15" viewBox="0 0 15 15" fill="none">
            <path d="M6 2H3a1 1 0 00-1 1v9a1 1 0 001 1h3M10 10l3-3-3-3M13 7.5H6" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round"/>
          </svg>
        </button>
      </div>
    </aside>
  )
}
