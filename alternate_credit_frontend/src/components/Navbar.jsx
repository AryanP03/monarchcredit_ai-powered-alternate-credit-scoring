import { Link, useNavigate } from 'react-router-dom'
import styles from './Navbar.module.css'

export default function Navbar({ transparent = false }) {
  const navigate = useNavigate()

  return (
    <header className={`${styles.navbar} ${transparent ? styles.transparent : ''}`}>
      <div className={`container ${styles.inner}`}>
        {/* Logo */}
        <Link to="/" className={styles.logo}>
          
          <span className={styles.logoText}>MonarchCredit</span>
        </Link>

        {/* Nav links */}
        <nav className={styles.links}>
          <a href="#about" className={styles.link}>About</a>
          <a href="#solutions" className={styles.link}>Solutions</a>
          <a href="#how" className={styles.link}>How it works</a>
        </nav>

        {/* Actions */}
        <div className={styles.actions}>
          <button className="btn btn-primary btn-sm" onClick={() => navigate('/auth')}>
            Sign in
          </button>
          <button className="btn btn-primary btn-sm" onClick={() => navigate('/auth?mode=signup')}>
            Apply now
          </button>
        </div>
      </div>
    </header>
  )
}
