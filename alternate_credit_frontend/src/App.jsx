import { Routes, Route, Navigate } from 'react-router-dom'

// Customer pages
import LandingPage        from './pages/customer/LandingPage'
import AuthPage           from './pages/customer/AuthPage'
import CustomerDashboard  from './pages/customer/CustomerDashboard'
import ProcessingPage     from './pages/customer/ProcessingPage'
import ResultPage         from './pages/customer/ResultPage'

// Manager pages
import ManagerLogin       from './pages/manager/ManagerLogin'
import ManagerDashboard   from './pages/manager/ManagerDashboard'
import DecisionPage       from './pages/manager/DecisionPage'

export default function App() {
  return (
    <Routes>
      {/* Customer routes */}
      <Route path="/"                   element={<LandingPage />} />
      <Route path="/auth"               element={<AuthPage />} />
      <Route path="/dashboard"          element={<CustomerDashboard />} />
      <Route path="/processing"         element={<ProcessingPage />} />
      <Route path="/result"             element={<ResultPage />} />

      {/* Manager routes */}
      <Route path="/manager"            element={<Navigate to="/manager/login" replace />} />
      <Route path="/manager/login"      element={<ManagerLogin />} />
      <Route path="/manager/dashboard"  element={<ManagerDashboard />} />
      <Route path="/manager/review/:id" element={<DecisionPage />} />

      {/* Fallback */}
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  )
}
