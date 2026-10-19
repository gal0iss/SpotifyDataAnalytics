import { useMemo, useState } from 'react';
import {
  BarChart3,
  Database,
  GitBranch,
  LayoutDashboard,
  Menu,
  Moon,
  Sun,
  X,
} from 'lucide-react';
import { LanguageContext, ThemeContext, useLanguage, useTheme, type Language, type Theme } from './i18n';
import { AnalyticsPage } from './pages/AnalyticsPage';
import { OverviewPage } from './pages/OverviewPage';
import { TechnicalPage } from './pages/TechnicalPage';
import type { Page } from './types';
import { useCopy } from './hooks/useCopy';

function AppContent() {
  const t = useCopy();
  const { language, setLanguage } = useLanguage();
  const { theme, setTheme } = useTheme();
  const [page, setPage] = useState<Page>('overview');
  const [menuOpen, setMenuOpen] = useState(false);

  const navigate = (next: Page) => {
    setPage(next);
    setMenuOpen(false);
    window.scrollTo({ top: 0, behavior: 'smooth' });
  };

  const navItems: [Page, string, typeof LayoutDashboard][] = [
    ['overview', t('overview'), LayoutDashboard],
    ['analytics', t('habits'), BarChart3],
    ['model', t('model'), Database],
    ['pipeline', t('pipeline'), GitBranch],
  ];

  return (
    <div className="app-shell">
      <header className="topbar">
        <button className="brand" onClick={() => navigate('overview')}>
          <span className="brand-mark">∿</span>
          <span>
            Listening Data <em>/ beta</em>
          </span>
        </button>

        <button
          className="mobile-menu"
          onClick={() => setMenuOpen(!menuOpen)}
          aria-label="Toggle navigation"
        >
          {menuOpen ? <X size={20} /> : <Menu size={20} />}
        </button>

        <nav className={menuOpen ? 'nav-open' : ''}>
          {navItems.map(([id, label, Icon]) => (
            <button
              className={page === id ? 'nav-link active' : 'nav-link'}
              key={id}
              onClick={() => navigate(id)}
            >
              <Icon size={15} />
              {label}
            </button>
          ))}
        </nav>

        <div className="topbar-actions">
          <button
            className="theme-toggle"
            onClick={() => setTheme(theme === 'light' ? 'dark' : 'light')}
            aria-label={theme === 'light' ? t('darkMode') : t('lightMode')}
            title={theme === 'light' ? t('darkMode') : t('lightMode')}
          >
            {theme === 'light' ? <Moon size={16} /> : <Sun size={16} />}
          </button>

          <button
            className="language-toggle"
            onClick={() => setLanguage(language === 'en' ? 'es' : 'en')}
            aria-label={`${t('language')}: ${language === 'en' ? t('spanish') : t('english')}`}
          >
            {language === 'en' ? 'ES' : 'EN'}
          </button>

          <a
            className="docs-link"
            href={`${import.meta.env.BASE_URL}Docs/Listening-data-Documentacion-Tecnica.pdf`}
            target="_blank"
            rel="noreferrer"
          >
            {t('docs')}
          </a>
        </div>
      </header>

      {page === 'overview' && <OverviewPage goTo={navigate} />}
      {page === 'analytics' && <AnalyticsPage />}
      {(page === 'model' || page === 'pipeline') && <TechnicalPage page={page} />}

      <footer className="footer">
        <span>{t('footer')}</span>
        <span>{t('stack')}</span>
      </footer>
    </div>
  );
}

export default function App() {
  const [language, setLanguage] = useState<Language>('es');
  const [theme, setTheme] = useState<Theme>(() =>
    (localStorage.getItem('spotify-analytics-theme') as Theme | null) === 'dark'
      ? 'dark'
      : 'light',
  );

  const languageValue = useMemo(() => ({ language, setLanguage }), [language]);
  const themeValue = useMemo(
    () => ({
      theme,
      setTheme: (nextTheme: Theme) => {
        localStorage.setItem('spotify-analytics-theme', nextTheme);
        setTheme(nextTheme);
      },
    }),
    [theme],
  );

  document.documentElement.dataset.theme = theme;

  return (
    <LanguageContext.Provider value={languageValue}>
      <ThemeContext.Provider value={themeValue}>
        <AppContent />
      </ThemeContext.Provider>
    </LanguageContext.Provider>
  );
}
