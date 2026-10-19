import { translations, useLanguage, type CopyKey } from '../i18n';

export function useCopy() {
  const { language } = useLanguage();
  const copy = translations[language];
  return (key: CopyKey) => copy[key];
}
