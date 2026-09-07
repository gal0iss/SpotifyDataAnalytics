import { useEffect, useState } from 'react';

export type QueryState<T> = { data: T | null; loading: boolean; error: string | null };

export function useQuery<T>(loader: () => Promise<T>, dependencies: unknown[]): QueryState<T> {
  const [state, setState] = useState<QueryState<T>>({ data: null, loading: true, error: null });

  useEffect(() => {
    let active = true;
    setState({ data: null, loading: true, error: null });
    loader().then((data) => {
      if (active) setState({ data, loading: false, error: null });
    }).catch((error: unknown) => {
      if (active) setState({ data: null, loading: false, error: error instanceof Error ? error.message : 'Unable to query the dataset.' });
    });
    return () => { active = false; };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, dependencies);

  return state;
}
