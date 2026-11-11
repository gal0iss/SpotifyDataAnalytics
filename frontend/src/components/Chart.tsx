import { useEffect, useRef } from 'react';
import * as echarts from 'echarts';

interface ChartProps { option: echarts.EChartsOption; height?: number; }

export function Chart({ option, height = 300 }: ChartProps) {
  const node = useRef<HTMLDivElement>(null);
  const chart = useRef<echarts.ECharts | null>(null);

  useEffect(() => {
    if (!node.current) return;
    chart.current = echarts.init(node.current);
    const resize = () => chart.current?.resize();
    window.addEventListener('resize', resize);
    return () => {
      window.removeEventListener('resize', resize);
      chart.current?.dispose();
      chart.current = null;
    };
  }, []);

  useEffect(() => {
    chart.current?.setOption(option, { notMerge: true });
  }, [option]);

  return <div ref={node} style={{ height, width: '100%' }} aria-label="Analytical chart" />;
}
