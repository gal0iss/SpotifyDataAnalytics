import { useEffect, useRef } from 'react';
import * as echarts from 'echarts';

interface ChartProps { option: echarts.EChartsOption; height?: number; }

export function Chart({ option, height = 300 }: ChartProps) {
  const node = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!node.current) return;
    const chart = echarts.init(node.current);
    chart.setOption(option);
    const resize = () => chart.resize();
    window.addEventListener('resize', resize);
    return () => { window.removeEventListener('resize', resize); chart.dispose(); };
  }, [option]);
  return <div ref={node} style={{ height, width: '100%' }} aria-label="Analytical chart" />;
}
