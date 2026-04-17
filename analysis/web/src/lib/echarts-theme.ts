// ECharts dark theme matching the site's OKLCH palette.
// Usage: echarts.registerTheme('openintel', THEME); echarts.init(el, 'openintel', ...)
export const PALETTE = [
  "#0071e3",
  "#5856d6",
  "#30d158",
  "#ff9f0a",
  "#ff453a",
  "#64d2ff",
  "#bf5af2",
  "#ffd60a",
  "#8e8e93",
  "#ff2d55",
];

export const THEME = {
  color: PALETTE,
  backgroundColor: "transparent",
  textStyle: {
    color: "#f5f5f7",
    fontFamily:
      "'Inter', 'Noto Sans SC', 'PingFang SC', system-ui, sans-serif",
  },
  title: {
    textStyle: { color: "#f5f5f7", fontWeight: 600 },
    subtextStyle: { color: "#86868b" },
  },
  legend: {
    textStyle: { color: "#c7c7cc" },
    icon: "roundRect",
    itemWidth: 10,
    itemHeight: 10,
    itemGap: 18,
  },
  grid: { containLabel: true, left: 8, right: 12, top: 36, bottom: 8 },
  tooltip: {
    backgroundColor: "rgba(14,14,18,0.92)",
    borderColor: "#2a2a30",
    borderWidth: 1,
    textStyle: { color: "#f5f5f7", fontSize: 12 },
    extraCssText: "backdrop-filter: blur(12px); border-radius: 10px;",
  },
  xAxis: {
    axisLine: { lineStyle: { color: "#2a2a30" } },
    axisTick: { lineStyle: { color: "#2a2a30" } },
    axisLabel: { color: "#86868b", fontSize: 11 },
    splitLine: { show: false },
  },
  yAxis: {
    axisLine: { show: false },
    axisTick: { show: false },
    axisLabel: { color: "#86868b", fontSize: 11 },
    splitLine: { lineStyle: { color: "#1d1d22" } },
  },
  bar: {
    itemStyle: { borderRadius: [6, 6, 0, 0] },
  },
  line: {
    smooth: true,
    symbol: "circle",
    symbolSize: 6,
    lineStyle: { width: 2 },
  },
};

export function fmtInt(v: number): string {
  if (Math.abs(v) >= 1e9) return (v / 1e9).toFixed(1) + "B";
  if (Math.abs(v) >= 1e6) return (v / 1e6).toFixed(1) + "M";
  if (Math.abs(v) >= 1e3) return (v / 1e3).toFixed(1) + "K";
  return v.toString();
}

export function fmtPct(v: number, decimals = 1): string {
  return v.toFixed(decimals) + "%";
}
