// Pure functions: chart-data JSON (from export_metrics.py) → ECharts options.
// Each function targets ONE chart id. Pages import both the data JSON and the
// matching builder, then pass the option to <EChart>.
import { PALETTE, fmtInt } from "./echarts-theme";

type AnyRec = Record<string, any>;

const BASE_GRID = { left: 16, right: 16, top: 24, bottom: 8, containLabel: true };

// ---- Tier A -------------------------------------------------------

export function optOverviewTldRecords(d: AnyRec) {
  const tlds = d.rows.map((r: AnyRec) => r.tld.toUpperCase());
  const domains = d.rows.map((r: AnyRec) => r.domains);
  const records = d.rows.map((r: AnyRec) => r.records);
  return {
    grid: BASE_GRID,
    tooltip: {
      trigger: "axis",
      axisPointer: { type: "shadow" },
      valueFormatter: (v: number) => fmtInt(v),
    },
    legend: { top: 0, right: 0, data: ["独立域名", "解析记录"] },
    xAxis: { type: "category", data: tlds },
    yAxis: [
      { type: "value", name: "域名数", axisLabel: { formatter: fmtInt } },
      {
        type: "value",
        name: "记录数",
        axisLabel: { formatter: fmtInt },
        splitLine: { show: false },
        position: "right",
      },
    ],
    series: [
      {
        name: "独立域名",
        type: "bar",
        data: domains,
        itemStyle: {
          borderRadius: [6, 6, 0, 0],
          color: {
            type: "linear",
            x: 0, y: 0, x2: 0, y2: 1,
            colorStops: [
              { offset: 0, color: PALETTE[0] },
              { offset: 1, color: "#0a4a8f" },
            ],
          },
        },
      },
      {
        name: "解析记录",
        type: "line",
        yAxisIndex: 1,
        data: records,
        lineStyle: { color: PALETTE[1], width: 2 },
        itemStyle: { color: PALETTE[1] },
        symbolSize: 8,
      },
    ],
    animationDuration: 1100,
    animationEasing: "cubicOut",
  };
}

export function optQueryTypes(d: AnyRec) {
  const top = d.rows.slice(0, 12);
  const cats = top.map((r: AnyRec) => r.type).reverse();
  const vals = top.map((r: AnyRec) => r.count).reverse();
  return {
    grid: { ...BASE_GRID, left: 60 },
    tooltip: {
      trigger: "axis",
      axisPointer: { type: "shadow" },
      valueFormatter: (v: number) => fmtInt(v),
    },
    xAxis: { type: "value", axisLabel: { formatter: fmtInt } },
    yAxis: { type: "category", data: cats },
    series: [
      {
        type: "bar",
        data: vals,
        itemStyle: {
          borderRadius: [0, 6, 6, 0],
          color: {
            type: "linear",
            x: 0, y: 0, x2: 1, y2: 0,
            colorStops: [
              { offset: 0, color: PALETTE[2] },
              { offset: 1, color: PALETTE[0] },
            ],
          },
        },
        label: {
          show: true,
          position: "right",
          color: "#c7c7cc",
          fontSize: 11,
          formatter: (p: any) => fmtInt(p.value),
        },
      },
    ],
    animationDuration: 1000,
  };
}

export function optColumnCompleteness(d: AnyRec) {
  const cats = d.rows.map((r: AnyRec) => r.column).reverse();
  const vals = d.rows.map((r: AnyRec) => r.non_null_pct).reverse();
  return {
    grid: { ...BASE_GRID, left: 120 },
    tooltip: {
      trigger: "axis",
      axisPointer: { type: "shadow" },
      valueFormatter: (v: number) => `${Number(v).toFixed(1)}%`,
    },
    xAxis: { type: "value", max: 100, axisLabel: { formatter: (v: number) => `${v}%` } },
    yAxis: { type: "category", data: cats, axisLabel: { fontSize: 11 } },
    series: [
      {
        type: "bar",
        data: vals,
        itemStyle: {
          borderRadius: [0, 6, 6, 0],
          color: (p: any) => {
            const v = p.value;
            if (v >= 70) return PALETTE[2];
            if (v >= 30) return PALETTE[3];
            return PALETTE[4];
          },
        },
        label: {
          show: true,
          position: "right",
          color: "#c7c7cc",
          fontSize: 11,
          formatter: (p: any) => `${Number(p.value).toFixed(1)}%`,
        },
      },
    ],
    animationDuration: 1100,
  };
}

export function optStatusCodes(d: AnyRec) {
  const data = d.rows.map((r: AnyRec) => ({
    name: r.name,
    value: r.count,
    itemStyle: { color:
      r.name === "NOERROR" ? PALETTE[2] :
      r.name === "NXDOMAIN" ? PALETTE[3] :
      r.name === "SERVFAIL" ? PALETTE[4] :
      r.name === "TIMEOUT" ? "#ff2d55" :
      PALETTE[1]
    },
  }));
  return {
    tooltip: {
      trigger: "item",
      formatter: (p: any) => `${p.name}<br/>${fmtInt(p.value)} · ${p.percent}%`,
    },
    legend: { top: 0, right: 0, itemGap: 14 },
    series: [
      {
        type: "pie",
        radius: ["45%", "72%"],
        center: ["50%", "55%"],
        avoidLabelOverlap: true,
        itemStyle: { borderColor: "#0a0a0c", borderWidth: 2 },
        label: {
          color: "#f5f5f7",
          formatter: (p: any) => `${p.name}\n${p.percent}%`,
        },
        data,
      },
    ],
    animationType: "scale",
    animationEasing: "cubicOut",
    animationDuration: 1200,
  };
}

export function optIpv4Ipv6(d: AnyRec) {
  const tlds = d.rows.map((r: AnyRec) => r.tld.toUpperCase());
  const dualPct = d.rows.map((r: AnyRec) => r.dual_pct);
  return {
    grid: BASE_GRID,
    tooltip: {
      trigger: "axis",
      axisPointer: { type: "shadow" },
      formatter: (params: any) => {
        const idx = params[0].dataIndex;
        const row = d.rows[idx];
        return `<strong>.${row.tld}</strong><br/>
          双栈:${fmtInt(row.dual_stack)} (${row.dual_pct.toFixed(1)}%)<br/>
          仅 v4:${fmtInt(row.v4_only)}<br/>
          仅 v6:${fmtInt(row.v6_only)}`;
      },
    },
    xAxis: { type: "category", data: tlds },
    yAxis: { type: "value", axisLabel: { formatter: (v: number) => `${v}%` }, max: 100 },
    series: [
      {
        name: "双栈占比",
        type: "bar",
        data: dualPct,
        itemStyle: {
          borderRadius: [6, 6, 0, 0],
          color: {
            type: "linear",
            x: 0, y: 0, x2: 0, y2: 1,
            colorStops: [
              { offset: 0, color: PALETTE[5] },
              { offset: 1, color: PALETTE[1] },
            ],
          },
        },
        label: { show: true, position: "top", color: "#c7c7cc", fontSize: 11,
          formatter: (p: any) => `${p.value.toFixed(1)}%` },
      },
    ],
    animationDuration: 1100,
  };
}

function topList15Bar(d: AnyRec, key: "domains", colorFrom: string, colorTo: string) {
  const rows = d.rows.slice(0, 15);
  const cats = rows.map((r: AnyRec) => r.provider).reverse();
  const vals = rows.map((r: AnyRec) => r[key]).reverse();
  return {
    grid: { ...BASE_GRID, left: 140 },
    tooltip: {
      trigger: "axis",
      axisPointer: { type: "shadow" },
      valueFormatter: (v: number) => fmtInt(v),
    },
    xAxis: { type: "value", axisLabel: { formatter: fmtInt } },
    yAxis: { type: "category", data: cats, axisLabel: { fontSize: 11 } },
    series: [
      {
        type: "bar",
        data: vals,
        itemStyle: {
          borderRadius: [0, 6, 6, 0],
          color: {
            type: "linear",
            x: 0, y: 0, x2: 1, y2: 0,
            colorStops: [
              { offset: 0, color: colorFrom },
              { offset: 1, color: colorTo },
            ],
          },
        },
        label: { show: true, position: "right", color: "#c7c7cc", fontSize: 11,
          formatter: (p: any) => fmtInt(p.value) },
      },
    ],
    animationDuration: 1000,
  };
}

export function optMxProviders(d: AnyRec) {
  return topList15Bar(d, "domains", PALETTE[3], PALETTE[0]);
}

export function optNsProviders(d: AnyRec) {
  return topList15Bar(d, "domains", PALETTE[2], PALETTE[1]);
}

export function optTxtEmailSecurity(d: AnyRec) {
  const tlds = d.rows.map((r: AnyRec) => r.tld.toUpperCase());
  const spf = d.rows.map((r: AnyRec) => r.spf_pct);
  const dmarc = d.rows.map((r: AnyRec) => r.dmarc_pct);
  return {
    grid: BASE_GRID,
    tooltip: { trigger: "axis", axisPointer: { type: "shadow" },
      valueFormatter: (v: number) => `${Number(v).toFixed(1)}%` },
    legend: { top: 0, right: 0, data: ["SPF", "DMARC"] },
    xAxis: { type: "category", data: tlds },
    yAxis: { type: "value", axisLabel: { formatter: (v: number) => `${v}%` } },
    series: [
      {
        name: "SPF", type: "bar",
        data: spf,
        itemStyle: { borderRadius: [6, 6, 0, 0], color: PALETTE[0] },
      },
      {
        name: "DMARC", type: "bar",
        data: dmarc,
        itemStyle: { borderRadius: [6, 6, 0, 0], color: PALETTE[1] },
      },
    ],
    animationDuration: 1000,
  };
}

export function optTtlDistribution(d: AnyRec) {
  const buckets = d.rows.map((r: AnyRec) => r.bucket);
  const vals = d.rows.map((r: AnyRec) => r.pct);
  return {
    grid: BASE_GRID,
    tooltip: {
      trigger: "axis",
      axisPointer: { type: "shadow" },
      formatter: (params: any) => {
        const idx = params[0].dataIndex;
        const row = d.rows[idx];
        return `<strong>${row.bucket}</strong><br/>${row.pct.toFixed(1)}% · ${fmtInt(row.count)}`;
      },
    },
    xAxis: { type: "category", data: buckets },
    yAxis: { type: "value", axisLabel: { formatter: (v: number) => `${v}%` } },
    series: [
      {
        type: "bar",
        data: vals,
        itemStyle: {
          borderRadius: [6, 6, 0, 0],
          color: (p: any) => {
            const c = ["#ff453a", "#ff9f0a", "#ffd60a", "#30d158", "#64d2ff", "#0071e3", "#5856d6"];
            return c[p.dataIndex] || PALETTE[0];
          },
        },
        label: { show: true, position: "top", color: "#c7c7cc", fontSize: 11,
          formatter: (p: any) => `${p.value.toFixed(1)}%` },
      },
    ],
    animationDuration: 1000,
  };
}

export function optNsRedundancy(d: AnyRec) {
  const cats = d.rows.map((r: AnyRec) => r.bucket);
  const vals = d.rows.map((r: AnyRec) => r.pct);
  return {
    grid: BASE_GRID,
    tooltip: {
      trigger: "axis",
      axisPointer: { type: "shadow" },
      formatter: (params: any) => {
        const idx = params[0].dataIndex;
        const row = d.rows[idx];
        return `<strong>${row.bucket} 台 NS</strong><br/>${row.pct.toFixed(1)}% · ${fmtInt(row.domains)} 个域名`;
      },
    },
    xAxis: { type: "category", data: cats, name: "NS 数量" },
    yAxis: { type: "value", axisLabel: { formatter: (v: number) => `${v}%` } },
    series: [
      {
        type: "bar",
        data: vals,
        itemStyle: {
          borderRadius: [6, 6, 0, 0],
          color: (p: any) => {
            const c = ["#ff453a", "#ff9f0a", "#30d158", "#30d158", "#0071e3", "#5856d6", "#bf5af2"];
            return c[p.dataIndex] || PALETTE[0];
          },
        },
        label: { show: true, position: "top", color: "#c7c7cc", fontSize: 11,
          formatter: (p: any) => `${p.value.toFixed(1)}%` },
      },
    ],
    animationDuration: 1000,
  };
}
