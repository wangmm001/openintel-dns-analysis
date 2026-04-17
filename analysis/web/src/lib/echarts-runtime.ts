// Client-side ECharts runtime. Imported only by <EChart> island.
import * as echarts from "echarts/core";
import {
  BarChart,
  LineChart,
  PieChart,
  ScatterChart,
  HeatmapChart,
  TreemapChart,
  RadarChart,
  GraphChart,
  SunburstChart,
} from "echarts/charts";
import {
  GridComponent,
  TooltipComponent,
  LegendComponent,
  TitleComponent,
  DatasetComponent,
  DataZoomComponent,
  MarkLineComponent,
  MarkPointComponent,
  VisualMapComponent,
  GraphicComponent,
  ToolboxComponent,
  PolarComponent,
} from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";
import { LabelLayout } from "echarts/features";
import { THEME } from "./echarts-theme";

echarts.use([
  BarChart,
  LineChart,
  PieChart,
  ScatterChart,
  HeatmapChart,
  TreemapChart,
  RadarChart,
  GraphChart,
  SunburstChart,
  GridComponent,
  TooltipComponent,
  LegendComponent,
  TitleComponent,
  DatasetComponent,
  DataZoomComponent,
  MarkLineComponent,
  MarkPointComponent,
  VisualMapComponent,
  GraphicComponent,
  ToolboxComponent,
  PolarComponent,
  LabelLayout,
  CanvasRenderer,
]);

echarts.registerTheme("openintel", THEME);

export { echarts };

const instances = new WeakMap<HTMLElement, echarts.ECharts>();

export function mountAll(root: ParentNode = document) {
  const targets = root.querySelectorAll<HTMLElement>("[data-echart]");
  targets.forEach((el) => {
    if (instances.has(el)) return;
    const optionRaw = el.dataset.option;
    if (!optionRaw) return;
    let option: unknown;
    try {
      option = JSON.parse(optionRaw);
    } catch (e) {
      console.error("[echart] bad option JSON", e, el.id);
      return;
    }
    const chart = echarts.init(el, "openintel", { renderer: "canvas" });
    chart.setOption(option as echarts.EChartsCoreOption);
    instances.set(el, chart);

    const ro = new ResizeObserver(() => chart.resize());
    ro.observe(el);
  });
}

export function disposeAll(root: ParentNode = document) {
  const targets = root.querySelectorAll<HTMLElement>("[data-echart]");
  targets.forEach((el) => {
    const chart = instances.get(el);
    if (chart) {
      chart.dispose();
      instances.delete(el);
    }
  });
}
