import { renderEmissionsSankey } from "../../charts/emissions_sankey.js";

export function mountSankeyChart(config) {
  return renderEmissionsSankey(config);
}
