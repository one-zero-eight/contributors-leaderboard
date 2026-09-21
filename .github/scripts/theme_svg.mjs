import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

export const adaptiveTheme = {
  "base-100": { light: "transparent", dark: "transparent" },
  "base-150": { light: "#f4eff5", dark: "#161216" },
  "base-200": { light: "#faf7fb", dark: "#19141a" },
  "base-300": { light: "#ded7e0", dark: "#261f27" },
  "base-content": { light: "#211c21", dark: "#fff4ff" },
  primary: { light: "#7c3aed", dark: "#9747ff" },
  "primary-content": { light: "#2f2833", dark: "#edf1fe" },
  "brand-gradient-start": { light: "#7c3aed", dark: "#9a2eff" },
  "brand-gradient-end": { light: "#b4237b", dark: "#d123a2" },
  "muted-content": { light: "#6f6870", dark: "#a89fa8" },
  "faint-content": { light: "#8a828b", dark: "#706971" },
  "metric-content": { light: "#5f5964", dark: "#b8bac5" },
  "rank-fill": { light: "#efe6f9", dark: "#3a2157" },
  "rank-stroke": { light: "#8b58c9", dark: "#6a34ad" },
  "badge-fill": { light: "#f3eafc", dark: "#2b1b3a" },
  "badge-stroke": { light: "#b994df", dark: "#45266a" },
};

function cssVariables(mode) {
  return Object.entries(adaptiveTheme)
    .map(([name, colors]) => `--${name}:${colors[mode]};`)
    .join("");
}

const adaptiveStyle = `<style id="adaptive-theme">:root{${cssVariables("light")}}@media (prefers-color-scheme:dark){:root{${cssVariables("dark")}}}</style>`;

function escapeRegExp(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

export function applyAdaptiveTheme(svg) {
  if (svg.includes('id="adaptive-theme"')) return svg;

  let themedSvg = svg;
  for (const [name, colors] of Object.entries(adaptiveTheme)) {
    themedSvg = themedSvg.replace(
      new RegExp(escapeRegExp(colors.dark), "gi"),
      `var(--${name})`,
    );
  }

  const svgTag = themedSvg.match(/<svg\b[^>]*>/i)?.[0];
  if (!svgTag) throw new Error("Input does not contain an <svg> root element");

  return themedSvg.replace(svgTag, `${svgTag}${adaptiveStyle}`);
}

export async function main(files = process.argv.slice(2)) {
  if (files.length === 0) {
    throw new Error("Pass at least one SVG path");
  }

  for (const file of files) {
    const svg = await fs.readFile(file, "utf8");
    const themedSvg = applyAdaptiveTheme(svg);
    await fs.writeFile(file, themedSvg, "utf8");
    console.log(`Applied adaptive theme to ${path.normalize(file)}`);
  }
}

const scriptPath = process.argv[1] ? pathToFileURL(process.argv[1]).href : "";
if (import.meta.url === scriptPath) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
