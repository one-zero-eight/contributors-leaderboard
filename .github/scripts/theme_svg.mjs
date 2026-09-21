import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

export const adaptiveTheme = {
  "base-100": { light: "#ffffff", dark: "#0f0c0f" },
  "base-150": { light: "#ffffff", dark: "#161216" },
  "base-200": { light: "#f6f8fa", dark: "#19141a" },
  "base-300": { light: "#d0d7de", dark: "#261f27" },
  "base-content": { light: "#1f2328", dark: "#fff4ff" },
  primary: { light: "#8250df", dark: "#9747ff" },
  "primary-content": { light: "#1f2328", dark: "#edf1fe" },
  "brand-gradient-start": { light: "#8250df", dark: "#9a2eff" },
  "brand-gradient-end": { light: "#bf3989", dark: "#d123a2" },
  "muted-content": { light: "#656d76", dark: "#a89fa8" },
  "faint-content": { light: "#6e7781", dark: "#706971" },
  "metric-content": { light: "#656d76", dark: "#b8bac5" },
  "rank-fill": { light: "#f3e8ff", dark: "#3a2157" },
  "rank-stroke": { light: "#8250df", dark: "#6a34ad" },
  "badge-fill": { light: "#f3e8ff", dark: "#2b1b3a" },
  "badge-stroke": { light: "#8250df", dark: "#45266a" },
};

function cssVariables(mode) {
  return Object.entries(adaptiveTheme)
    .map(([name, colors]) => `--${name}:${colors[mode]};`)
    .join("");
}

const adaptiveStyle = `<style id="adaptive-theme">:root{color-scheme:light dark;${cssVariables("light")}}@media (prefers-color-scheme:dark){:root{${cssVariables("dark")}}}</style>`;

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
