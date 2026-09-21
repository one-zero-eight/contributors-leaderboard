import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

export const START_MARKER = "<!-- monthly-leaderboard:start -->";
export const END_MARKER = "<!-- monthly-leaderboard:end -->";

const MONTHLY_SVG_EMBED =
  /<img\s+[^>]*src="https:\/\/raw\.githubusercontent\.com\/one-zero-eight\/contributors-leaderboard\/main\/leaderboard-overall-month\.svg"[^>]*\/?>/i;

const MONTHLY_DETAILS_OPEN =
  /<details>\s*<summary>\s*View last month['’]s contributor leaderboard\s*<\/summary>\s*/i;

function normalizeNewlines(value) {
  return String(value).replaceAll("\r\n", "\n");
}

function wrappedTable(markdown) {
  const body = normalizeNewlines(markdown).trimEnd();
  return `${START_MARKER}\n${body}\n${END_MARKER}`;
}

function findMarkerRange(source) {
  const startIndex = source.indexOf(START_MARKER);
  const endIndex = source.indexOf(END_MARKER);

  if (startIndex === -1 && endIndex === -1) return null;

  if (startIndex === -1 || endIndex === -1) {
    throw new Error("Monthly leaderboard markers are incomplete");
  }
  if (endIndex < startIndex) {
    throw new Error("Monthly leaderboard markers are reversed");
  }
  if (
    source.indexOf(START_MARKER, startIndex + START_MARKER.length) !== -1 ||
    source.indexOf(END_MARKER, endIndex + END_MARKER.length) !== -1
  ) {
    throw new Error("Monthly leaderboard markers are duplicated");
  }

  return { start: startIndex, end: endIndex + END_MARKER.length };
}

function findMonthlySvgRange(source) {
  const matches = [...source.matchAll(new RegExp(MONTHLY_SVG_EMBED.source, "gi"))];
  if (matches.length === 0) return null;
  if (matches.length !== 1) {
    throw new Error(
      "Expected exactly one monthly leaderboard SVG embed for first-run adoption",
    );
  }
  const match = matches[0];
  return { start: match.index, end: match.index + match[0].length };
}

function expandThroughMonthlyDetails(source, innerRange) {
  const before = source.slice(0, innerRange.start);
  const after = source.slice(innerRange.end);
  const open = before.match(
    new RegExp(`${MONTHLY_DETAILS_OPEN.source}$`, "i"),
  );
  const close = after.match(/^\s*<\/details>/i);

  if (open && close) {
    return {
      start: innerRange.start - open[0].length,
      end: innerRange.end + close[0].length,
    };
  }
  if (open || close) {
    throw new Error("Monthly leaderboard details wrapper is malformed");
  }
  return innerRange;
}

export function updateProfileReadme(readme, markdown) {
  const source = normalizeNewlines(readme);
  const replacement = wrappedTable(markdown);
  const markerRange = findMarkerRange(source);

  if (markerRange) {
    const range = expandThroughMonthlyDetails(source, markerRange);
    return (
      source.slice(0, range.start) + replacement + source.slice(range.end)
    );
  }

  const svgRange = findMonthlySvgRange(source);
  if (!svgRange) {
    throw new Error(
      "Expected exactly one monthly leaderboard SVG embed for first-run adoption",
    );
  }

  const range = expandThroughMonthlyDetails(source, svgRange);
  return source.slice(0, range.start) + replacement + source.slice(range.end);
}

export async function main(argv = process.argv.slice(2)) {
  const [readmePath, markdownPath] = argv;
  if (!readmePath || !markdownPath) {
    throw new Error(
      "Usage: node update_profile_readme.mjs <readme-path> <markdown-path>",
    );
  }

  const readme = await fs.readFile(readmePath, "utf8");
  const markdown = await fs.readFile(markdownPath, "utf8");
  const updated = updateProfileReadme(readme, markdown);
  if (updated === normalizeNewlines(readme)) {
    console.log(`No profile README changes for ${path.normalize(readmePath)}`);
    return;
  }

  await fs.writeFile(readmePath, updated, "utf8");
  console.log(`Updated ${path.normalize(readmePath)}`);
}

const scriptPath = process.argv[1] ? pathToFileURL(process.argv[1]).href : "";
if (import.meta.url === scriptPath) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
