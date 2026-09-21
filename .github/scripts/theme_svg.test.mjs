import assert from "node:assert/strict";
import test from "node:test";

import {
  adaptiveTheme,
  applyAdaptiveTheme,
  sourcePalette,
} from "./theme_svg.mjs";

test("replaces Typst source palette colors with CSS variables", () => {
  const input =
    '<svg viewBox="0 0 10 10"><path fill="#0f0c0f"/><path fill="#19141A" stroke="#261f27"/><path fill="#9747ff"/></svg>';

  const output = applyAdaptiveTheme(input);

  assert.match(output, /fill="var\(--base-100\)"/);
  assert.match(output, /fill="var\(--base-200\)"/);
  assert.match(output, /stroke="var\(--base-300\)"/);
  assert.match(output, /fill="var\(--primary\)"/);
  assert.equal(sourcePalette["base-100"], "#0f0c0f");
  assert.equal(adaptiveTheme["base-100"].dark, "transparent");
});

test("injects light defaults and a dark prefers-color-scheme override", () => {
  const output = applyAdaptiveTheme('<svg><path fill="#fff4ff"/></svg>');

  assert.match(output, /<style id="adaptive-theme">/);
  assert.match(output, /--base-100:transparent/);
  assert.match(output, /@media \(prefers-color-scheme:dark\)/);
  assert.match(output, /--base-100:transparent/);
  assert.match(
    output,
    new RegExp(`--primary:${adaptiveTheme.primary.dark.replace("#", "\\#")}`),
  );
});

test("is idempotent", () => {
  const once = applyAdaptiveTheme('<svg><path fill="#0f0c0f"/></svg>');
  const twice = applyAdaptiveTheme(once);

  assert.equal(twice, once);
  assert.equal((twice.match(/id="adaptive-theme"/g) ?? []).length, 1);
});

test("rejects non-SVG input", () => {
  assert.throws(
    () => applyAdaptiveTheme("<div>not svg</div>"),
    /does not contain an <svg> root element/,
  );
});
