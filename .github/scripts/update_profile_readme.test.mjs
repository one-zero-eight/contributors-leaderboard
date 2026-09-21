import assert from "node:assert/strict";
import test from "node:test";

import {
  END_MARKER,
  START_MARKER,
  updateProfileReadme,
} from "./update_profile_readme.mjs";

const summarySvg =
  '<img src="https://raw.githubusercontent.com/one-zero-eight/contributors-leaderboard/main/leaderboard-overall-summary.svg" alt="Overall contribution statistics for the last six months" width="100%" />';
const monthlySvg =
  '<img src="https://raw.githubusercontent.com/one-zero-eight/contributors-leaderboard/main/leaderboard-overall-month.svg" alt="Overall contributor leaderboard for the last month" width="100%" />';

const baseReadme = `## Contributing

Intro text.

${summarySvg}

<details>
<summary>View last month’s contributor leaderboard</summary>

${monthlySvg}

</details>
`;

const table = `### Overall contribution — Last month

**3 commits · 1 PRs merged · 1 PRs opened · 0 issues opened · 1 contributors**

| Rank | Contributor | Commits | PRs merged | PRs opened | Issues |
|---:|---|---:|---:|---:|---:|
| 1 | <img src="https://github.com/alice.png?size=32" width="24" height="24" align="absmiddle"> @alice | **3** | 1 | 1 | 0 |
`;

const updatedTable = table.replace("**3**", "**4**");

test("adopts the monthly SVG embed on first run", () => {
  const updated = updateProfileReadme(baseReadme, table);

  assert.match(updated, new RegExp(summarySvg.replaceAll("/", "\\/")));
  assert.match(updated, /<details>\n<summary>View last month/);
  assert.match(updated, new RegExp(START_MARKER));
  assert.match(updated, new RegExp(END_MARKER));
  assert.match(updated, /@alice/);
  assert.equal(updated.includes(monthlySvg), false);
  assert.match(updated, /<\/details>\n$/);
});

test("replaces marked content on subsequent runs and is idempotent", () => {
  const first = updateProfileReadme(baseReadme, table);
  const second = updateProfileReadme(first, updatedTable);
  const third = updateProfileReadme(second, updatedTable);

  assert.equal(second.includes("| **4** |"), true);
  assert.equal(second.includes("| **3** |"), false);
  assert.equal(third, second);
  assert.equal((second.match(new RegExp(START_MARKER, "g")) ?? []).length, 1);
  assert.equal((second.match(new RegExp(END_MARKER, "g")) ?? []).length, 1);
});

test("rejects malformed markers and ambiguous embeds", () => {
  assert.throws(
    () =>
      updateProfileReadme(
        `${START_MARKER}\nold\n`,
        table,
      ),
    /markers are incomplete/,
  );
  assert.throws(
    () =>
      updateProfileReadme(
        `${END_MARKER}\n${START_MARKER}\n`,
        table,
      ),
    /markers are reversed/,
  );
  assert.throws(
    () =>
      updateProfileReadme(
        `${START_MARKER}\na\n${END_MARKER}\n${START_MARKER}\nb\n${END_MARKER}`,
        table,
      ),
    /markers are duplicated/,
  );
  assert.throws(
    () => updateProfileReadme("no monthly embed here", table),
    /Expected exactly one monthly leaderboard SVG embed/,
  );
  assert.throws(
    () =>
      updateProfileReadme(
        `${monthlySvg}\n${monthlySvg}`,
        table,
      ),
    /Expected exactly one monthly leaderboard SVG embed/,
  );
});

test("preserves surrounding content outside the replacement span", () => {
  const updated = updateProfileReadme(baseReadme, table);
  assert.match(updated, /^## Contributing\n\nIntro text\.\n\n/);
  assert.match(updated, new RegExp(summarySvg.replaceAll("/", "\\/")));
  assert.match(updated, /<summary>View last month’s contributor leaderboard<\/summary>/);
});
