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

const table = `### Contribution — last month

**3 commits · 1 PRs merged · 1 PRs opened · 0 issues opened · 1 contributors**

| Rank | Contributor | Commits | PRs | Issues |
|---:|---|---:|---:|---:|
| 1 | <a href="https://github.com/alice"><img src="https://github.com/alice.png?size=32" width="24" height="24" align="absmiddle"></a> <a href="https://github.com/alice">alice</a> | **3** | 1 / 1 | 0 |
`;

const updatedTable = table.replace("**3**", "**4**");

test("adopts the monthly SVG embed and removes the details wrapper", () => {
  const updated = updateProfileReadme(baseReadme, table);

  assert.match(updated, new RegExp(summarySvg.replaceAll("/", "\\/")));
  assert.equal(updated.includes("<details>"), false);
  assert.equal(updated.includes("<summary>"), false);
  assert.equal(updated.includes("</details>"), false);
  assert.match(updated, new RegExp(START_MARKER));
  assert.match(updated, new RegExp(END_MARKER));
  assert.match(updated, /### Contribution — last month/);
  assert.match(updated, /<a href="https:\/\/github\.com\/alice">alice<\/a>/);
  assert.equal(updated.includes(monthlySvg), false);
});

test("unwraps an existing details-wrapped marked section with nested commit details", () => {
  const wrapped = `## Contributing

${summarySvg}

<details>
<summary>View last month’s contributor leaderboard</summary>

${START_MARKER}
### Contribution — last month

| Rank | Contributor | Commits | PRs | Issues |
|---:|---|---:|---:|---:|
| 1 | alice | <details><summary><strong>3</strong></summary><a href="https://github.com/one-zero-eight/website/commits?author=alice">website — 3 commits</a></details> | 1 / 1 | 0 |
${END_MARKER}

</details>
`;

  const updated = updateProfileReadme(wrapped, table);
  assert.equal(updated.includes("View last month"), false);
  assert.equal(
    (updated.match(/<details>/g) ?? []).length,
    0,
  );
  assert.match(updated, /### Contribution — last month/);
  assert.match(updated, new RegExp(summarySvg.replaceAll("/", "\\/")));
});

test("replaces marked content on subsequent runs and is idempotent", () => {
  const first = updateProfileReadme(baseReadme, table);
  const second = updateProfileReadme(first, updatedTable);
  const third = updateProfileReadme(second, updatedTable);

  assert.equal(second.includes("| **4** |"), true);
  assert.equal(second.includes("| **3** |"), false);
  assert.equal(third, second);
  assert.equal(second.includes("<details>"), false);
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
  assert.equal(updated.includes("View last month"), false);
});
