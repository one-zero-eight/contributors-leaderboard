import assert from "node:assert/strict";
import test from "node:test";

import { leaderboardConfig } from "../../leaderboard.config.mjs";
import {
  buildRepositoryBreakdowns,
  monthsBackDate,
  rankContributors,
  serializeMonthlyMarkdown,
  serializeTypstData,
  shouldExcludeLogin,
  validateConfig,
} from "./generate_leaderboard.mjs";

const validConfig = {
  organization: "one-zero-eight",
  months: 6,
  monthlyOverall: {
    id: "overall_month",
    title: "Overall",
    description: "Last month",
    months: 1,
  },
  excludedAccounts: { suffixes: ["[bot]"], logins: ["ci-bot"] },
  leaderboards: [
    { id: "overall", title: "Overall", description: "All repositories" },
    {
      id: "website",
      title: "Website",
      description: "Website repository",
      repository: "website",
    },
  ],
};

test("validates the maintained leaderboard config", () => {
  assert.doesNotThrow(() => validateConfig(validConfig));
  assert.throws(
    () =>
      validateConfig({
        ...validConfig,
        leaderboards: [...validConfig.leaderboards, validConfig.leaderboards[0]],
      }),
    /Duplicate leaderboard id/,
  );
  assert.throws(
    () =>
      validateConfig({
        ...validConfig,
        leaderboards: validConfig.leaderboards.filter(
          (section) => section.repository,
        ),
      }),
    /exactly one organization-wide leaderboard/,
  );
  assert.throws(
    () =>
      validateConfig({
        ...validConfig,
        monthlyOverall: { ...validConfig.monthlyOverall, months: 0 },
      }),
    /monthlyOverall.months must be a positive integer/,
  );
});

test("excludes configured automation accounts case-insensitively", () => {
  const exclusions = leaderboardConfig.excludedAccounts;
  assert.equal(shouldExcludeLogin("dependabot[bot]", exclusions), true);
  assert.equal(shouldExcludeLogin("CI-BOT", exclusions), true);
  assert.equal(shouldExcludeLogin("Claude", exclusions), true);
  assert.equal(shouldExcludeLogin("human-contributor", exclusions), false);
});

test("ranks every positive commit contributor deterministically", () => {
  const commits = new Map([
    ["zeta", 2],
    ["Beta", 4],
    ["alpha", 4],
    ["ignored", 0],
  ]);
  const metrics = new Map([
    ["alpha", { prsMerged: 2, prsOpened: 3, issues: 4 }],
  ]);

  assert.deepEqual(rankContributors(commits, metrics), [
    {
      login: "alpha",
      commits: 4,
      prsMerged: 2,
      prsOpened: 3,
      issues: 4,
    },
    {
      login: "Beta",
      commits: 4,
      prsMerged: 0,
      prsOpened: 0,
      issues: 0,
    },
    {
      login: "zeta",
      commits: 2,
      prsMerged: 0,
      prsOpened: 0,
      issues: 0,
    },
  ]);
});

test("uses a clamped rolling calendar-month window", () => {
  assert.equal(
    monthsBackDate(new Date("2026-08-31T12:00:00Z"), 6).toISOString(),
    "2026-02-28T00:00:00.000Z",
  );
});

test("serializes safe Typst data with totals and empty sections", () => {
  const output = serializeTypstData({
    organization: "one-zero-eight",
    months: 6,
    from: "2026-02-25",
    to: "2026-08-25",
    generatedAt: "2026-08-25T00:00:00.000Z",
    leaderboards: [
      {
        id: "overall",
        title: 'Overall "contribution"',
        description: "All repositories",
        contributors: [
          {
            login: "alice",
            commits: 3,
            prsMerged: 2,
            prsOpened: 4,
            issues: 1,
          },
        ],
      },
      {
        id: "website",
        title: "Website",
        description: "Website repository",
        repository: "website",
        contributors: [],
      },
    ],
  });

  assert.match(output, /title: "Overall \\"contribution\\""/);
  assert.match(output, /commits: 3/);
  assert.match(output, /prs_merged: 2/);
  assert.match(output, /repository: none/);
  assert.match(output, /months: 6/);
  assert.match(output, /from: "2026-02-25"/);
  assert.match(output, /contributors: \(\n\n      \)/);
});

test("builds deterministic per-repository commit breakdowns", () => {
  const breakdowns = buildRepositoryBreakdowns(
    new Map([
      ["website", new Map([["alice", 4], ["bob", 1], ["ignored", 0]])],
      ["monorepo", new Map([["alice", 10], ["carol", 2]])],
      ["events", new Map([["alice", 4]])],
    ]),
  );

  assert.deepEqual(breakdowns.get("alice"), [
    { name: "monorepo", commits: 10 },
    { name: "events", commits: 4 },
    { name: "website", commits: 4 },
  ]);
  assert.deepEqual(breakdowns.get("bob"), [{ name: "website", commits: 1 }]);
  assert.equal(breakdowns.has("ignored"), false);
});

test("serializes monthly Markdown for every contributor", () => {
  const markdown = serializeMonthlyMarkdown("one-zero-eight", {
    title: "Contribution",
    months: 1,
    contributors: [
      {
        login: "alice",
        commits: 14,
        prsMerged: 2,
        prsOpened: 3,
        issues: 1,
        repositories: [
          { name: "monorepo", commits: 10 },
          { name: "website", commits: 4 },
        ],
      },
      {
        login: "bob",
        commits: 5,
        prsMerged: 0,
        prsOpened: 0,
        issues: 0,
        repositories: [{ name: "website", commits: 5 }],
      },
    ],
  });

  assert.match(markdown, /^### Contribution — last month\n/);
  assert.match(
    markdown,
    /\*\*19 commits · 2 PRs merged · 3 PRs opened · 1 issues opened · 2 contributors\*\*/,
  );
  assert.match(
    markdown,
    /\| Rank \| Contributor \| Commits \| PRs \| Issues \|/,
  );
  assert.match(
    markdown,
    /<a href="https:\/\/github\.com\/alice"><img src="https:\/\/github\.com\/alice\.png\?size=32" width="24" height="24" align="absmiddle"><\/a> <a href="https:\/\/github\.com\/alice">alice<\/a>/,
  );
  assert.match(
    markdown,
    /<details><summary><strong>14<\/strong><\/summary><a href="https:\/\/github\.com\/one-zero-eight\/monorepo\/commits\?author=alice">monorepo — 10 commits<\/a><br><a href="https:\/\/github\.com\/one-zero-eight\/website\/commits\?author=alice">website — 4 commits<\/a><\/details>/,
  );
  assert.match(
    markdown,
    /\| 2 \| <a href="https:\/\/github\.com\/bob"><img src="https:\/\/github\.com\/bob\.png\?size=32" width="24" height="24" align="absmiddle"><\/a> <a href="https:\/\/github\.com\/bob">bob<\/a> \| <details><summary><strong>5<\/strong><\/summary><a href="https:\/\/github\.com\/one-zero-eight\/website\/commits\?author=bob">website — 5 commits<\/a><\/details> \| 0 \/ 0 \| 0 \|/,
  );
  assert.match(markdown, /\| 1 \| .* \| .* \| 2 \/ 3 \| 1 \|/);
});

test("serializes empty monthly Markdown and escapes HTML", () => {
  const empty = serializeMonthlyMarkdown("one-zero-eight", {
    title: "Contribution",
    months: 1,
    contributors: [],
  });
  assert.match(
    empty,
    /\*\*0 commits · 0 PRs merged · 0 PRs opened · 0 issues opened · 0 contributors\*\*/,
  );
  assert.equal(empty.includes("| 1 |"), false);

  const escaped = serializeMonthlyMarkdown("one-zero-eight", {
    title: "Contribution",
    months: 1,
    contributors: [
      {
        login: 'a<b>"c',
        commits: 1,
        prsMerged: 0,
        prsOpened: 0,
        issues: 0,
        repositories: [],
      },
    ],
  });
  assert.match(escaped, />a&lt;b&gt;&quot;c<\/a>/);
});

test("ignores repository metadata when serializing Typst data", () => {
  const withoutRepos = serializeTypstData({
    organization: "one-zero-eight",
    months: 6,
    from: "2026-02-25",
    to: "2026-08-25",
    generatedAt: "2026-08-25T00:00:00.000Z",
    leaderboards: [
      {
        id: "overall_month",
        title: "Overall contribution",
        description: "Monthly",
        months: 1,
        from: "2026-07-25",
        to: "2026-08-25",
        contributors: [
          {
            login: "alice",
            commits: 3,
            prsMerged: 1,
            prsOpened: 1,
            issues: 0,
          },
        ],
      },
    ],
  });
  const withRepos = serializeTypstData({
    organization: "one-zero-eight",
    months: 6,
    from: "2026-02-25",
    to: "2026-08-25",
    generatedAt: "2026-08-25T00:00:00.000Z",
    leaderboards: [
      {
        id: "overall_month",
        title: "Overall contribution",
        description: "Monthly",
        months: 1,
        from: "2026-07-25",
        to: "2026-08-25",
        contributors: [
          {
            login: "alice",
            commits: 3,
            prsMerged: 1,
            prsOpened: 1,
            issues: 0,
            repositories: [{ name: "website", commits: 3 }],
          },
        ],
      },
    ],
  });

  assert.equal(withRepos, withoutRepos);
  assert.equal(withRepos.includes("repositories"), false);
});
