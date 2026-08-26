import assert from "node:assert/strict";
import test from "node:test";

import {
  monthsBackDate,
  rankContributors,
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
  const exclusions = validConfig.excludedAccounts;
  assert.equal(shouldExcludeLogin("dependabot[bot]", exclusions), true);
  assert.equal(shouldExcludeLogin("CI-BOT", exclusions), true);
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
