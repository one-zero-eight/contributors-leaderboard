import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

import { leaderboardConfig } from "../../leaderboard.config.mjs";

const API = "https://api.github.com";
const GQL = "https://api.github.com/graphql";
const DEFAULT_OUTPUT = "typst/generated-data.typ";
const DEFAULT_MARKDOWN_OUTPUT = "leaderboard-overall-month.md";

function sleep(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function isoDate(date) {
  return date.toISOString().slice(0, 10);
}

export function monthsBackDate(now, months) {
  const date = new Date(now);
  const originalDay = date.getUTCDate();

  date.setUTCDate(1);
  date.setUTCMonth(date.getUTCMonth() - months);
  const daysInTargetMonth = new Date(
    Date.UTC(date.getUTCFullYear(), date.getUTCMonth() + 1, 0),
  ).getUTCDate();
  date.setUTCDate(Math.min(originalDay, daysInTargetMonth));
  date.setUTCHours(0, 0, 0, 0);

  return date;
}

export function validateConfig(config) {
  if (!config?.organization || typeof config.organization !== "string") {
    throw new Error("Config organization must be a non-empty string");
  }
  if (!Number.isInteger(config.months) || config.months <= 0) {
    throw new Error("Config months must be a positive integer");
  }
  if (
    !config.monthlyOverall ||
    !Number.isInteger(config.monthlyOverall.months) ||
    config.monthlyOverall.months <= 0
  ) {
    throw new Error("Config monthlyOverall.months must be a positive integer");
  }
  if (
    !config.monthlyOverall.id ||
    !/^[a-z][a-z0-9_]*$/.test(config.monthlyOverall.id) ||
    !config.monthlyOverall.title ||
    !config.monthlyOverall.description
  ) {
    throw new Error("Config monthlyOverall needs a valid id, title, and description");
  }
  if (!Array.isArray(config.leaderboards) || config.leaderboards.length === 0) {
    throw new Error("Config must contain at least one leaderboard");
  }

  const ids = new Set();
  let overallCount = 0;
  for (const section of config.leaderboards) {
    if (!section?.id || !/^[a-z][a-z0-9_]*$/.test(section.id)) {
      throw new Error(`Invalid leaderboard id: ${section?.id ?? "missing"}`);
    }
    if (ids.has(section.id)) {
      throw new Error(`Duplicate leaderboard id: ${section.id}`);
    }
    if (!section.title || !section.description) {
      throw new Error(`Leaderboard ${section.id} needs a title and description`);
    }
    if (!section.repository) overallCount += 1;
    ids.add(section.id);
  }
  if (ids.has(config.monthlyOverall.id)) {
    throw new Error(`Duplicate leaderboard id: ${config.monthlyOverall.id}`);
  }
  if (overallCount !== 1) {
    throw new Error("Config must contain exactly one organization-wide leaderboard");
  }
}

export function shouldExcludeLogin(login, excludedAccounts = {}) {
  const normalizedLogin = String(login).toLowerCase();
  const blockedLogins = new Set(
    (excludedAccounts.logins ?? []).map((value) => value.toLowerCase()),
  );
  if (blockedLogins.has(normalizedLogin)) return true;

  return (excludedAccounts.suffixes ?? []).some((suffix) =>
    normalizedLogin.endsWith(suffix.toLowerCase()),
  );
}

export function rankContributors(commitMap, metricsMap = new Map()) {
  return Array.from(commitMap.entries())
    .filter(([, commits]) => commits > 0)
    .map(([login, commits]) => ({
      login,
      commits,
      prsMerged: metricsMap.get(login)?.prsMerged ?? 0,
      prsOpened: metricsMap.get(login)?.prsOpened ?? 0,
      issues: metricsMap.get(login)?.issues ?? 0,
    }))
    .sort(
      (left, right) =>
        right.commits - left.commits ||
        left.login.localeCompare(right.login, "en", { sensitivity: "base" }),
    );
}

function typstString(value) {
  return JSON.stringify(String(value));
}

function sumContributors(contributors) {
  return contributors.reduce(
    (totals, contributor) => ({
      commits: totals.commits + contributor.commits,
      prsMerged: totals.prsMerged + contributor.prsMerged,
      prsOpened: totals.prsOpened + contributor.prsOpened,
      issues: totals.issues + contributor.issues,
    }),
    { commits: 0, prsMerged: 0, prsOpened: 0, issues: 0 },
  );
}

export function buildRepositoryBreakdowns(repositoryCommits) {
  const byLogin = new Map();
  for (const [repository, commits] of repositoryCommits.entries()) {
    for (const [login, count] of commits.entries()) {
      if (count <= 0) continue;
      const existing = byLogin.get(login) ?? [];
      existing.push({ name: repository, commits: count });
      byLogin.set(login, existing);
    }
  }

  for (const [login, repositories] of byLogin.entries()) {
    repositories.sort(
      (left, right) =>
        right.commits - left.commits ||
        left.name.localeCompare(right.name, "en", { sensitivity: "base" }),
    );
    byLogin.set(login, repositories);
  }
  return byLogin;
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function periodHeading(months) {
  return months === 1 ? "Last month" : `Last ${months} months`;
}

function renderCommitsCell(organization, login, commits, repositories = []) {
  if (repositories.length === 0) {
    return `**${commits}**`;
  }

  const links = repositories
    .map((repository) => {
      const href = `https://github.com/${encodeURIComponent(organization)}/${encodeURIComponent(repository.name)}/commits?author=${encodeURIComponent(login)}`;
      const label = `${escapeHtml(repository.name)} — ${repository.commits} commits`;
      return `<a href="${href}">${label}</a>`;
    })
    .join("<br>");

  return `<details><summary><strong>${commits}</strong></summary>${links}</details>`;
}

export function serializeMonthlyMarkdown(organization, section) {
  const contributors = section.contributors ?? [];
  const totals = sumContributors(contributors);
  const months = section.months ?? 1;
  const heading = `### ${section.title} — ${periodHeading(months)}`;
  const summary = `**${totals.commits} commits · ${totals.prsMerged} PRs merged · ${totals.prsOpened} PRs opened · ${totals.issues} issues opened · ${contributors.length} contributors**`;

  const rows = contributors.map((contributor, index) => {
    const login = escapeHtml(contributor.login);
    const profileUrl = `https://github.com/${encodeURIComponent(contributor.login)}`;
    const avatar = `<img src="https://github.com/${encodeURIComponent(contributor.login)}.png?size=32" width="24" height="24" align="absmiddle">`;
    const contributorCell = `<a href="${profileUrl}">${avatar}</a> <a href="${profileUrl}">${login}</a>`;
    const commitsCell = renderCommitsCell(
      organization,
      contributor.login,
      contributor.commits,
      contributor.repositories ?? [],
    );
    return `| ${index + 1} | ${contributorCell} | ${commitsCell} | ${contributor.prsMerged} / ${contributor.prsOpened} | ${contributor.issues} |`;
  });

  return [
    heading,
    "",
    summary,
    "",
    "| Rank | Contributor | Commits | PRs | Issues |",
    "|---:|---|---:|---:|---:|",
    ...rows,
    "",
  ].join("\n");
}

export function serializeTypstData(data) {
  const sections = data.leaderboards
    .map((section) => {
      const contributors = section.contributors
        .map(
          (contributor) => `      (
        login: ${typstString(contributor.login)},
        commits: ${contributor.commits},
        prs_merged: ${contributor.prsMerged},
        prs_opened: ${contributor.prsOpened},
        issues: ${contributor.issues},
      ),`,
        )
        .join("\n");
      const totals = sumContributors(section.contributors);

      return `    ${section.id}: (
      id: ${typstString(section.id)},
      title: ${typstString(section.title)},
      description: ${typstString(section.description)},
      repository: ${section.repository ? typstString(section.repository) : "none"},
      months: ${section.months ?? data.months},
      from: ${typstString(section.from ?? data.from)},
      to: ${typstString(section.to ?? data.to)},
      totals: (
        commits: ${totals.commits},
        prs_merged: ${totals.prsMerged},
        prs_opened: ${totals.prsOpened},
        issues: ${totals.issues},
      ),
      contributors: (
${contributors}
      ),
    ),`;
    })
    .join("\n");

  return `// Generated by .github/scripts/generate_leaderboard.mjs. Do not edit.
#let leaderboard-data = (
  organization: ${typstString(data.organization)},
  months: ${data.months},
  from: ${typstString(data.from)},
  to: ${typstString(data.to)},
  generated_at: ${typstString(data.generatedAt)},
  leaderboards: (
${sections}
  ),
)
`;
}

function gqlStringLiteral(value) {
  return JSON.stringify(String(value));
}

function createGitHubClient(token) {
  const headers = {
    Accept: "application/vnd.github+json",
    Authorization: `Bearer ${token}`,
    "X-GitHub-Api-Version": "2022-11-28",
  };

  async function get(url) {
    const response = await fetch(url, { headers });
    if (!response.ok) {
      const body = await response.text().catch(() => "");
      throw new Error(
        `GET ${url} failed ${response.status} ${response.statusText}\n${body}`,
      );
    }
    return response.json();
  }

  async function getContributorStats(url, attempts = 7) {
    let delay = 2_000;
    for (let attempt = 0; attempt < attempts; attempt += 1) {
      const response = await fetch(url, { headers });
      if (response.status === 202) {
        await sleep(delay);
        delay = Math.min(delay * 2, 60_000);
        continue;
      }
      if (!response.ok) {
        const body = await response.text().catch(() => "");
        throw new Error(
          `GET ${url} failed ${response.status} ${response.statusText}\n${body}`,
        );
      }
      return response.json();
    }
    return null;
  }

  async function graphql(query) {
    const response = await fetch(GQL, {
      method: "POST",
      headers: { ...headers, "Content-Type": "application/json" },
      body: JSON.stringify({ query }),
    });
    const json = await response.json().catch(() => ({}));
    if (!response.ok || json.errors) {
      throw new Error(`GraphQL failed: ${JSON.stringify(json.errors ?? json)}`);
    }
    return json.data;
  }

  return { get, getContributorStats, graphql };
}

async function mapLimit(items, limit, callback) {
  let currentIndex = 0;
  const output = new Array(items.length);
  const workers = Array.from(
    { length: Math.min(limit, items.length) },
    async () => {
      while (currentIndex < items.length) {
        const index = currentIndex;
        currentIndex += 1;
        output[index] = await callback(items[index], index);
      }
    },
  );
  await Promise.all(workers);
  return output;
}

async function listOrganizationRepositories(client, organization) {
  const repositories = [];
  let page = 1;
  while (true) {
    const url = `${API}/orgs/${encodeURIComponent(organization)}/repos?per_page=100&page=${page}&sort=pushed&direction=desc&type=all`;
    const batch = await client.get(url);
    if (!Array.isArray(batch) || batch.length === 0) break;

    for (const repository of batch) {
      if (!repository.archived) repositories.push(repository.name);
    }
    if (batch.length < 100) break;
    page += 1;
  }
  return repositories;
}

function commitsInWindow(weeks, sinceEpochSeconds) {
  return (weeks ?? []).reduce(
    (total, week) =>
      typeof week?.w === "number" && week.w >= sinceEpochSeconds
        ? total + (week.c ?? 0)
        : total,
    0,
  );
}

async function fetchRepositoryCommits(
  client,
  organization,
  repository,
  sinceEpochSecondsByWindow,
  excludedAccounts,
) {
  const url = `${API}/repos/${encodeURIComponent(organization)}/${encodeURIComponent(repository)}/stats/contributors`;
  const stats = await client.getContributorStats(url);
  const commitsByWindow = Object.fromEntries(
    Object.keys(sinceEpochSecondsByWindow).map((window) => [window, new Map()]),
  );

  for (const entry of stats ?? []) {
    const login = entry?.author?.login;
    if (!login || shouldExcludeLogin(login, excludedAccounts)) continue;
    for (const [window, sinceEpochSeconds] of Object.entries(
      sinceEpochSecondsByWindow,
    )) {
      const count = commitsInWindow(entry.weeks, sinceEpochSeconds);
      if (count > 0) commitsByWindow[window].set(login, count);
    }
  }
  return commitsByWindow;
}

function addCommits(target, source) {
  for (const [login, commits] of source.entries()) {
    target.set(login, (target.get(login) ?? 0) + commits);
  }
}

async function fetchContributionMetrics(
  client,
  organization,
  login,
  from,
  to,
  repository,
) {
  const scope = repository
    ? `repo:${organization}/${repository}`
    : `org:${organization}`;
  const issues = `${scope} is:issue author:${login} created:${from}..${to}`;
  const prsOpened = `${scope} is:pr author:${login} created:${from}..${to}`;
  const prsMerged = `${scope} is:pr author:${login} merged:${from}..${to}`;
  const query = `
    query {
      issues: search(query: ${gqlStringLiteral(issues)}, type: ISSUE, first: 1) { issueCount }
      prsOpened: search(query: ${gqlStringLiteral(prsOpened)}, type: ISSUE, first: 1) { issueCount }
      prsMerged: search(query: ${gqlStringLiteral(prsMerged)}, type: ISSUE, first: 1) { issueCount }
    }
  `;
  const result = await client.graphql(query);
  return {
    issues: result.issues.issueCount ?? 0,
    prsOpened: result.prsOpened.issueCount ?? 0,
    prsMerged: result.prsMerged.issueCount ?? 0,
  };
}

async function enrichContributors(
  client,
  config,
  section,
  commitMap,
  from,
  to,
) {
  const metrics = new Map();
  for (const login of commitMap.keys()) {
    const contributionMetrics = await fetchContributionMetrics(
      client,
      config.organization,
      login,
      from,
      to,
      section.repository,
    ).catch((error) => {
      console.warn(`Metrics failed for ${section.id}/${login}: ${error.message}`);
      return { issues: 0, prsOpened: 0, prsMerged: 0 };
    });
    metrics.set(login, contributionMetrics);
    await sleep(120);
  }
  return rankContributors(commitMap, metrics);
}

export async function collectLeaderboardData(config, token, now = new Date()) {
  validateConfig(config);
  const client = createGitHubClient(token);
  const fromDate = monthsBackDate(now, config.months);
  const monthlyFromDate = monthsBackDate(now, config.monthlyOverall.months);
  const from = isoDate(fromDate);
  const monthlyFrom = isoDate(monthlyFromDate);
  const to = isoDate(now);
  const sinceEpochSeconds = Math.floor(fromDate.getTime() / 1_000);
  const monthlySinceEpochSeconds = Math.floor(
    monthlyFromDate.getTime() / 1_000,
  );

  console.log(`Organization: ${config.organization}`);
  console.log(`Window: ${from}..${to}`);

  const repositoryNames = await listOrganizationRepositories(
    client,
    config.organization,
  );
  const repositoryCommits = new Map();
  const monthlyRepositoryCommits = new Map();
  await mapLimit(repositoryNames, 4, async (repository) => {
    const commitsByWindow = await fetchRepositoryCommits(
      client,
      config.organization,
      repository,
      {
        primary: sinceEpochSeconds,
        monthly: monthlySinceEpochSeconds,
      },
      config.excludedAccounts,
    ).catch((error) => {
      console.warn(`Stats failed for ${repository}: ${error.message}`);
      return { primary: new Map(), monthly: new Map() };
    });
    repositoryCommits.set(repository, commitsByWindow.primary);
    monthlyRepositoryCommits.set(repository, commitsByWindow.monthly);
  });

  const overallCommits = new Map();
  for (const commits of repositoryCommits.values()) addCommits(overallCommits, commits);
  const monthlyOverallCommits = new Map();
  for (const commits of monthlyRepositoryCommits.values()) {
    addCommits(monthlyOverallCommits, commits);
  }

  const leaderboards = [];
  for (const section of config.leaderboards) {
    const commitMap = section.repository
      ? (repositoryCommits.get(section.repository) ?? new Map())
      : overallCommits;
    const contributors = await enrichContributors(
      client,
      config,
      section,
      commitMap,
      from,
      to,
    );
    leaderboards.push({
      ...section,
      months: config.months,
      from,
      to,
      contributors,
    });
  }

  const monthlyContributors = await enrichContributors(
    client,
    config,
    config.monthlyOverall,
    monthlyOverallCommits,
    monthlyFrom,
    to,
  );
  const monthlyBreakdowns = buildRepositoryBreakdowns(monthlyRepositoryCommits);
  const monthlyContributorsWithRepos = monthlyContributors.map(
    (contributor) => ({
      ...contributor,
      repositories: monthlyBreakdowns.get(contributor.login) ?? [],
    }),
  );
  leaderboards.push({
    ...config.monthlyOverall,
    from: monthlyFrom,
    to,
    contributors: monthlyContributorsWithRepos,
  });

  return {
    organization: config.organization,
    months: config.months,
    from,
    to,
    generatedAt: now.toISOString(),
    leaderboards,
  };
}

export function monthlySectionFromData(data) {
  const section = data.leaderboards.find(
    (entry) => entry.id === leaderboardConfig.monthlyOverall.id,
  );
  if (!section) {
    throw new Error(
      `Missing monthly leaderboard section: ${leaderboardConfig.monthlyOverall.id}`,
    );
  }
  return section;
}

export async function main() {
  const token = process.env.GH_TOKEN ?? process.env.GITHUB_TOKEN;
  if (!token) throw new Error("Missing GH_TOKEN or GITHUB_TOKEN");

  const data = await collectLeaderboardData(leaderboardConfig, token);
  const output = process.env.LEADERBOARD_DATA_OUTPUT ?? DEFAULT_OUTPUT;
  await fs.mkdir(path.dirname(output), { recursive: true });
  await fs.writeFile(output, serializeTypstData(data), "utf8");
  console.log(`Wrote ${output}`);

  const markdownOutput =
    process.env.LEADERBOARD_MARKDOWN_OUTPUT ?? DEFAULT_MARKDOWN_OUTPUT;
  const monthlyMarkdown = serializeMonthlyMarkdown(
    data.organization,
    monthlySectionFromData(data),
  );
  await fs.mkdir(path.dirname(path.resolve(markdownOutput)), {
    recursive: true,
  });
  await fs.writeFile(markdownOutput, monthlyMarkdown, "utf8");
  console.log(`Wrote ${markdownOutput}`);
}

const scriptPath = process.argv[1] ? pathToFileURL(process.argv[1]).href : "";
if (import.meta.url === scriptPath) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
