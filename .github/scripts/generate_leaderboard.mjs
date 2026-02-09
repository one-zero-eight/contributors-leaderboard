import fs from "node:fs/promises";

const ORG = process.env.ORG || "one-zero-eight";
const MONTHS = Number(process.env.MONTHS || "6");
const OVERALL_TOP_N = Number(process.env.OVERALL_TOP_N || "50");
const PER_REPO_TOP_N = Number(process.env.PER_REPO_TOP_N || "10");
const PER_REPO_ENRICH_TOP_REPOS = Number(process.env.PER_REPO_ENRICH_TOP_REPOS || "10");

const TOKEN = process.env.GH_TOKEN || process.env.GITHUB_TOKEN;
if (!TOKEN) {
  console.error("Missing GH_TOKEN or GITHUB_TOKEN");
  process.exit(1);
}

const API = "https://api.github.com";
const GQL = "https://api.github.com/graphql";
const HEADERS = {
  "Accept": "application/vnd.github+json",
  "Authorization": `Bearer ${TOKEN}`,
  "X-GitHub-Api-Version": "2022-11-28",
};

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function isoDate(d) {
  return d.toISOString().slice(0, 10);
}

function monthsBackDate(months) {
  const d = new Date();
  // Approx six months window in days to avoid month length edge cases
  const days = Math.round(months * 30.4375); // average days per month
  d.setUTCDate(d.getUTCDate() - days);
  d.setUTCHours(0, 0, 0, 0);
  return d;
}

function escapeXml(s) {
  return String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&apos;");
}

function gqlStringLiteral(s) {
  // GraphQL string literal with escapes
  return `"${String(s).replaceAll("\\", "\\\\").replaceAll('"', '\\"')}"`;
}

async function ghGet(url) {
  const res = await fetch(url, { headers: HEADERS });
  if (res.status === 401 || res.status === 403) {
    const body = await res.text().catch(() => "");
    throw new Error(`GET ${url} failed ${res.status} ${res.statusText}\n${body}`);
  }
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`GET ${url} failed ${res.status} ${res.statusText}\n${body}`);
  }
  return res.json();
}

async function ghGetWith202Retry(url, attempts = 7) {
  let delay = 2000;
  for (let i = 0; i < attempts; i++) {
    const res = await fetch(url, { headers: HEADERS });
    if (res.status === 202) {
      await sleep(delay);
      delay = Math.min(delay * 2, 60000);
      continue;
    }
    if (!res.ok) {
      const body = await res.text().catch(() => "");
      throw new Error(`GET ${url} failed ${res.status} ${res.statusText}\n${body}`);
    }
    return res.json();
  }
  // If still 202, treat as unavailable
  return null;
}

async function gql(query) {
  const res = await fetch(GQL, {
    method: "POST",
    headers: {
      ...HEADERS,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ query }),
  });

  const json = await res.json().catch(() => ({}));
  if (!res.ok || json.errors) {
    throw new Error(`GraphQL failed: ${JSON.stringify(json.errors || json)}`);
  }
  return json.data;
}

async function listOrgRepos(org) {
  const repos = [];
  let page = 1;
  while (true) {
    const url = `${API}/orgs/${encodeURIComponent(org)}/repos?per_page=100&page=${page}&sort=pushed&direction=desc&type=all`;
    const batch = await ghGet(url);
    if (!Array.isArray(batch) || batch.length === 0) break;
    for (const r of batch) {
      if (r.archived) continue;
      repos.push({
        name: r.name,
        full_name: r.full_name,
        private: r.private,
        fork: r.fork,
      });
    }
    if (batch.length < 100) break;
    page += 1;
  }
  return repos;
}

function sumCommitsInWindowFromStats(stats, sinceEpochSec) {
  // stats: /stats/contributors element
  // weeks[]: { w: epochSec, c: commits }
  if (!stats?.weeks) return 0;
  let sum = 0;
  for (const w of stats.weeks) {
    if (typeof w?.w !== "number") continue;
    if (w.w >= sinceEpochSec) sum += (w.c || 0);
  }
  return sum;
}

function mapLimit(items, limit, fn) {
  let idx = 0;
  const out = new Array(items.length);
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (idx < items.length) {
      const my = idx++;
      out[my] = await fn(items[my], my);
    }
  });
  return Promise.all(workers).then(() => out);
}

async function fetchRepoCommitStats(org, repo, sinceEpochSec) {
  const url = `${API}/repos/${encodeURIComponent(org)}/${encodeURIComponent(repo)}/stats/contributors`;
  const data = await ghGetWith202Retry(url);
  if (!data) return { perLogin: new Map(), total: 0 };

  const perLogin = new Map();
  let total = 0;

  for (const entry of data) {
    const login = entry?.author?.login;
    if (!login) continue;
    const c = sumCommitsInWindowFromStats(entry, sinceEpochSec);
    if (c <= 0) continue;
    perLogin.set(login, c);
    total += c;
  }
  return { perLogin, total };
}

async function fetchSearchCountsOrg(org, login, fromISO, toISO) {
  const qIssues = `org:${org} is:issue author:${login} created:${fromISO}..${toISO}`;
  const qPrsOpened = `org:${org} is:pr author:${login} created:${fromISO}..${toISO}`;
  const qPrsMerged = `org:${org} is:pr author:${login} merged:${fromISO}..${toISO}`;

  const query = `
    query {
      issues: search(query: ${gqlStringLiteral(qIssues)}, type: ISSUE, first: 1) { issueCount }
      prsOpened: search(query: ${gqlStringLiteral(qPrsOpened)}, type: ISSUE, first: 1) { issueCount }
      prsMerged: search(query: ${gqlStringLiteral(qPrsMerged)}, type: ISSUE, first: 1) { issueCount }
    }
  `;
  const data = await gql(query);
  return {
    issues: data.issues.issueCount || 0,
    prsOpened: data.prsOpened.issueCount || 0,
    prsMerged: data.prsMerged.issueCount || 0,
  };
}

async function fetchSearchCountsRepo(owner, repo, login, fromISO, toISO) {
  const qIssues = `repo:${owner}/${repo} is:issue author:${login} created:${fromISO}..${toISO}`;
  const qPrsOpened = `repo:${owner}/${repo} is:pr author:${login} created:${fromISO}..${toISO}`;
  const qPrsMerged = `repo:${owner}/${repo} is:pr author:${login} merged:${fromISO}..${toISO}`;

  const query = `
    query {
      issues: search(query: ${gqlStringLiteral(qIssues)}, type: ISSUE, first: 1) { issueCount }
      prsOpened: search(query: ${gqlStringLiteral(qPrsOpened)}, type: ISSUE, first: 1) { issueCount }
      prsMerged: search(query: ${gqlStringLiteral(qPrsMerged)}, type: ISSUE, first: 1) { issueCount }
    }
  `;
  const data = await gql(query);
  return {
    issues: data.issues.issueCount || 0,
    prsOpened: data.prsOpened.issueCount || 0,
    prsMerged: data.prsMerged.issueCount || 0,
  };
}

function toSortedArrayFromMap(map) {
  return Array.from(map.entries()).sort((a, b) => b[1] - a[1]);
}

function formatLine(cols) {
  // cols: [ {text, width, align} ]
  return cols
    .map(({ text, width, align }) => {
      const s = String(text ?? "");
      if (width == null) return s;
      if (align === "right") return s.padStart(width, " ");
      return s.padEnd(width, " ");
    })
    .join(" ");
}

function buildSvg(lines, opts = {}) {
  const fontSize = opts.fontSize ?? 14;
  const lineHeight = opts.lineHeight ?? 18;
  const margin = opts.margin ?? 20;
  const width = opts.width ?? 1100;

  const height = margin * 2 + lines.length * lineHeight + 10;

  const y0 = margin + fontSize;

  const textEls = lines.map((line, i) => {
    const y = y0 + i * lineHeight;
    return `<text x="${margin}" y="${y}">${escapeXml(line)}</text>`;
  }).join("\n");

  return `<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}">
  <rect x="0" y="0" width="${width}" height="${height}" fill="white"/>
  <style>
    text {
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
      font-size: ${fontSize}px;
      fill: #111;
      white-space: pre;
    }
  </style>
${textEls}
</svg>`;
}

async function main() {
  const now = new Date();
  const from = monthsBackDate(MONTHS);
  const fromISO = isoDate(from);
  const toISO = isoDate(now);
  const sinceEpochSec = Math.floor(from.getTime() / 1000);

  console.log(`ORG ${ORG}`);
  console.log(`Window ${fromISO}..${toISO}`);

  const repos = await listOrgRepos(ORG);

  console.log(`Repos ${repos.length}`);

  const overallCommits = new Map();          // login -> commits
  const repoCommits = new Map();             // repo -> Map(login->commits)
  const repoTotals = new Map();              // repo -> total commits

  // Limit concurrency to avoid hammering stats endpoints
  await mapLimit(repos, 4, async (r) => {
    const { perLogin, total } = await fetchRepoCommitStats(ORG, r.name, sinceEpochSec).catch((e) => {
      console.warn(`Stats failed for ${r.full_name}: ${e.message}`);
      return { perLogin: new Map(), total: 0 };
    });

    repoCommits.set(r.name, perLogin);
    repoTotals.set(r.name, total);

    for (const [login, c] of perLogin.entries()) {
      overallCommits.set(login, (overallCommits.get(login) || 0) + c);
    }
  });

  const overallTop = toSortedArrayFromMap(overallCommits).slice(0, OVERALL_TOP_N);

  // Enrich overall top users with org-wide issues/PR counts
  const overallMetrics = new Map(); // login -> {issues, prsOpened, prsMerged}
  for (const [login] of overallTop) {
    const m = await fetchSearchCountsOrg(ORG, login, fromISO, toISO).catch((e) => {
      console.warn(`Search failed for ${login}: ${e.message}`);
      return { issues: 0, prsOpened: 0, prsMerged: 0 };
    });
    overallMetrics.set(login, m);
    await sleep(150); // gentle throttle
  }

  // Per-repo: pick top repos by commit activity to enrich with PR/issue counts
  const topRepos = Array.from(repoTotals.entries())
    .sort((a, b) => b[1] - a[1])
    .filter(([, total]) => total > 0)
    .slice(0, PER_REPO_ENRICH_TOP_REPOS)
    .map(([name]) => name);

  const perRepoEnriched = new Map(); // repo -> Map(login -> metrics)
  for (const repo of topRepos) {
    const perLogin = repoCommits.get(repo) || new Map();
    const topLogins = toSortedArrayFromMap(perLogin).slice(0, PER_REPO_TOP_N).map(([login]) => login);

    const mMap = new Map();
    for (const login of topLogins) {
      const m = await fetchSearchCountsRepo(ORG, repo, login, fromISO, toISO).catch((e) => {
        console.warn(`Repo search failed ${repo} ${login}: ${e.message}`);
        return { issues: 0, prsOpened: 0, prsMerged: 0 };
      });
      mMap.set(login, m);
      await sleep(120);
    }
    perRepoEnriched.set(repo, mMap);
  }

  // Build SVG text lines
  const lines = [];
  lines.push(`${ORG} leaderboard last ${MONTHS} months ${fromISO}..${toISO}`);
  lines.push("");
  lines.push("Overall top contributors");
  lines.push(formatLine([
    { text: "rk", width: 2, align: "right" },
    { text: "login", width: 22 },
    { text: "commits", width: 7, align: "right" },
    { text: "pr_m", width: 5, align: "right" },
    { text: "pr_o", width: 5, align: "right" },
    { text: "issues", width: 6, align: "right" },
  ]));
  lines.push("-".repeat(60));

  overallTop.forEach(([login, commits], idx) => {
    const m = overallMetrics.get(login) || { issues: 0, prsOpened: 0, prsMerged: 0 };
    lines.push(formatLine([
      { text: String(idx + 1), width: 2, align: "right" },
      { text: login, width: 22 },
      { text: String(commits), width: 7, align: "right" },
      { text: String(m.prsMerged), width: 5, align: "right" },
      { text: String(m.prsOpened), width: 5, align: "right" },
      { text: String(m.issues), width: 6, align: "right" },
    ]));
  });

  lines.push("");
  lines.push("Per repository leaderboards");
  lines.push("");

  const reposByActivity = Array.from(repoTotals.entries())
    .sort((a, b) => b[1] - a[1])
    .filter(([, total]) => total > 0)
    .map(([name]) => name);

  for (const repo of reposByActivity) {
    const perLogin = repoCommits.get(repo) || new Map();
    const top = toSortedArrayFromMap(perLogin).slice(0, PER_REPO_TOP_N);

    lines.push(`${ORG}/${repo}  commits ${repoTotals.get(repo) || 0}`);
    const enriched = perRepoEnriched.get(repo);

    if (enriched) {
      lines.push(formatLine([
        { text: "rk", width: 2, align: "right" },
        { text: "login", width: 22 },
        { text: "commits", width: 7, align: "right" },
        { text: "pr_m", width: 5, align: "right" },
        { text: "pr_o", width: 5, align: "right" },
        { text: "issues", width: 6, align: "right" },
      ]));
      lines.push("-".repeat(60));
      top.forEach(([login, commits], idx) => {
        const m = enriched.get(login) || { issues: 0, prsOpened: 0, prsMerged: 0 };
        lines.push(formatLine([
          { text: String(idx + 1), width: 2, align: "right" },
          { text: login, width: 22 },
          { text: String(commits), width: 7, align: "right" },
          { text: String(m.prsMerged), width: 5, align: "right" },
          { text: String(m.prsOpened), width: 5, align: "right" },
          { text: String(m.issues), width: 6, align: "right" },
        ]));
      });
    } else {
      lines.push(formatLine([
        { text: "rk", width: 2, align: "right" },
        { text: "login", width: 22 },
        { text: "commits", width: 7, align: "right" },
      ]));
      lines.push("-".repeat(38));
      top.forEach(([login, commits], idx) => {
        lines.push(formatLine([
          { text: String(idx + 1), width: 2, align: "right" },
          { text: login, width: 22 },
          { text: String(commits), width: 7, align: "right" },
        ]));
      });
    }

    lines.push("");
  }

  const svg = buildSvg(lines, { width: 1100, fontSize: 14, lineHeight: 18, margin: 20 });
  await fs.writeFile("leaderboard.svg", svg, "utf8");
  console.log("Wrote leaderboard.svg");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
