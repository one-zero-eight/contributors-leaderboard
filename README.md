# Contributors leaderboard

The repository publishes website-styled SVG leaderboards for contributions to
the [`one-zero-eight`](https://github.com/one-zero-eight) GitHub organization.
The combined `leaderboard.svg` is accompanied by standalone Overall, Monorepo,
and Website views, a compact Overall summary, a one-month Overall SVG, and a
Markdown monthly table published to the organization profile README.

## Structure

- `leaderboard.config.mjs` defines the organization, six-month and one-month
  rolling periods, account exclusions, and leaderboard sections.
- `.github/scripts/generate_leaderboard.mjs` collects GitHub activity and writes
  `typst/generated-data.typ` plus `leaderboard-overall-month.md`.
- `.github/scripts/update_profile_readme.mjs` replaces the monthly section in
  `one-zero-eight/.github` `profile/README.md`.
- `typst/leaderboard.typ` renders the generated records using the shared website
  colors and Rubik font.

The generated data includes every non-automated account with at least one
commit in the configured period. Contributors are ranked by commits, with their
merged PR, opened PR, and opened issue counts shown alongside the ranking.

## Generate locally

Node.js 20 or newer and Typst 0.15.1 are required. Provide a GitHub token with
access to the repositories that should be included in the organization-wide
view:

```sh
GH_TOKEN=... node .github/scripts/generate_leaderboard.mjs
```

Compile the combined board:

```sh
typst compile typst/leaderboard.typ leaderboard.svg \
  --input view=overview \
  --font-path typst/assets/fonts \
  --ignore-system-fonts
```

Use `overall`, `monorepo`, or `website` as the `view` input for a standalone
six-month board. Use `summary` for the compact Overall statistics card and
`overall_month` for the full one-month Overall board. Run the generator tests
with:

```sh
node --test \
  .github/scripts/generate_leaderboard.test.mjs \
  .github/scripts/theme_svg.test.mjs \
  .github/scripts/update_profile_readme.test.mjs
```

The workflow refreshes all six SVGs and the monthly Markdown every Monday,
commits them here, and publishes the Markdown table into the organization
profile README. Publishing requires an organization `GH_PAT` secret with
Contents read/write access to `one-zero-eight/.github`.
