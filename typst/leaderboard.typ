#import "theme.typ": *
#import "generated-data.typ": leaderboard-data

#let view = sys.inputs.at("view", default: "overview")
#let valid-views = ("overview", "summary", "overall", "overall_month", "monorepo", "website")
#if not valid-views.contains(view) {
  panic("Unknown leaderboard view: " + view)
}

#let is-overview = view == "overview"
#let is-summary = view == "summary"
#let page-width = if is-overview { 1180pt } else { 820pt }

#set page(
  width: page-width,
  height: auto,
  margin: 0pt,
  fill: base-100,
)
#set text(
  font: "Rubik",
  size: 11pt,
  fill: base-content,
  lang: "en",
)
#set par(leading: 0.55em)

#let metric-label(value, label) = block[
  #text(size: 19pt, weight: 650, fill: primary-content)[#value]
  #linebreak()
  #text(size: 8.5pt, weight: 500, fill: metric-content)[
    #label
  ]
]

#let period-label(months) = if months == 1 {
  [Last month]
} else {
  [Last #months months]
}

#let rank-badge(rank) = {
  box(
    width: 27pt,
    height: 27pt,
    fill: rank-fill,
    stroke: rank-stroke,
    radius: 50%,
    inset: 0pt,
    align(center + horizon, text(weight: 650, size: 9pt)[#rank]),
  )
}

#let contributor-row(contributor, rank) = block(
  width: 100%,
  fill: base-150,
  radius: field-radius,
  inset: (x: 10pt, y: 7pt),
)[
  #grid(
    columns: (32pt, 1fr, 65pt, 65pt, 65pt, 56pt),
    column-gutter: 6pt,
    align: (center + horizon, left + horizon, right + horizon, right + horizon, right + horizon, right + horizon),
    rank-badge(rank),
    text(weight: 450)[#contributor.login],
    text(weight: 600)[#contributor.commits],
    [#contributor.prs_merged],
    [#contributor.prs_opened],
    [#contributor.issues],
  )
]

#let table-heading() = block(
  width: 100%,
  inset: (x: 10pt, bottom: 7pt),
)[
  #grid(
    columns: (32pt, 1fr, 65pt, 65pt, 65pt, 56pt),
    column-gutter: 6pt,
    align: (center, left, right, right, right, right),
    text(size: 8pt, weight: 600, fill: faint-content)[RANK],
    text(size: 8pt, weight: 600, fill: faint-content)[CONTRIBUTOR],
    text(size: 8pt, weight: 600, fill: faint-content)[COMMITS],
    text(size: 8pt, weight: 600, fill: faint-content)[PR MERGED],
    text(size: 8pt, weight: 600, fill: faint-content)[PR OPENED],
    text(size: 8pt, weight: 600, fill: faint-content)[ISSUES],
  )
]

#let section-card(section, show-contributors: true) = block(
  width: 100%,
  fill: base-200,
  stroke: base-300,
  radius: card-radius,
  inset: 18pt,
)[
  #grid(
    columns: (1fr, auto),
    column-gutter: 20pt,
    align: (left + top, right + top),
    [
      #grid(
        columns: (auto, auto),
        column-gutter: 10pt,
        align: bottom,
        text(size: 20pt, weight: 650)[#section.title],
        text(size: 9.5pt, fill: muted-content)[#period-label(section.months)],
      )
    ],
    [
      #block(
        fill: badge-fill,
        stroke: badge-stroke,
        radius: 99pt,
        inset: (x: 10pt, y: 5pt),
      )[
        #text(size: 8pt, weight: 600, fill: primary)[
          #section.contributors.len() CONTRIBUTORS
        ]
      ]
    ],
  )
  #v(16pt)
  #grid(
    columns: (1fr, 1fr, 1fr, 1fr),
    column-gutter: 8pt,
    metric-label(section.totals.commits, "COMMITS"),
    metric-label(section.totals.prs_merged, "PRS MERGED"),
    metric-label(section.totals.prs_opened, "PRS OPENED"),
    metric-label(section.totals.issues, "ISSUES OPENED"),
  )
  #if show-contributors {
    v(17pt)
    table-heading()
    for (index, contributor) in section.contributors.enumerate() {
      contributor-row(contributor, index + 1)
      v(2pt)
    }
    if section.contributors.len() == 0 {
      block(width: 100%, fill: base-150, radius: field-radius, inset: 18pt)[
        #align(center)[#text(fill: muted-content)[No contributions in this period.]]
      ]
    }
  }
]

#let overall = leaderboard-data.leaderboards.overall
#let monorepo = leaderboard-data.leaderboards.monorepo
#let website = leaderboard-data.leaderboards.website

#block(width: 100%, inset: 24pt)[
  #if is-overview {
    section-card(overall)
    v(18pt)
    grid(
      columns: (1fr, 1fr),
      column-gutter: 18pt,
      align: top,
      section-card(monorepo),
      section-card(website),
    )
  } else if is-summary {
    section-card(overall, show-contributors: false)
  } else {
    section-card(leaderboard-data.leaderboards.at(view))
  }
]
