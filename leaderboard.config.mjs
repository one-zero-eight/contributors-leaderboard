export const leaderboardConfig = {
  organization: "one-zero-eight",
  months: 6,
  monthlyOverall: {
    id: "overall_month",
    title: "Overall contribution",
    description:
      "Summarized contributions across all one-zero-eight repositories.",
    months: 1,
  },
  excludedAccounts: {
    suffixes: ["[bot]"],
    logins: ["ci-bot", "cursoragent", "claude"],
  },
  leaderboards: [
    {
      id: "overall",
      title: "Overall contribution",
      description:
        "Summarized contributions across all one-zero-eight repositories.",
    },
    {
      id: "monorepo",
      title: "Monorepo",
      description: "Contributions to one-zero-eight/monorepo.",
      repository: "monorepo",
    },
    {
      id: "website",
      title: "Website",
      description: "Contributions to one-zero-eight/website.",
      repository: "website",
    },
  ],
};
