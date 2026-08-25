export const leaderboardConfig = {
  organization: "one-zero-eight",
  months: 6,
  excludedAccounts: {
    suffixes: ["[bot]"],
    logins: ["ci-bot", "cursoragent"],
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
