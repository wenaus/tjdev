#!/usr/bin/env python3
"""Report recent PR activity across all ePIC LXR-indexed repositories.

Usage:
    python epic_pr_activity.py [--hours N] [--state open|closed|all]

Defaults to last 24 hours, open PRs. Queries GitHub via `gh` CLI.
"""
import argparse
import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone

# All repos indexed by the ePIC LXR code browser.
# Almost all under eic/ org; exceptions noted.
REPOS = [
    "eic/acts",
    "eic/algorithms",
    "eic/containers",
    "eic/DD4hep",
    "eic/DEMPgen",
    "eic/detector_benchmarks",
    "eic/drich-dev",
    "eic/EDM4eic",
    "eic/edpm",
    "eic/eic-shell",
    "eic/eic-spack",
    "eic/eic.github.io",
    "eic/EICrecon",
    "eic/epic",
    "eic/epic-capybara",
    "eic/epic-data",
    "eic/epic-lfhcal-tbana",
    "eic/epic-prod",
    "eic/estarlight",
    "eic/firebird",
    "eic/firehose",
    "eic/geant4",
    "eic/HEPMC_Merger",
    "eic/image_browser",
    "eic/irt",
    "JeffersonLab/JANA2",
    "eic/job_submission_condor",
    "eic/job_submission_slurm",
    "eic/JPsiDataSet",
    "eic/LQGENEP",
    "eic/npsim",
    "eic/pfRICH",
    "eic/phoenix-eic-event-display",
    "eic/physics_benchmarks",
    "eic/run-cvmfs-osg-eic-shell",
    "eic/simulation_campaign_datasets",
    "eic/simulation_campaign_hepmc3",
    "eic/simulation_campaign_single",
    "eic/snippets",
    "eic/trigger-gitlab-ci",
    "eic/tutorial-analysis",
    "eic/tutorial-developing-benchmarks",
    "eic/tutorial-geometry-development-using-dd4hep",
    "eic/tutorial-jana2",
    "eic/tutorial-reconstruction-algorithms",
    "eic/tutorial-setting-up-environment",
    "eic/tutorial-simulations-using-ddsim-and-geant4",
    "eic/UpsilonGen",
]


def get_prs(repo, since_date, state="open"):
    """Query GitHub for PRs updated since since_date."""
    cmd = [
        "gh", "search", "prs",
        f"--updated=>={since_date}",
        f"--repo={repo}",
        f"--state={state}",
        "--json=title,url,number,author,updatedAt,createdAt",
        "--limit=50",
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            return []
        return json.loads(result.stdout) if result.stdout.strip() else []
    except (subprocess.TimeoutExpired, json.JSONDecodeError):
        return []


def main():
    parser = argparse.ArgumentParser(description="ePIC PR activity report")
    parser.add_argument("--hours", type=int, default=24,
                        help="Look back N hours (default: 24)")
    parser.add_argument("--state", default="open", choices=["open", "closed", "all"],
                        help="PR state filter (default: open)")
    args = parser.parse_args()

    cutoff = datetime.now(timezone.utc) - timedelta(hours=args.hours)
    since_date = cutoff.strftime("%Y-%m-%d")

    print(f"ePIC PR activity — last {args.hours}h (since {since_date}), state={args.state}")
    print(f"Scanning {len(REPOS)} LXR-indexed repos...\n")

    total = 0
    for repo in REPOS:
        prs = get_prs(repo, since_date, args.state)
        if not prs:
            continue

        # Filter to PRs actually updated within the time window
        filtered = []
        for pr in prs:
            updated = pr.get("updatedAt", "")
            if updated:
                try:
                    ts = datetime.fromisoformat(updated.replace("Z", "+00:00"))
                    if ts >= cutoff:
                        filtered.append(pr)
                except ValueError:
                    filtered.append(pr)
            else:
                filtered.append(pr)

        if not filtered:
            continue

        print(f"## {repo}")
        for pr in sorted(filtered, key=lambda p: p.get("updatedAt", ""), reverse=True):
            num = pr["number"]
            title = pr["title"]
            author = pr.get("author", {}).get("login", "?")
            created = pr.get("createdAt", "")[:10]
            updated = pr.get("updatedAt", "")[:10]
            url = pr["url"]

            new_flag = ""
            if created >= since_date:
                new_flag = " [NEW]"

            print(f"  #{num} {title}{new_flag}")
            print(f"       by {author} — updated {updated}  {url}")

        total += len(filtered)
        print()

    if total == 0:
        print("No PR activity found.")
    else:
        print(f"Total: {total} PRs with activity")


if __name__ == "__main__":
    main()
