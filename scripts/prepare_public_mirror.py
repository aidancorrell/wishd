"""Prepare a cleaned, isolated Git mirror without changing or pushing the source.

The replacement file uses git-filter-repo's replace-text format and contains
secrets: store it outside Git with mode 0600. The output must not already exist.
Review the output and scan it before any separate, explicitly approved push.
"""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", help="Local repository to clone")
    parser.add_argument("output", type=Path, help="New local mirror directory")
    parser.add_argument("replacements", type=Path, help="Private git-filter-repo replacement file")
    args = parser.parse_args()
    destination = args.output.resolve()
    replacements = args.replacements.resolve(strict=True)
    source = Path(args.source).resolve(strict=True)
    if destination.exists():
        parser.error("output already exists; choose a new directory")
    if not replacements.is_file():
        parser.error("replacements must be a regular file")

    # --no-local copies objects rather than hard-linking the shared working repo.
    subprocess.run(
        ["git", "clone", "--mirror", "--no-local", str(source), str(destination)], check=True
    )
    subprocess.run(
        [
            "uvx", "--from", "git-filter-repo==2.47.0", "git-filter-repo",
            "--replace-text", str(replacements),
            "--replace-message", str(replacements),
            "--path", "dev/spark/eventlog/", "--invert-paths",
        ],
        cwd=destination, check=True,
    )
    # filter-repo usually removes origin; make absence an explicit output invariant.
    remotes = subprocess.check_output(["git", "remote"], cwd=destination, text=True).splitlines()
    for remote in remotes:
        subprocess.run(["git", "remote", "remove", remote], cwd=destination, check=True)
    print(f"Cleaned local mirror: {destination}. No remotes configured; nothing was pushed.")


if __name__ == "__main__":
    main()
