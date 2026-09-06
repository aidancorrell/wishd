"""Read wish:d settings, with legacy DATASPINE_* compatibility.

An explicitly set WISHD_* value wins, including an empty value. Existing storage
paths and metric names stay stable so a rename does not orphan operator data.
"""

from __future__ import annotations

import os
from collections.abc import Iterator, Mapping


class Environment(Mapping[str, str]):
    def __getitem__(self, key: str) -> str:
        if key.startswith(("DATASPINE_", "WISHD_")):
            suffix = key.split("_", 1)[1]
            preferred = f"WISHD_{suffix}"
            if preferred in os.environ:
                return os.environ[preferred]
            return os.environ[f"DATASPINE_{suffix}"]
        return os.environ[key]

    def __iter__(self) -> Iterator[str]:
        return iter(os.environ)

    def __len__(self) -> int:
        return len(os.environ)


env = Environment()
