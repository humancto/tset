"""License registry for showcase datasets.

Every showcase dataset MUST register its license here before
``download.py`` is allowed to fetch anything. This keeps attribution
explicit and forces a conscious choice when adding new sources.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class License:
    spdx: str
    name: str
    url: str
    attribution: str


LICENSES: dict[str, License] = {
    "wikitext2": License(
        spdx="CC-BY-SA-3.0",
        name="Creative Commons Attribution-ShareAlike 3.0",
        url="https://creativecommons.org/licenses/by-sa/3.0/",
        attribution=(
            "WikiText-2 dataset by Stephen Merity et al. "
            "Source: Salesforce Research, https://blog.salesforceairesearch.com/the-wikitext-long-term-dependency-language-modeling-dataset/"
        ),
    ),
    "click_source": License(
        spdx="BSD-3-Clause",
        name="BSD 3-Clause License",
        url="https://opensource.org/licenses/BSD-3-Clause",
        attribution=(
            "Click is © Pallets and contributors. "
            "Source: https://github.com/pallets/click"
        ),
    ),
    "synthetic_stream": License(
        spdx="CC0-1.0",
        name="Creative Commons Zero v1.0 Universal",
        url="https://creativecommons.org/publicdomain/zero/1.0/",
        attribution="Generated locally from a deterministic seed; no upstream source.",
    ),
}


def license_for(dataset: str) -> License:
    if dataset not in LICENSES:
        raise KeyError(
            f"dataset {dataset!r} has no registered license; "
            f"add one to examples/datasets/_lib/licenses.py before downloading"
        )
    return LICENSES[dataset]
