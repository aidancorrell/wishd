"""Asking a model a narrow question when the alternative is a table we maintain forever.

Three places in this codebase were keeping a lookup table of other people's
vocabulary: the CUR column spellings AWS ships (`cost.py`), the type names each
engine uses (`checks.py`), and the shape of each adapter's error text
(`dbt_artifacts.py`). Every one of them is incomplete the day a vendor adds a
spelling, and wrong in a way nobody notices -- a default CUR 2.0 export
attributed every row to no cluster for a full release, and the totals still
reconciled perfectly against the AWS console.

TypeSafe's System One models answer a bounded question with a typed answer and a
probability. That is the useful shape here: not "write me a summary", but "which
of these 87 column headers holds the unblended cost", where **the options come
from us**. A model that can only choose among headers we found cannot invent a
header, which is the property that makes this safe to put near a bill.

Four rules, and the first two are the ones that matter:

  **Every judgment is optional.** No API key configured means every function here
  returns `None` and each caller keeps the behaviour it had before. This is not a
  dependency; it is an upgrade to code that already works. `WISHD_TYPESAFE_API_KEY`
  turns it on, and nothing else changes.

  **A judgment never gates detection or delivery.** Same rule the alert paths
  already follow: a 500 from an external service is a lost enrichment, and letting
  it raise would turn that into a lost monitoring sweep. Every call is wrapped,
  every failure is logged and swallowed, and the caller's fallback runs.

  **Confidence is a threshold the caller owns.** A Choice returns its whole
  distribution, and what counts as confident enough depends on what happens next:
  misreading a bill is not misordering a search result. Callers pass their own
  floor and it is recorded next to the answer, so a reader can disagree with it --
  the same demand `heuristics.py` makes of every threshold in this project.

  **Options always include a way out.** Every Choice carries an explicit "none of
  these" option. Asked for a cost column in an export that has none, the model
  should say so, and measured against the real CUR fixtures it does -- removing
  `lineItem/UnblendedCost` makes it answer `__none__` at 0.89 rather than reaching
  for `BlendedCost`. An answer we decline to use is the same outcome we had
  before; a confident wrong one is worse than never asking.

**Not validated against a live deployment.** The measurements cited in this module
come from the captures in `tests/fixtures/` and from error text written to the
shape each engine emits. See `ROADMAP.md` and `docs/integrations.md`.
"""

from __future__ import annotations

import logging
from typing import Any

from .config import env

log = logging.getLogger("dataspine.typesafe")

API_KEY_ENV = "DATASPINE_TYPESAFE_API_KEY"
API_URL_ENV = "DATASPINE_TYPESAFE_API_URL"
MODEL_ENV = "DATASPINE_TYPESAFE_MODEL"

DEFAULT_URL = "https://api.typesafe.ai/v1/systemone"

# Pinned rather than floating. `jev-latest` would silently change the answers a
# stored judgment was made with, and "the model moved" is not a diagnosis anyone
# can reach from a wrong cluster attribution three weeks later.
DEFAULT_MODEL = "jev-1.13.0"

# One request, one wait. These calls sit inside cron jobs with other work queued
# behind them -- a CUR import, a monitor sweep -- so the honest move past this is
# to give up and keep the fallback.
TIMEOUT_S = 30.0

# The option every Choice carries. Underscored so it cannot collide with a real
# candidate: CUR headers, column names and table names are all plausible strings.
NONE_OPTION = "__none__"

# TypeSafe's own ceiling on a Choice.
MAX_OPTIONS = 255


def configured() -> bool:
    """Whether judgments are available at all.

    Callers check this before building state, because assembling a question is
    not free and there is no point paying for one nobody will ask.
    """
    return bool(_key())


def _key() -> str:
    try:
        return (env[API_KEY_ENV] or "").strip()
    except KeyError:
        return ""


def _setting(name: str, default: str) -> str:
    try:
        return (env[name] or "").strip() or default
    except KeyError:
        return default


def ask(state: Any, questions: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Put several questions to one piece of state. Returns answers by id, or `{}`.

    **Questions are batched because they are free to batch and expensive to
    serialise.** They are evaluated in parallel and cannot see one another's
    answers, so asking "which column is the cost" and "which column is the
    resource id" together costs one round trip instead of six, and neither
    answer can contaminate the other.

    Never raises. An empty dict means "no judgment available" and is exactly what
    a caller with no key configured gets, so there is one fallback path rather
    than two.
    """
    key = _key()
    if not key:
        return {}

    try:
        import httpx
    except ImportError:  # pragma: no cover - declared dependency
        log.warning("httpx is not installed; TypeSafe judgments are unavailable")
        return {}

    payload = {
        "state": state,
        "model": _setting(MODEL_ENV, DEFAULT_MODEL),
        "questions": questions,
    }
    try:
        response = httpx.post(
            _setting(API_URL_ENV, DEFAULT_URL),
            json=payload,
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            timeout=TIMEOUT_S,
        )
    except Exception as exc:  # noqa: BLE001 - an enrichment must not lose the sweep
        log.warning("typesafe request failed: %s", exc)
        return {}

    if response.status_code != 200:
        # Logged without the body: a 4xx here carries the question back, and the
        # question can carry table and column names into the log.
        log.warning("typesafe returned HTTP %s", response.status_code)
        return {}

    try:
        answers = response.json().get("answers")
    except Exception as exc:  # noqa: BLE001
        log.warning("typesafe returned unreadable JSON: %s", exc)
        return {}
    return answers if isinstance(answers, dict) else {}


def choose(
    state: Any,
    instructions: str,
    options: dict[str, str],
    *,
    min_confidence: float,
) -> tuple[str | None, float]:
    """One Choice over caller-supplied options. Returns `(option, confidence)`.

    `None` for the option covers all four ways this declines -- no key, a failed
    request, the model choosing `__none__`, or confidence below the caller's floor
    -- because every one of them means the same thing downstream: use the
    fallback. The confidence is returned regardless so a caller can record how
    close it came.
    """
    answers = ask(state, {"choice": _choice(instructions, options)})
    return _read_choice(answers.get("choice"), min_confidence=min_confidence)


def _choice(instructions: str, options: dict[str, str]) -> dict[str, Any]:
    """A Choice question with the escape hatch attached.

    Truncated at `MAX_OPTIONS` rather than refused. A caller handing over more
    candidates than the API accepts has a narrowing problem of its own, and
    failing the whole judgment would lose the answer for the candidates that did
    fit -- which, for a CUR header or a column name, are ordered most-likely-first
    by the code that found them.
    """
    criteria = dict(list(options.items())[: MAX_OPTIONS - 1])
    criteria[NONE_OPTION] = "none of these is the thing being asked about"
    return {"type": "choice", "instructions": instructions, "criteria": criteria}


def _read_choice(answer: Any, *, min_confidence: float) -> tuple[str | None, float]:
    if not isinstance(answer, dict):
        return None, 0.0
    choice = answer.get("choice")
    confidence = answer.get("confidence")
    confidence = float(confidence) if isinstance(confidence, int | float) else 0.0
    if not isinstance(choice, str) or choice == NONE_OPTION:
        return None, confidence
    if confidence < min_confidence:
        log.debug("typesafe declined %r at confidence %.2f", choice, confidence)
        return None, confidence
    return choice, confidence


# ------------------------------------------------------------- failure causes

# What an adapter's error text can mean. Chosen so that the answer changes what
# somebody does about it: a permission problem and a resource problem are both
# "the run failed", and they are different people's afternoons.
#
# `assertion` is separate from the rest because it is the only one where the
# machinery worked perfectly. Everything else is a pipeline that stopped.
FAILURE_CAUSES = {
    "permission": (
        "the engine refused access: a missing grant, an expired credential, a "
        "role without rights on the object"
    ),
    "missing_object": (
        "an object the SQL referenced does not exist -- an upstream table, view "
        "or column that was never built, or was dropped"
    ),
    "schema_mismatch": (
        "the object exists but its shape is wrong: a column that is absent, a "
        "type that will not cast, a column-count mismatch"
    ),
    "resource_exhaustion": (
        "the query or job ran out of a resource: memory, disk, a warehouse or "
        "cluster limit, or a statement timeout it reached under load"
    ),
    "transient_infra": (
        "the platform itself failed in a way unrelated to this SQL: a dropped "
        "connection, a lost node, a 5xx from the service, a cluster that died"
    ),
    "assertion": (
        "the SQL ran correctly and a declared data-quality test on its result "
        "did not hold"
    ),
    "syntax": "the SQL could not be compiled or parsed at all",
}

# Which causes are about the data rather than the machinery. Derived in code from
# the cause rather than asked as its own question, and that is deliberate: asked
# directly, "is this a data problem" scored 11/14 against labels that were
# themselves arguable, because a missing upstream table is honestly either. The
# Choice already carries the distinction, and a mapping here can be argued with
# in a diff. Keep the judgment raw; keep the policy in code.
DATA_CAUSES = frozenset({"assertion", "schema_mismatch"})

# Below this the cause is dropped rather than shown. A wrong label on an alert is
# worse than no label: it sends the reader to the wrong first guess, and they
# trust the next one less. The one misclassification measured came back at 0.61
# while every correct answer sat at 0.86 or above.
CAUSE_MIN_CONFIDENCE = 0.8


def classify_failure(
    error_text: str | None, *, engine: str | None = None, node: str | None = None
) -> dict[str, Any]:
    """What kind of failure this error text describes. `{}` when we cannot say.

    Returns `cause`, `confidence`, `retryable` (a probability, or None) and
    `data_problem` (derived from the cause, not asked).

    The two questions go together in one request because they are independent
    judgments about the same text: the cause is what to put in the alert and what
    to route on, and retryability is a different axis entirely -- a resource
    exhaustion and a dropped connection are both infrastructure and only one of
    them is worth running again unchanged.

    Measured over fourteen adapter errors across Snowflake, Spark, Postgres and
    dbt: thirteen causes correct, and the fourteenth returned at confidence 0.61,
    below the floor, so it was dropped rather than shown wrong. **Those errors
    were written to the shape each engine emits rather than taken from a capture**
    -- `tests/fixtures/` carries dbt assertion failures and no adapter errors --
    so treat the figure as indicative. See `ROADMAP.md`.
    """
    text = (error_text or "").strip()
    if not text or not configured():
        return {}

    state = {
        "engine": engine or "unknown",
        "node": node or "unknown",
        # Truncated: the first lines of an adapter error carry the diagnosis and
        # the rest is a stack trace, which costs tokens to say the same thing.
        "error_text": text[:2000],
    }
    answers = ask(
        state,
        {
            "cause": _choice(
                "What made this pipeline node fail? Judge from `error_text`.",
                FAILURE_CAUSES,
            ),
            "retryable": {
                "type": "noul",
                "instructions": (
                    "Would running this again, unchanged, plausibly succeed? Answer "
                    "yes only when the failure is about the state of the platform at "
                    "that moment rather than about this SQL, this data or this "
                    "configuration."
                ),
            },
        },
    )
    cause, confidence = _read_choice(
        answers.get("cause"), min_confidence=CAUSE_MIN_CONFIDENCE
    )
    if cause is None:
        return {}
    return {
        "cause": cause,
        "confidence": confidence,
        "retryable": _read_noul(answers.get("retryable")),
        "data_problem": cause in DATA_CAUSES,
    }


# ------------------------------------------------------------------- renames

# A rename is only ever added to the sentence, never subtracted from the breach,
# so the cost of being wrong is one misleading clause rather than a missed
# removal. The floor is still high, because "renamed to X" sends someone to go
# and look at X.
RENAME_MIN_CONFIDENCE = 0.8


def rename_of(
    removed: str, added: dict[str, str], *, table: str, columns: dict[str, str]
) -> str | None:
    """Which added column, if any, is `removed` under a new name.

    A schema comparison produces two lists of names and no relationship between
    them, so dropping `customer_id` in the same write that adds `cust_id` reads
    as a removal plus an unrelated addition -- which is a true description and a
    useless one. The fix a reader needs is "point at `cust_id`", and they only
    get there by recognising the two names as one column, which is a judgment
    about what the names *mean*. No amount of string distance settles it:
    `customer_id` and `customer_segment` are closer by every edit metric than
    `customer_id` and `cust_id`, and are not the same column.

    The options are the columns actually added, plus none, so this cannot invent
    a name. Measured over eight schema changes built from the fixture tables:
    all four renames found, including one buried among two unrelated additions,
    and all four non-renames correctly declined -- among them the
    `customer_id`/`customer_segment` trap.
    """
    if not removed or not added or not configured():
        return None

    options = {name: f"newly added, type {type_}" for name, type_ in added.items()}
    state = {
        "table": table,
        # The schema as it now stands, so the other columns are visible: whether
        # `cust_id` is `customer_id` renamed reads differently in a table that
        # still has a `customer_id` beside it.
        "schema_now": columns,
        "removed": removed,
        "added": [{"name": n, "type": t} for n, t in added.items()],
    }
    choice, _ = choose(
        state,
        (
            f"The column `{removed}` disappeared from this table in the same write "
            "that added the columns listed in `added`. Which added column, if any, "
            "is the same data under a new name? Choose none when the removal and "
            "the additions are unrelated -- a column genuinely dropped while other, "
            "different columns happened to be added."
        ),
        options,
        min_confidence=RENAME_MIN_CONFIDENCE,
    )
    return choice


def _read_noul(answer: Any) -> float | None:
    """The probability from a Noul, or None when there was no usable answer.

    None rather than 0.0: a Noul of 0.0 is the model saying "certainly not", and a
    caller that cannot tell that from "nobody asked" will treat a service outage
    as a confident negative.
    """
    if not isinstance(answer, dict):
        return None
    value = answer.get("noul")
    return float(value) if isinstance(value, int | float) else None
