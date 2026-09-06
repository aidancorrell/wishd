"""Handing a failure to a coding agent instead of describing it to a person.

Every alert dataspine sends already answers "what broke". The link to Snowsight
answers "show me". Neither answers "fix it", and the gap between them is where
the on-call engineer spends their evening: reading the failing SQL, finding the
model that wrote it, working out which upstream change is responsible.

That reading is work an agent can start unattended, and dataspine is unusually
well placed to brief one. It already holds the compiled SQL the warehouse ran,
the adapter's error text, the table the assertion was about, and — the part no
agent can get from the repository alone — which *upstream* runs failed in the
same window. An agent handed all four starts from a diagnosis. An agent handed
a Slack message starts from a sentence.

**Two of the three targets go through our own `/handoff` endpoint; one does not.**
Which is which was measured rather than assumed — see `docs/agent-handoff.md`:

  **Claude Code, locally** has a real deep link, `claude-cli://open`, registered
  by a handler that ships with the CLI. It takes `cwd`, `repo` and `q`, stages
  the prompt for review before running anything, and **Slack navigates it
  straight from a button** — verified by clicking one. It is the only target
  whose URL carries the whole briefing, so it is the only one that needs no page:
  the click goes from Slack to a terminal with nothing in between.

  **Claude Code in the cloud** has no such address. `claude.ai/code/<id>`
  *attaches* to a session that already exists; creating one with a prompt is an
  action, not a URL.

  **Codex** has a deep link, `codex://threads/new`, but it cannot carry a prompt
  — `codex app` takes a workspace path and nothing else. Opening a blank thread
  in the right directory is not sending anybody the error, so the prompt-carrying
  path is `codex exec`, a command rather than a link.

A Slack button holds a URL and nothing else. Since two of the three are not URLs,
pointing every button at its agent would have shipped one target and faked two,
and routing *every* button through a page would have charged the one target that
works for the sins of the two that do not. So each goes the shortest way it can.

**What the two routes trade.** The direct link freezes the briefing at alert time
and offers no explanation on a machine with no handler registered. The page is
built at click time — so an alert opened in the morning still carries the current
lineage — has no length limit, and can say why nothing happened. The direct link
is faster; the page is more honest. Ordering them this way is a judgement about
which failure is likelier, not a claim that one dominates.

Two consequences worth naming, because they stop being problems rather than
getting solved. The briefing only competes with Slack's URL cap on the direct
route, and falls back to the page when it loses. And "whichever agent is
available" — which no server can know, since the reader's laptop is not the
server — is answered on the reader's own machine, by the page, where the answer
actually lives.

## Configured, never guessed

`links.py` refuses to invent a URL, on the grounds that a link that 404s costs
someone a click and their trust in every other link on the page. A button that
opens an agent in the wrong repository is the same mistake with a longer feedback
loop, so the repository and checkout path are configuration. Nothing is offered
that has not been named.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlencode

from .config import env

log = logging.getLogger("dataspine.agents")

# The repository the agent should work in, as `owner/repo`. Claude Code's deep
# link validates this shape itself and rejects anything else, so we hold to it
# for every target rather than letting one of them accept a looser value.
REPO_ENV = "DATASPINE_AGENT_REPO"

# An absolute path to a local checkout. Distinct from the repo on purpose: the
# deep link takes both, and `cwd` is what decides whether the agent opens in the
# reader's working copy or has to clone one.
CWD_ENV = "DATASPINE_AGENT_CWD"

# Which targets to offer, comma-separated. Unset means the feature is off, which
# is the right default for something that puts a button labelled "fix this" in
# front of a whole channel.
TARGETS_ENV = "DATASPINE_AGENT_TARGETS"

# Signing secret for handoff keys. Without it no button is rendered at all: the
# endpoint would otherwise take an unauthenticated `(event, dedup_key)` from
# anyone and read back the failing SQL, which is exactly the shape of an oracle
# we should not be running.
SECRET_ENV = "DATASPINE_AGENT_SECRET"

CLAUDE_CLI = "claude-cli"
CLAUDE_CLOUD = "claude-cloud"
CODEX = "codex"
KNOWN_TARGETS = (CLAUDE_CLI, CLAUDE_CLOUD, CODEX)

TARGET_LABELS = {
    CLAUDE_CLI: "Claude Code",
    CLAUDE_CLOUD: "Claude Cloud",
    CODEX: "Codex",
}

_REPO_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")

# Claude Code's own threshold for warning that a link-supplied prompt is long
# enough to need scrolling. Ours is about a different risk — a URL long enough
# that something between here and the terminal truncates it — so the briefing is
# trimmed to fit rather than merely flagged.
MAX_PROMPT_CHARS = 6000

# How much of one SQL statement is worth sending. A dbt test's compiled SQL is
# an assertion and is normally short; the cap is for the pathological generated
# one, and cutting it is better than losing the error text that follows it.
MAX_SQL_CHARS = 2000

# Slack's cap on a Block Kit button `url`. A direct deep link has to live inside
# it, and a message Slack rejects is a lost alert -- so an over-long briefing
# falls back to the page rather than being posted and refused.
SLACK_URL_LIMIT = 3000


# ------------------------------------------------------------------- configuration


def repo() -> str | None:
    """The configured `owner/repo`, or None if it is unset or malformed.

    Malformed is treated as absent rather than raised on, because this is read
    on the delivery path: a typo in an env var must cost the channel a button,
    never the alert it was attached to.
    """
    value = env.get(REPO_ENV, "").strip()
    if not value:
        return None
    if not _REPO_RE.match(value):
        log.warning("%s is not in owner/repo form: %r", REPO_ENV, value)
        return None
    return value


def cwd() -> str | None:
    """The configured local checkout path, or None."""
    value = env.get(CWD_ENV, "").strip()
    if not value:
        return None
    if not value.startswith("/"):
        # A relative path means nothing on the reader's machine, and the deep
        # link handler validates it anyway.
        log.warning("%s must be an absolute path: %r", CWD_ENV, value)
        return None
    return value


def secret() -> str | None:
    value = env.get(SECRET_ENV, "").strip()
    return value or None


def targets() -> tuple[str, ...]:
    """The enabled targets, in the order they should appear.

    Unknown names are dropped with a warning rather than accepted: a typo that
    silently renders no button is the failure mode `slack.parse_routes` exists
    to prevent, and the same reasoning applies here.
    """
    raw = env.get(TARGETS_ENV, "").strip()
    if not raw:
        return ()
    out = []
    for name in (part.strip().lower() for part in raw.split(",")):
        if not name:
            continue
        if name not in KNOWN_TARGETS:
            log.warning("unknown agent target %r in %s; known: %s",
                        name, TARGETS_ENV, ", ".join(KNOWN_TARGETS))
            continue
        if name not in out:
            out.append(name)
    return tuple(out)


def configured() -> bool:
    """Whether any handoff can be offered at all.

    A secret is as load-bearing as a target: the buttons address an endpoint that
    reads failing SQL back out of the database, and an unsigned key would let
    anyone who guessed a `dedup_key` do the same.
    """
    return bool(targets()) and secret() is not None


# ----------------------------------------------------------------------- the key

def sign(event: str, dedup_key: str) -> str | None:
    """An opaque, tamper-evident name for one notification's briefing.

    Deliberately not a random token stored in a table. The briefing is derived,
    so the key only has to *say which* notification without being forgeable, and
    a signed value needs no storage, no cleanup and no retention policy.

    Returns None when unsigned, so a caller that forgot to check `configured()`
    renders no button rather than a broken one.
    """
    key = secret()
    if not key:
        return None
    payload = json.dumps({"e": event, "k": dedup_key}, separators=(",", ":"), sort_keys=True)
    body = base64.urlsafe_b64encode(payload.encode()).decode().rstrip("=")
    mac = hmac.new(key.encode(), body.encode(), hashlib.sha256).hexdigest()[:32]
    return f"{body}.{mac}"


def unsign(token: str) -> tuple[str, str] | None:
    """`(event, dedup_key)` from a signed key, or None if it does not verify."""
    key = secret()
    if not key or not token or "." not in token:
        return None
    body, _, mac = token.rpartition(".")
    expected = hmac.new(key.encode(), body.encode(), hashlib.sha256).hexdigest()[:32]
    if not hmac.compare_digest(mac, expected):
        return None
    try:
        padded = body + "=" * (-len(body) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded.encode()).decode())
    except (ValueError, UnicodeDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    event, dedup = payload.get("e"), payload.get("k")
    if not isinstance(event, str) or not isinstance(dedup, str):
        return None
    return event, dedup


# ------------------------------------------------------------------- the briefing


@dataclass(frozen=True)
class Briefing:
    """What an agent needs to start, assembled from what we already store.

    Every field is optional because every producer is. A Postgres adapter reports
    no `query_id`; a check forwarded from a Snowflake DMF carries no compiled
    SQL; a table nobody has profiled has no upstreams recorded. The prompt is
    built from whatever is present and says nothing about what is not — an agent
    told "the SQL is unavailable" wastes a turn looking for it.
    """

    title: str
    what: str
    table: str | None = None
    column: str | None = None
    check: str | None = None
    error: str | None = None
    sql: str | None = None
    failures: int | None = None
    query_id: str | None = None
    upstreams: tuple[str, ...] = ()
    failing_upstreams: tuple[str, ...] = ()
    unique_id: str | None = None
    dashboard_url: str | None = None
    warehouse_url: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)


def prompt(briefing: Briefing) -> str:
    """The briefing as the text an agent is actually handed.

    Written as a task, not a report. The closing instruction matters more than it
    looks: an agent that opens a pull request against the analytics repo without
    being asked has turned a monitoring alert into a code review someone now owes
    a response to, so the default is to diagnose and propose.
    """
    lines = [
        f"A data quality check just failed in production. {briefing.what}",
        "",
        "## What failed",
    ]
    if briefing.check:
        lines.append(f"- Check: `{briefing.check}`")
    if briefing.table:
        lines.append(f"- Table: `{briefing.table}`")
    if briefing.column:
        lines.append(f"- Column: `{briefing.column}`")
    if briefing.failures:
        lines.append(f"- Failing rows: {briefing.failures:,}")
    if briefing.unique_id:
        lines.append(f"- dbt node: `{briefing.unique_id}`")

    if briefing.error:
        lines += ["", "## Error reported by the warehouse", "```", briefing.error.strip(), "```"]

    if briefing.sql:
        sql = briefing.sql.strip()
        if len(sql) > MAX_SQL_CHARS:
            sql = sql[:MAX_SQL_CHARS].rstrip() + "\n-- … truncated"
        lines += ["", "## The query that found the bad rows", "```sql", sql, "```"]

    if briefing.failing_upstreams:
        # The single most valuable thing dataspine knows that the repository does
        # not. A failing test with a failed upstream in the same window is
        # usually a symptom, and an agent that starts at the symptom rewrites a
        # correct model.
        lines += [
            "",
            "## Upstream runs that also failed in this window",
            *(f"- `{name}`" for name in briefing.failing_upstreams),
            "",
            "Treat these as the likely cause before changing the failing model itself.",
        ]
    elif briefing.upstreams:
        lines += [
            "",
            "## Upstream tables",
            *(f"- `{name}`" for name in briefing.upstreams),
        ]

    links = [
        (label, url)
        for label, url in (
            ("The failing query in Snowsight", briefing.warehouse_url),
            ("This alert in wish:d", briefing.dashboard_url),
        )
        if url
    ]
    if links:
        lines += ["", "## Links", *(f"- {label}: {url}" for label, url in links)]

    lines += [
        "",
        "## What to do",
        "Work out why this check failed. Start from the query above and the "
        "models that feed the table, and check whether an upstream change "
        "explains it before assuming the failing model is wrong.",
        "",
        "Report what you find and propose a fix. Do not commit, push, or open a "
        "pull request unless I ask.",
    ]

    text = "\n".join(lines)
    if len(text) > MAX_PROMPT_CHARS:
        text = text[:MAX_PROMPT_CHARS].rstrip() + "\n\n… briefing truncated."
    return text


# -------------------------------------------------------------------- the targets


def briefing_from_node(node: Any, invocation: Any) -> Briefing | None:
    """A briefing for one failed dbt node, built without touching the database.

    Exists because the dbt path has no connection to touch: `dbt_job_notification`
    is a pure function over an invocation, called from the webhook before anything
    is written. Everything the agent needs is on the node anyway — dbt records the
    compiled SQL, the adapter's message and the column it asserted on.

    The one thing that degrades is lineage. `_lineage_context` answers *which
    upstreams also failed*, and that is a query; here we can only name the
    upstreams themselves, from dbt's own `depends_on`. Naming them without
    claiming any of them failed is honest — the alternative was to assert a cause
    we have not checked.
    """
    if node is None:
        return None
    try:
        from . import dbt_artifacts

        table = (
            dbt_artifacts.tested_relation(invocation, node)
            if node.is_test
            else node.relation
        )
        leaf = table.rsplit(".", 1)[-1] if table else "a table"

        # dbt's graph, resolved through the invocation's own nodes so the reader
        # gets `stg_orders` rather than `model.analytics.stg_orders`.
        by_id = {n.unique_id: n for n in getattr(invocation, "nodes", ())}
        upstreams = tuple(
            by_id[dep].name
            for dep in getattr(node, "depends_on", ())
            if dep in by_id and by_id[dep].unique_id != node.unique_id
        )

        return Briefing(
            title=f"{leaf} — {node.name} failed",
            what=f"The `{node.name}` check on `{table}` failed.",
            table=table,
            column=node.column,
            check=node.name,
            error=node.message,
            sql=node.compiled_code,
            failures=node.failures,
            query_id=node.query_id,
            upstreams=upstreams,
            unique_id=node.unique_id,
            warehouse_url=(snowflake_query_url(node.query_id) or None),
        )
    except Exception:
        # Delivery must never break detection: a node shaped differently from
        # what we expect costs the alert its button, not the alert.
        log.exception("could not build briefing for dbt node")
        return None


def snowflake_query_url(query_id: str | None) -> str | None:
    from . import links as links_mod

    link = links_mod.snowflake_query(query_id)
    return link.get("url") if link else None


def claude_cli_url(briefing: Briefing, *, max_chars: int | None = None) -> str | None:
    """`claude-cli://open?…` — the one target a URL can carry whole.

    Verified against the handler rather than documentation: the scheme is
    registered by "Claude Code URL Handler.app", the host must be `open`, and
    `cwd`/`repo`/`q` are the parameters its parser reads. The handler base64s the
    prompt before it reaches the shell it opens, so arbitrary SQL and error text
    in `q` need no quoting of ours beyond correct URL encoding.

    `repo` is sent only when configured *and* well-formed, because the parser
    rejects the whole link on a bad one rather than ignoring the parameter.
    """
    params: dict[str, str] = {}
    if (path := cwd()) is not None:
        params["cwd"] = path
    if (slug := repo()) is not None:
        params["repo"] = slug
    if not params:
        # With neither, the agent opens somewhere arbitrary. That is the guessed
        # link this module exists to refuse.
        return None
    text = prompt(briefing)
    url = "claude-cli://open?" + urlencode({**params, "q": text})
    if max_chars is None or len(url) <= max_chars:
        return url

    # Percent-encoding roughly doubles the SQL-and-newline-heavy tail, so the
    # overshoot cannot be subtracted from the prompt directly. Halve the budget
    # until it fits: at most a handful of iterations, and it keeps the head of
    # the briefing — what failed, and the error — which is the half that matters.
    budget = len(text)
    while budget > 200:
        budget //= 2
        trimmed = text[:budget].rstrip() + "\n\n… briefing truncated; full detail in wish:d."
        url = "claude-cli://open?" + urlencode({**params, "q": trimmed})
        if len(url) <= max_chars:
            return url
    return None


def codex_app_url() -> str | None:
    """`codex://threads/new?…` — opens the Codex app in the right workspace.

    Carries no prompt, and cannot: `codex app` accepts a workspace path and
    nothing else. So this is offered *beside* the command below rather than
    instead of it — on its own it would open a blank thread, which is not
    sending anyone the error.
    """
    path = cwd()
    if not path:
        return None
    return "codex://threads/new?" + urlencode({"cwd": path})


def codex_command(briefing: Briefing) -> str:
    """The `codex exec` invocation that carries the whole briefing.

    Single-quoted with the shell's own escape for an embedded quote, because the
    briefing contains SQL and SQL contains apostrophes.
    """
    text = prompt(briefing).replace("'", "'\\''")
    return f"codex exec '{text}'"


# ------------------------------------------------------- rebuilding the briefing


def briefing_for(conn: Any, event: str, dedup_key: str) -> Briefing | None:
    """The briefing for one notification, derived rather than recalled.

    Returns None when the notification cannot be found, which is not an error:
    a `dedup_key` names a check result, and retention eventually removes those.
    An alert from last month opening a page that says so is honest; one opening
    an agent with an empty briefing is not.
    """
    try:
        if event == "data_test":
            return _data_test_briefing(conn, dedup_key)
        if event == "dbt_job":
            return _dbt_node_briefing(conn, dedup_key)
    except Exception:
        # The handoff is delivery, and delivery must never break detection. A
        # malformed key or a schema we did not expect costs the reader a button,
        # not the sweep that produced the alert.
        log.exception("could not build briefing for %s/%s", event, dedup_key)
        return None
    return None


def _data_test_briefing(conn: Any, dedup_key: str) -> Briefing | None:
    """`source/table/check/measured_at`, as `data_test_notifications` keys it."""
    parts = dedup_key.split("/", 3)
    if len(parts) != 4:
        return None
    source, table, check, measured_at = parts
    row = conn.execute(
        """
        select source, table_name, check_name, status, value, measured_at, details
        from external_checks
        where source = %(source)s
          and table_name = %(table)s
          and check_name = %(check)s
          and measured_at = %(measured_at)s
        limit 1
        """,
        {"source": source, "table": table, "check": check, "measured_at": measured_at},
    ).fetchone()
    return briefing_from_check_row(conn, row) if row else None


def _dbt_node_briefing(conn: Any, dedup_key: str) -> Briefing | None:
    """`<root run>/<dbt unique_id>`, as `_dbt_node_notification` keys it.

    Looked up by `unique_id` rather than by run, because that is what identifies
    the assertion across the several runs it has been evaluated in — and the
    latest result is the one an agent should be reasoning about.
    """
    _, _, unique_id = dedup_key.partition("/")
    if not unique_id:
        return None
    row = conn.execute(
        """
        select source, table_name, check_name, status, value, measured_at, details
        from external_checks
        where details->>'unique_id' = %(unique_id)s
        order by measured_at desc
        limit 1
        """,
        {"unique_id": unique_id},
    ).fetchone()
    return briefing_from_check_row(conn, row) if row else None


def briefing_from_check_row(conn: Any, row: dict[str, Any]) -> Briefing:
    from . import links as links_mod

    details = row.get("details") or {}
    table = row.get("table_name")
    check = row.get("check_name")
    leaf = table.rsplit(".", 1)[-1] if table else "a table"

    failures = row.get("value")
    failures = int(failures) if failures is not None else None

    upstreams, failing = _lineage_context(conn, table)

    return Briefing(
        title=f"{leaf} — {check} failed",
        what=f"The `{check}` check on `{table}` failed.",
        table=table,
        column=details.get("column"),
        check=check,
        error=details.get("message"),
        sql=details.get("compiled_sql"),
        failures=failures,
        query_id=details.get("query_id"),
        upstreams=upstreams,
        failing_upstreams=failing,
        unique_id=details.get("unique_id"),
        warehouse_url=(links_mod.snowflake_query(details.get("query_id")) or {}).get("url"),
        extra={"source": row.get("source"), "test_type": details.get("test_type")},
    )


def _lineage_context(conn: Any, table: str | None) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Upstream tables, and which of them had a run fail recently.

    The second is the part worth sending. dataspine is the only system in the
    stack that can answer it — dbt knows the graph but not what happened on it,
    and the warehouse knows what happened but not the graph — and it is what
    stops an agent rewriting a correct model to satisfy a test that broke
    because something above it did.
    """
    if not table:
        return (), ()
    from . import lineage

    # One physical table can legitimately resolve to several entities that
    # `identity.py` declined to merge -- `AIDAN_DEV.PUBLIC.fct_order_items` and
    # `AIDAN_DEV.dbt_aidan.fct_order_items` are one model in two schemas, not one
    # table. So the fallback to a leaf-name match must never outrank an exact
    # one, and ties break on id rather than on whatever the planner returns
    # first: an unordered `limit 1` here silently attributed a check to a
    # different environment's lineage, and did it differently between calls.
    entity = conn.execute(
        """
        select de.id
        from dataset_entities de
        join dataset_identities di on di.entity_id = de.id
        join datasets d on d.id = di.dataset_id
        where d.name = %(table)s
           or regexp_replace(d.name, '^.*[./]', '')
              = regexp_replace(%(table)s, '^.*[./]', '')
        order by (d.name = %(table)s) desc, de.id
        limit 1
        """,
        {"table": table},
    ).fetchone()
    if not entity:
        return (), ()

    rows = lineage.upstream(conn, entity["id"], depth=2)
    if not rows:
        return (), ()

    # Nearest first: `lineage.upstream` returns hop distance precisely so the
    # likeliest cause sorts to the top, and an agent reads the list in order.
    ordered = tuple(
        r["name"] for r in sorted(rows, key=lambda r: r.get("distance", 0)) if r.get("name")
    )

    # Matched by **entity id, through `dataset_identities`** -- never by name.
    # `dataset_entities.name` is a leaf (`stg_orders`) while `datasets.name` is
    # whatever the producer called it (`AIDAN_DEV.PUBLIC.stg_orders`,
    # `/warehouse/iceberg/analytics_marts/stg_orders`), so comparing the two
    # matches nothing and this silently returned "no upstream failed" for every
    # input. `identity.py` exists to merge exactly those spellings; going back
    # through it is both correct and the only thing that stays correct when a
    # table arrives under a name nobody predicted.
    failed = conn.execute(
        """
        select distinct de.name
        from runs r
        join run_datasets rd on rd.run_id = r.run_id
        join dataset_identities di on di.dataset_id = rd.dataset_id
        join dataset_entities de on de.id = di.entity_id
        where rd.direction = 'OUTPUT'
          and r.state = 'FAILED'
          and r.ended_at >= now() - interval '24 hours'
          and di.entity_id = any(%(ids)s)
        """,
        {"ids": [r["id"] for r in rows]},
    ).fetchall()
    failing = tuple(r["name"] for r in failed)
    return ordered[:10], failing[:10]


# --------------------------------------------------------------------- the buttons


def handoff_urls(
    event: str,
    dedup_key: str,
    base_url: str | None,
    briefing: Briefing | None = None,
) -> tuple[tuple[str, str], ...]:
    """`(label, url)` for each enabled target — the actions an alert offers.

    **`claude-cli` goes straight to the terminal.** Slack navigates the
    `claude-cli://` scheme from a Block Kit button — measured, by clicking one —
    so for the one target whose URL can carry the whole briefing there is no
    reason to route a reader through a page first.

    The other two still address `/handoff` here, because neither has a URL that
    carries a prompt: Claude cloud has no create-a-session address at all, and
    Codex's deep link opens a workspace and nothing more. That asymmetry is the
    feature working as designed, not an inconsistency to be tidied away.

    What the direct link costs is worth stating. The briefing is built **now**
    rather than at click time, so an alert opened in the morning carries the
    night's lineage; and a machine with no handler registered gets a click that
    does nothing, where the page would have explained itself. Both were accepted
    deliberately — see ADR-008.
    """
    if not base_url or not configured():
        return ()
    key = sign(event, dedup_key)
    if not key:
        return ()
    base = base_url.rstrip("/")

    out: list[tuple[str, str]] = []
    for target in targets():
        url = None
        if target == CLAUDE_CLI and briefing is not None:
            url = claude_cli_url(briefing, max_chars=SLACK_URL_LIMIT)
        # Falls back to the page whenever the direct link cannot be built — no
        # checkout configured, or a briefing too long to survive the cap. The
        # page has neither limit, so the action is still offered.
        out.append((TARGET_LABELS[target], url or f"{base}/handoff/{key}?target={target}"))
    return tuple(out)
