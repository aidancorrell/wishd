# Handing an alert to a coding agent

Every wishd alert answers *what broke*. The Snowsight link answers *show me*.
Neither answers *fix it*, and that is where the on-call engineer's evening goes:
reading the failing SQL, finding the model that wrote it, working out which
upstream change is responsible.

This feature puts a row of buttons on the alert that hands that reading to a
coding agent, already briefed. `src/dataspine/agents.py` is the implementation;
this file is the part that had to be *measured*, because none of it is in any
vendor's documentation.

---

## Configuration

Nothing is offered until it is named. With `WISHD_AGENT_TARGETS` unset the
feature is entirely off, which is the right default for something that puts a
button saying "fix this" in front of a whole channel.

| Variable | Required | What it does |
|---|---|---|
| `WISHD_AGENT_TARGETS` | yes | Comma-separated: `claude-cli`, `claude-cloud`, `codex`. Unknown names are dropped with a warning. |
| `WISHD_AGENT_SECRET` | yes | Signs the handoff key. **Without it no button renders at all** — see [Why the key is signed](#why-the-key-is-signed). |
| `WISHD_AGENT_CWD` | for local targets | Absolute path to a checkout on the *reader's* machine. |
| `WISHD_AGENT_REPO` | optional | `owner/repo`. Rejected, with a warning, in any other shape. |
| `WISHD_BASE_URL` | yes | Already required for alert links. The `codex` and `claude-cloud` buttons address this host. |
| `WISHD_SLACK_SIGNING_SECRET` | to silence the ⚠ | The Slack app's Signing Secret, for `/slack/interactivity`. See [The interactivity warning](#the-interactivity-warning). |

`WISHD_AGENT_CWD` deserves a note: it is a path on the machine of whoever
*clicks*, not on the wishd host. A team whose checkouts live in different
places gets one value that suits some of them; there is no way for a server to
know better, and guessing per-reader is the thing this feature refuses to do.

---

## What was measured

Three targets, and **only one of them is reachable by a URL**. That asymmetry is
what decides which button is a deep link and which goes through a page.

### Claude Code, locally — a real deep link

Verified against the handler, on macOS, 2026-09-05, Claude Code 2.1.261.

`claude-cli:` is registered by `Claude Code URL Handler.app`, which ships beside
the CLI. Its parser takes:

```
claude-cli://open?cwd=<abs path>&repo=<owner/repo>&q=<prompt>
```

- The host **must** be `open`. Anything else raises `Unknown deep link action`.
- `repo` **must** match `owner/repo`, or the whole link is rejected — not the
  parameter, the link. So wishd omits it rather than sending a malformed one.
- `q` is the prompt. Claude Code **stages it for review** rather than running it:
  *"The prompt below was supplied by the link — review carefully before pressing
  Enter."* Nothing executes without a human keystroke.

Firing one shows what the handler does with it:

```
claude --deep-link-origin \
  --deep-link-cwd-b64=<base64 cwd> \
  --prefill-b64=<base64 prompt>
```

The prompt is **base64'd before it reaches the shell** the handler opens — it
goes out through `osascript … do script`, so a raw prompt would be an injection
surface and they encode it to close that. The consequence for us is a good one:
the briefing can contain arbitrary SQL, quotes and newlines and needs no quoting
of ours beyond correct URL encoding. A 1,139-character briefing containing
backticks, embedded `'` quotes and a SQL block round-tripped byte-identical.

It opens the machine's default terminal and starts a fresh session. It does not
attach to a running one.

### Claude Code in the cloud — no URL exists

`claude.ai/code/<id>` *attaches* to a session that already exists. There is no
create-a-session-with-this-prompt URL: creating one is `claude --cloud`, an
action, not an address.

So the page shows the briefing to copy, and links to `claude.ai/code`. **It does
not create the session.** Doing that would mean running the Claude CLI on the
wishd host, authenticated, spawning a process on a remote trigger — a
deployment and security decision that belongs to the operator rather than to a
default. See the deferral in `ROADMAP.md`.

### Codex — a deep link that cannot carry a prompt

`codex://` is registered by the Codex desktop app, and `codex app` opens
`codex://threads/new?<workspace>`. But `codex app` accepts a workspace path and
**nothing else** — there is no prompt parameter. Opening a blank thread in the
right directory is not sending anybody the error.

So the prompt-carrying path is the CLI, and the page leads with it:

```
codex exec '<briefing>'
```

with the app link offered underneath as a convenience. The page says plainly that
the link cannot carry the prompt, because a button that looks like it should and
doesn't is worse than one that isn't there.

---

## Why one button is a deep link and two are not

A Slack button holds a URL and nothing else. Of the three targets, exactly one
has a URL that carries the whole briefing — so that one *is* the deep link, and
the click goes from Slack to a terminal with nothing in between.

**Slack navigates `claude-cli://` straight from a Block Kit button.** Measured by
clicking one, 2026-09-05. Slack accepts the scheme at post time and follows it on
click, from both a button and an mrkdwn link. The briefing fits comfortably: 1,349
characters against Slack's 3,000-character cap on a button `url`.

The other two address `/handoff/<key>?target=…` here, because neither has a URL
that takes a prompt. The endpoint then decides what the target means: a page
offering the `codex exec` command, or the briefing to paste into Claude cloud.

Routing *every* button through the page would have charged the one target that
works for the sins of the two that do not. Routing *none* of them through it
would have shipped one target and faked two.

### What the two routes trade

| | Direct deep link | Via `/handoff` |
|---|---|---|
| Briefing built | at alert time, frozen | at click time, current |
| Length limit | 3,000 chars, falls back to the page | none |
| No handler installed | click does nothing, silently | page explains itself |
| Lineage on the dbt path | upstreams only, from `depends_on` | upstreams *and* which of them failed |

That last row is the one to watch. `dbt_job_notification` is a pure function over
an invocation with no database connection — it is called from the webhook before
anything is written — so a briefing built there can name upstream models from
dbt's own graph but cannot say which of them failed. It says so by omission
rather than guessing. The data-test path has a connection and carries the full
picture.

An over-long briefing falls back to the page rather than being posted: Slack
rejects a button `url` past 3,000 characters, and a rejected message is a lost
alert.

## The interactivity warning

**Every button in an `actions` block sends Slack an interaction payload when
clicked — including a `url` button that Slack is already opening.** An app with
no Request URL configured cannot answer, so Slack draws a ⚠ beside the message.
The buttons work; the alert just looks broken, which for an alerting product is
its own kind of broken.

`POST /slack/interactivity` exists to say 200 and nothing more. Configure it as
the app's **Interactivity Request URL** at `https://<your wishd>/slack/interactivity`
and set `WISHD_SLACK_SIGNING_SECRET` to the app's Signing Secret.

It deliberately does not act on the payload. The click's real effect is the URL
Slack is opening in the reader's browser; a second, server-side effect fired from
the same click would be a surprise nobody asked for.

Slack signs `v0:{timestamp}:{body}` with the Signing Secret. The timestamp is
inside the signed string so it cannot be edited, and its age is checked anyway —
a signature stays valid forever, so without a window a captured request could be
replayed indefinitely. An unset secret refuses everything rather than accepting
anything.

If you would rather not configure a Slack app at all, mrkdwn links generate no
interaction payload and therefore never warn — at the cost of not looking like
buttons.

### Why the key is signed

The handoff page renders **the compiled SQL of a failing check**. The key names
which check, and if it were guessable the page would be an oracle for anyone who
could guess a table and check name.

So the key is an HMAC over `(event, dedup_key)`, and `agents.configured()`
returns `False` until a secret is set — no secret, no buttons, rather than
buttons leading to an open page. A bad signature and an unset secret return the
same 404, so an unauthenticated caller learns nothing about which they hit.

It is deliberately *not* a random token in a table: the briefing is derived, so
the key only has to say *which* notification without being forgeable. A signed
value needs no storage, no cleanup, and no retention policy of its own.

---

## What the agent is told

Rebuilt at click time, as a query over what is already stored — so an alert that
sat in a channel overnight hands over the current state of the lineage rather
than last night's, and nothing new has to be retained.

- What failed: check, table, column, failing row count, dbt node id
- The adapter's error text
- The compiled SQL that found the bad rows
- **Upstream runs that also failed in the same window**
- Links back to Snowsight and to wishd

The fourth is the one worth the effort. dbt knows the graph but not what happened
on it; the warehouse knows what happened but not the graph. An agent that starts
at the symptom will rewrite a correct model to satisfy a test that broke because
something above it did, so the briefing names the likely cause and says to look
there first.

It closes by asking for a diagnosis and a proposed fix, and explicitly **not** a
commit, push or pull request. An agent that opens a PR against the analytics repo
unasked has turned a monitoring alert into a code review somebody now owes a
response to.

## What is never offered

- **A `severity: warn` test.** Its author said in writing that they did not want
  waking for it; offering to put an agent on it is a louder version of the same
  interruption.
- **A recovery.** There is nothing left to investigate.
- **A grouped message.** With twelve failures in one message there is no answer
  to "which one would it investigate?", and a button that silently picks is worse
  than no button. Thread replies are rendered one at a time, so each failing test
  under a dbt run still gets its own.
