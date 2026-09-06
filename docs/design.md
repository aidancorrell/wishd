# Interface design

The rules the UI is built on, the measurements behind them, and how to check a
change has not broken one. Written for whoever picks this up next, including a
future session that has none of the context.

Precedence, when these disagree with something: [architecture decisions](architecture.md) win, because
they constrain what is even buildable here. ADR-002 in particular — server-
rendered Jinja, hand-written CSS, **no build step, no framework, no JavaScript**
— is why several things below look unusual.

---

## The rules

### 1. Chroma means run state. Nothing else may use it.

Colour is reserved for run-state fills and running/warning/critical text.
Navigation, links, buttons, focus rings and current-row highlights are
achromatic — they earn their affordance from weight, border and underline.

There is deliberately **no `--accent` token**. There used to be, and it held the
same hex as `--running`, so a hyperlink and a running pipeline were the same
colour on a page whose entire job is telling you what state things are in. When
something needs emphasis, reach for weight or a neutral, never a hue.

### 2. Completed text is quiet; completed fills remain distinct.

Completed labels use neutral grey text. Completed dots and bars use a sage fill
with a teal lean (`--ok-fill`), so the run band still distinguishes states.
Do not use the fill token for text: text contrast and state separation have
separate requirements.

### 3. A state has a fill colour and a text colour, and they are not the same.

The amber that separates cleanly from red as a 3px bar measures 2.6:1 as text,
well under the 4.5:1 floor. So `--warn` is the readable one for words and
`--warn-fill` the vivid one for dots, bars and washes. Same for crit; `--ok`'s
text role stays neutral while its fill carries colour.

Getting this backwards is easy and silent. Text roles are `color:`; fill roles
are `background:`, `border-color:`, `box-shadow:`, `fill:` and `stroke:`.

### 4. Colour is never the only signal.

Each state differs in **silhouette** too: circle for completed, circle with a
halo for running, square for failed, diamond for aborted, dashed ring for
unknown. They stay distinct at 8px. A greyscale render must separate all five
with no colour information at all — that is the test, not an aspiration.

Failed bars in the band fill the track's full height for the same reason.

State dots are `role="img"` with an `aria-label`, **not `aria-hidden`**: run rows
and timeline entries show the dot with no state word beside it, so hiding it
removes the state entirely for a screen reader.

### 5. Neutrals are untinted.

A colour cast in the greys is low-volume chroma competing with the state colours. Keep neutrals visually untinted.

---

## Tokens

Defined in `src/dataspine/static/style.css`. Three blocks, in this order:
`:root` (light) → `@media (prefers-color-scheme: dark) { :root:not([data-theme="light"]) }`
→ `:root[data-theme="dark"]`. **The two dark blocks are duplicated** because
there is no build step to hoist them; keep them in sync (there is a check for
this — see below).

| role | light | dark |
|---|---|---|
| `--ground` | `#f1f1f2` | `#131317` |
| `--surface` | `#ffffff` | `#1b1b21` |
| `--surface-2` | `#e2e2e5` | `#25252c` |
| `--ink` | `#1b1b1e` | `#d9d9e0` |
| `--ink-2` | `#44444a` | `#a5a5af` |
| `--ink-3` | `#63636b` | `#8b8b96` |
| `--rule` | `#d2d2d7` | `#3a3a44` |
| `--rule-2` | `#9d9da8` | `#54545f` |
| `--ok` / `-fill` | `#6a6a74` / `#56967a` | `#8a8a95` / `#7cbb9d` |
| `--running` / `-fill` | `#1f5fd0` | `#6fa4f0` |
| `--warn` / `-fill` | `#8a5e00` / `#d18f00` | `#e5b352` / `#e0a63a` |
| `--crit` / `-fill` | `#a3271c` / `#c1372a` | `#f08076` / `#e2564a` |

Type scale: `--t-xs` 11px, `--t-sm` 12.5px, `--t-base` 13.5px, `--t-md` 15px,
`--t-lg` 20px, `--t-xl` 30px. Spacing unit `--step` 0.25rem.

---

## The contract

Any palette change must still satisfy all of these. They are not preferences.

| constraint | threshold | why |
|---|---|---|
| Every text token on `--surface` **and** `--ground` | ≥ 4.5:1 | WCAG AA. `--ink-3` shipped at 3.77:1 for a while and carries most of the secondary text. |
| State **fills**, pairwise, under deuteranopia *and* protanopia | ΔE ≥ 0.10 (OKLab) | A just-noticeable difference is ~0.02. Below 0.05 two states are one colour. |
| `--ink` on `--ground`, **dark theme only** | ≤ 13.5:1 | Near-white on near-black halates. This is a tool people keep open for hours. Light theme is deliberately uncapped — dark-on-light does not halate, and crisp is good. |
| `--rule` on `--surface` | ≥ 1.5:1 | Not a spec floor. 1.4–1.7 is the industry norm for table hairlines (GitHub's `#d0d7de` is 1.42); dark mode shipped at 1.19:1 and the rules were simply invisible. `--rule-2`, which draws structural and interactive borders, is held higher. |
| The two dark token blocks | identical | No build step hoists them. |

### How to check

There is no dependency to install; the maths is short enough to inline.

- **Contrast**: sRGB → linear → relative luminance → `(L1+.05)/(L2+.05)`.
- **Perceptual distance**: convert to OKLab, take Euclidean distance. OKLab
  because equal steps look equal across hues, which sRGB and HSL do not give.
- **Colour deficiency**: apply the Machado (2009) severity-1.0 matrices to
  *linear* RGB, then convert back.

  ```
  deuteranopia            protanopia
  [ 0.367322  0.860646 -0.227968]   [ 0.152286  1.052583 -0.204868]
  [ 0.280085  0.672501  0.047413]   [ 0.114503  0.786281  0.099216]
  [-0.011820  0.042940  0.968881]   [-0.003882 -0.048116  1.051998]
  ```

Check the actual served stylesheet after a build as well as the source: stale assets or
duplicate token blocks can hide an incorrect cascade. Keep completed text neutral and
verify any change to the sage fill against both colour-deficiency simulations.

---

## Typography

Three faces, vendored as `woff2` under `src/dataspine/static/fonts` with the OFL
licence beside them. ~124 KB, no CDN, nothing phones home, offline installs
work. ADR-002 permits vendored assets; it forbids a build step, and
`StaticFiles` serves these with none.

| face | carries | why |
|---|---|---|
| **Newsreader** | page titles, the wordmark, incident headings | A page title is read once, not scanned, so it can afford a voice. It is the only place the interface has one. |
| **Hanken Grotesk** | body, labels, prose | Wider apertures than Plex Sans at 13px, which is the size most of this interface is set at. |
| **IBM Plex Mono** | identifiers, durations, counts, anything in a column | Unchanged. |

Both new faces are variable, so three sans weights cost one file. Plex Sans was
dropped when Hanken replaced it rather than left behind — an unreferenced face
still ships to whoever clones the repo.

System font stacks were the state before either, and are the single most
reliable signal that nobody chose anything.

Mono is for identifiers, durations, counts and anything in a column. Everything
mono that can line up gets `font-variant-numeric: tabular-nums`.

### The serif is not decoration

It marks a boundary: everything in Newsreader is something a person reads, and
everything in Plex Mono is something a machine emitted. The sans sits between
them. A run id in the serif would be as wrong as a page title in the mono.

---

## Theming

Three states, not two, and the third is the common one:

- **no stored choice** → nothing stamped, the media query decides
- **`data-theme="light"`** → beats a dark OS
- **`data-theme="dark"`** → beats a light OS

Implemented as a form and a cookie (`web.set_theme`), not a script — ADR-002
leaves no JavaScript to persist a choice or re-apply it on the next load. The
server already stamps every page, so it stamps this too, which also means the
theme is correct in the **first painted frame**, with none of the flash a
script-applied theme has to work around.

"Auto" genuinely clears the cookie. It is not a synonym for whichever theme we
prefer. `next` is constrained to same-site paths so the form cannot become an
open redirect.

---

## Layout conventions

- **Rows of data are grids, not flows.** Fixed columns, right-aligned tabular
  numbers, one elastic column (usually the name) that truncates. Cells are
  emitted even when empty — an empty cell keeps its column, and a column that
  moves per row is the thing the grid replaces.
- **Disclosure is native `<details>`** (ADR-002 names it). Never put an `<a>`
  inside a `<summary>`: it is both a navigation and a toggle, ambiguous with a
  mouse and broken with a keyboard. Links go in the expanded panel. There is a
  test asserting this.
- **`<details>` renders its contents whether open or shut**, so every expandable
  row costs page weight on every load. Inline trees are capped, and when the cap
  bites it drops the rows nobody opens: failed first, then live, then recent.
- **Icons are a sprite** (`_icons.html`), defined once and referenced with
  `<use>`. They used to be inlined per call site; a 7-day overview emitted 781
  copies and half the page was repeated icon markup.
- One uppercase treatment only — `.eyebrow`. Uppercase everywhere else is label
  soup and stops reading as a label.
- **Prose does not belong in the UI.** Panels used to open with explanatory
  paragraphs about OpenLineage heartbeats. That is documentation; it lives in
  `title` attributes or in docs. The most reliable tell of generated design is
  that every section is introduced like a speaker at a conference.

---

## Open

Roughly in the order I would take them.

- **The icon set.** Seven of eighteen keys share a glyph (`postgres`/`mysql`/
  `redshift`/`hive` are one cylinder; `s3`/`gcs`/`adls` one bucket), `dbt` and
  `databricks` collide, stroke weights range 1.3–1.6 so optical weight is
  inconsistent, and `bigquery` is a magnifying glass — a search icon for a
  warehouse. Unblocked now that ink values are settled, since icon weight has to
  be tuned against them.
- **A wordmark.** The favicon is a 🧬 emoji and the brand is the word in mono.
  Emoji-as-mark is a clear "assembled quickly" signal and it is the first thing
  anyone sees in a tab.
- **Band time ticks.** It labels only its two ends; hour gridlines and a marked
  "now" edge would let you read *when* a pile-up happened.
- **Snap spacing to `--step`.** The token exists and is barely used; padding is
  still ad-hoc rem values per component.
- **Compact rows as a preference.** In-flight rows run ~56px against a 40–44px
  convention for ops tables. A cookie-backed form would do it with no
  JavaScript, the same mechanism the theme control uses.

## Traps

Things that went wrong here, so they do not go wrong again.

- **A missing layout rule reads as bad taste.** The overview's counts used
  `class="stats"` while the stylesheet only defined `.health`, so six numbers
  stacked vertically down 370px. A large share of "this looks like a hackathon
  project" was that one miss, not the palette.
- **Screenshot after ingest settles.** The gateway ingests asynchronously; a
  screenshot taken immediately after seeding caught the correlator mid-flight
  with placeholders unfilled, showing `11 unstitched` and fragment rows. It
  self-healed to 0. Nothing was wrong.
- **Render the page before judging it.** Several real defects here were invisible
  in the HTML source and obvious in a screenshot. If the browser extension
  cannot reach localhost, headless Chrome against a saved copy works:
  `curl` the page and `/static/style.css`, rewrite the two asset paths, then
  `chrome --headless --screenshot`.
