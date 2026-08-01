I explored the author's `mosaic` repo (Next 16 + Go hub + `@xyflow/react` + a hand-rolled `useRoomSocket`), and ran the dataviz skill's palette validator to derive — not eyeball — every color below. Every hex quoted passed the six checks; the failures I hit and fixed are noted inline.

---

# ITHACA — Frontend Design Document

---

## 0. The one idea

**Color is a data channel, not decoration.** The chrome of this product is achromatic (blue-cast neutrals, hairlines, tabular mono). The only saturated pixels on screen are *event state*. That single rule is why it won't look like a shadcn template — every generic dashboard spends color on chrome, then has nothing left to say with it.

Corollary, and the credibility rule for the whole build: **the frontend never simulates. It only observes.** Every particle, counter, retry ladder and breaker trip is a real Go-side state transition arriving over the wire.

---

## 1. Stack decision

### 1.1 Verdict: **Vite 7 + React 19 + React Router 7 SPA, compiled into the Go binary via `embed.FS`.** Not Next.

This is a deviation from the author's habit and it is the right one. The defense:

**Next.js earns its keep through the server** — RSC, streaming, ISR, `next/image`, middleware. Ithaca has *zero* server-renderable content. Every screen is a live view of state that changes 10× per second behind a login wall. Running a Node process to server-render a shell that is immediately replaced by WebSocket data is paying full price for nothing.

**The Kubernetes argument is decisive.** Next-with-Node means a second image, a second Deployment, a second Service, a second HPA, a second readiness probe, a second thing to roll back at 2am, and a cross-service hop for every API call (or CORS + a token in JS). The Vite build is `dist/` → `//go:embed web/dist` → **one binary, one container, one Deployment, one Ingress**. Same origin for HTTP and WS. This is what Grafana, Prometheus, Traefik, and Vault all do, and "the Go binary serves its own console" is a *better* story for a recruiter than "Next app on Vercel."

**Next static export (`output: "export"`) does not rescue it.** App Router static export can't produce `/events/[id]` without `generateStaticParams`, and this app's detail routes are unbounded UUIDs. An SPA with `index.html` fallback handles that natively — Go's static handler serves `index.html` for any non-`/api`, non-`/ws` path and the router takes over.

**Nothing the author actually likes is lost.** React 19, TypeScript, npm + `package-lock.json`, Tailwind v4 CSS-first (`@import "tailwindcss"` + `@theme inline`, no config file), shadcn CLI in `base-nova` style on `@base-ui/react`, `cva` + `tailwind-merge` + `cn()`, lucide, sonner, `@xyflow/react`, `react-resizable-panels`, framer-motion — all framework-agnostic, all carry over unchanged. shadcn has a first-class Vite install path; `components.json` just points `tailwind.css` at `src/styles/globals.css`.

Three things change, all small:
- `next/font` → self-host Geist. Drop `Geist[wght].woff2` and `GeistMono[wght].woff2` in `public/fonts/` with two `@font-face` blocks and `font-display: swap`. Zero dependency, guaranteed, and one fewer network origin.
- `next-themes` → a 30-line `ThemeProvider` using `localStorage` + `matchMedia`, plus a **blocking inline script in `index.html`** that stamps `class="dark"` on `<html>` before first paint (no flash). This is ~40 lines total and removes a dependency.
- `next/link` / `usePathname` → React Router's `<Link>` / `useLocation`.

Dev experience is preserved exactly: `vite dev` on `:5173` with `server.proxy` forwarding `/api` and `/ws` (with `ws: true`) to Go on `:8080`. Two terminals, same as mosaic today.

### 1.2 Auth

**Session cookie for the dashboard; API keys never touch the browser.**

- `POST /api/v1/auth/login` → Go sets `ithaca_session`, `HttpOnly; Secure; SameSite=Lax; Path=/`, containing an opaque session ID backed by Redis (not a JWT — you need instant revocation for a control plane).
- Every `/api/v1/*` call is cookie-authenticated. No `Authorization` header, no token in JS, no XSS token theft.
- **The WS handshake gets the cookie for free**, because it's same-origin. This is a concrete improvement over mosaic's `?token=<jwt>` querystring, which leaks credentials into every proxy and access log — worth calling out in the README as a deliberate fix.
- CSRF: `SameSite=Lax` blocks cross-site POSTs; additionally require a `X-Ithaca-CSRF` header echoing a non-HttpOnly `ithaca_csrf` cookie on all mutating verbs (double-submit).
- Customer API keys (`ik_live_…`) authenticate *ingest* (`POST /v1/ingest`) via `Authorization: Bearer`. They are shown **once** at creation and stored server-side as SHA-256 hashes. The Settings screen shows prefix + last-4 + created/last-used only.
- Boot sequence: SPA loads → `GET /api/v1/me` → 200 renders app, 401 redirects to `/login` with `?next=`.

### 1.3 Data fetching: **yes, adopt TanStack Query v5.** Finally.

Their custom-hooks-plus-axios habit would require hand-rolling all five of the things this app actually needs:

1. `useInfiniteQuery` with keyset cursors for the events table.
2. Targeted cache invalidation driven by WS deltas — the exact reason the library exists.
3. Optimistic mutations with rollback for replay/bulk-replay.
4. Request dedup across the three panes that all read endpoint health.
5. `refetchOnWindowFocus` + `staleTime`, so a tab left open for an hour isn't lying.

13KB gzipped. Use `fetch` (with a thin `apiFetch` wrapper adding the CSRF header and throwing typed `ApiError`), not axios — axios buys nothing when you have one origin and no interceptor needs.

**The critical boundary — this is the architectural spine of the app:**

> **TanStack Query owns entity state. It never sees the firehose.**
> **A plain-JS `StreamStore` (refs + ring buffers + `requestAnimationFrame`) owns stream state. It never sets React state per message.**

Frames at 10Hz carrying 1,000 events/sec must not touch the query cache or the reconciler. Only *entity-level* deltas (an endpoint's circuit tripped; an event reached a terminal state) cross the boundary into `queryClient.setQueryData`. Everything else lands in ring buffers that React reads through `useSyncExternalStore` with a rAF-coalesced notifier.

### 1.4 Locked dependency list

```
react 19 · react-dom 19 · react-router 7 · vite 7 · typescript 5.7
tailwindcss 4 · @tailwindcss/vite · tailwind-merge · clsx · class-variance-authority · tw-animate-css
@base-ui/react (shadcn base-nova) · lucide-react · sonner · react-resizable-panels
@tanstack/react-query 5 · @tanstack/react-virtual 3
@xyflow/react 12
uplot 1.6
framer-motion 12   ← layout/overlay transitions ONLY, never data
cmdk (or shadcn's command)
```

**Not installed, deliberately:** recharts, visx, d3, zustand, redux, axios, moment/dayjs (use `Intl.DateTimeFormat` + `Intl.RelativeTimeFormat`), lodash.

---

## 2. The design system

### 2.1 The oklch token set

All values below were run through the dataviz validator. My first pass **failed** three hard checks (dark lightness band, chroma floor on the neutral `queued`, and the normal-vision ΔE floor between `queued` and `delivering`), so these are the re-stepped, passing values — do not "brighten" them by eye without re-running the validator.

`src/styles/tokens.css`:

```css
:root {
  /* ── surfaces (light) ── */
  --bg-canvas:    oklch(0.985 0.003 265);  /* #f9fafc  page plane   */
  --bg-surface:   oklch(1     0     0);    /* #ffffff  panels/cards */
  --bg-inset:     oklch(0.965 0.003 265);  /* #f2f3f5  wells, thead */
  --bg-raised:    oklch(1     0     0);    /* overlays              */
  --bg-hover:     oklch(0.960 0.004 265);

  /* ── hairlines: elevation without shadow ── */
  --line:         oklch(0.20 0.01 265 / 10%);   /* default rule      */
  --line-strong:  oklch(0.20 0.01 265 / 18%);   /* section divider   */
  --line-faint:   oklch(0.20 0.01 265 / 5%);    /* in-table row rule */

  /* ── text hierarchy (contrast vs #ffffff, validated) ── */
  --fg:      oklch(0.20 0.008 265);  /* #14161a  18.1:1  primary   */
  --fg-2:    oklch(0.44 0.008 265);  /* #505257   7.8:1  secondary */
  --fg-3:    oklch(0.56 0.008 265);  /* #727479   4.7:1  muted     */
  --fg-4:    oklch(0.68 0.008 265);  /* #96989d   2.9:1  LARGE/decorative only */

  /* ── signal: the single brand accent (teal 195°) ── */
  --signal:        oklch(0.50 0.105 195);  /* #137171  5.8:1 — links, focus, primary */
  --signal-ink:    oklch(0.46 0.100 195);  /* #106565  6.8:1 — on cards */
  --signal-wash:   oklch(0.50 0.105 195 / 10%);
  --focus-ring:    oklch(0.50 0.105 195 / 55%);

  /* ── quantitative ordinal ramp (teal, ONE hue) — p50→p99, volume heat ── */
  --q-1: oklch(0.72 0.100 195);  /* #49b8b8 */
  --q-2: oklch(0.58 0.115 195);  /* #1a8b8b */
  --q-3: oklch(0.44 0.100 195);  /* #0e5f5f */

  /* ── status ramp — chart marks (validated categorical, adjacent) ── */
  --st-queued:     oklch(0.550 0.028 255);  /* #677382 slate  — inert   */
  --st-delivering: oklch(0.550 0.131 245);  /* #1677b8 azure  — in-flight */
  --st-delivered:  oklch(0.470 0.150 152);  /* #116d38 green  — terminal ok */
  --st-retrying:   oklch(0.665 0.130 80);   /* #bd891d amber  — degraded */
  --st-failed:     oklch(0.470 0.190 28);   /* #a91414 red    — attempt failed */
  --st-dead:       oklch(0.560 0.200 315);  /* #9f43c3 violet — DLQ */

  /* ── status ink: text on a tinted pill (WCAG text contrast, ≥6:1 on its pill) ── */
  --st-queued-ink:     oklch(0.44 0.028 255);
  --st-delivering-ink: oklch(0.44 0.105 245);
  --st-delivered-ink:  oklch(0.44 0.109 152);
  --st-retrying-ink:   oklch(0.44 0.086 80);
  --st-failed-ink:     oklch(0.44 0.150 28);
  --st-dead-ink:       oklch(0.44 0.150 315);

  /* ── chart chrome ── */
  --grid:     oklch(0.925 0.004 265);  /* #e5e6e9 */
  --axis:     oklch(0.860 0.005 265);  /* #cfd1d4 */

  /* ── geometry ── */
  --radius:    0.375rem;   /* 6px — deliberately tighter than shadcn's 10px */
  --row-h:     2rem;       /* 32px compact, 40px comfortable */
  --gutter:    0.75rem;
}

.dark {
  --bg-canvas:    oklch(0.145 0.007 265);  /* #090a0d */
  --bg-surface:   oklch(0.185 0.007 265);  /* #111316 */
  --bg-inset:     oklch(0.165 0.007 265);  /* #0d0e11 */
  --bg-raised:    oklch(0.225 0.007 265);  /* #1a1c1f */
  --bg-hover:     oklch(0.265 0.007 265);  /* #232529 */

  --line:         oklch(1 0 0 / 9%);
  --line-strong:  oklch(1 0 0 / 15%);
  --line-faint:   oklch(1 0 0 / 5%);

  --fg:      oklch(0.97 0.004 265);  /* #f4f5f8  18.1:1 */
  --fg-2:    oklch(0.78 0.008 265);  /* #b5b7bd   9.8:1 */
  --fg-3:    oklch(0.62 0.010 265);  /* #83868c   5.4:1 */
  --fg-4:    oklch(0.50 0.010 265);  /* #606369   3.3:1 */

  --signal:      oklch(0.80 0.110 195);  /* #57d4d4  11.1:1 */
  --signal-ink:  oklch(0.80 0.110 195);
  --signal-wash: oklch(0.80 0.110 195 / 12%);
  --focus-ring:  oklch(0.80 0.110 195 / 60%);

  --q-1: oklch(0.78 0.100 195);  /* #5ecbcb */
  --q-2: oklch(0.66 0.115 195);  /* #22a6a6 */
  --q-3: oklch(0.54 0.110 195);  /* #177e7e */

  --st-queued:     oklch(0.600 0.030 255);  /* #748192 */
  --st-delivering: oklch(0.620 0.147 245);  /* #1c8dd8 */
  --st-delivered:  oklch(0.520 0.129 152);  /* #157d41 */
  --st-retrying:   oklch(0.665 0.130 80);   /* #bd891d — mode-invariant */
  --st-failed:     oklch(0.575 0.190 28);   /* #d23a31 */
  --st-dead:       oklch(0.610 0.190 315);  /* #ad57d0 */

  --st-queued-ink:     oklch(0.84 0.020 255);
  --st-delivering-ink: oklch(0.84 0.080 245);
  --st-delivered-ink:  oklch(0.84 0.120 152);
  --st-retrying-ink:   oklch(0.84 0.120 80);
  --st-failed-ink:     oklch(0.84 0.083 28);
  --st-dead-ink:       oklch(0.84 0.110 315);

  --grid: oklch(0.26 0.006 265);  /* #232427 */
  --axis: oklch(0.34 0.007 265);  /* #36383c */
}
```

Wired into Tailwind v4 with `@theme inline` in `globals.css`, mirroring mosaic's pattern exactly:

```css
@import "tailwindcss";
@import "tw-animate-css";
@import "./tokens.css";
@custom-variant dark (&:is(.dark *));

@theme inline {
  --color-canvas: var(--bg-canvas);
  --color-surface: var(--bg-surface);
  --color-inset: var(--bg-inset);
  --color-raised: var(--bg-raised);
  --color-fg: var(--fg);
  --color-fg-2: var(--fg-2);
  --color-fg-3: var(--fg-3);
  --color-fg-4: var(--fg-4);
  --color-line: var(--line);
  --color-signal: var(--signal);
  --color-st-queued: var(--st-queued);
  /* … one per status, per ink … */
  --font-sans: "Geist", ui-sans-serif, system-ui, sans-serif;
  --font-mono: "Geist Mono", ui-monospace, SFMono-Regular, monospace;
  --radius-sm: 4px; --radius-md: 5px; --radius-lg: 6px; --radius-xl: 8px;
}
```

**Validation record** (so this is reproducible, not vibes):

| set | mode | worst adjacent CVD ΔE | normal-vision ΔE | contrast |
|---|---|---|---|---|
| delivered/retrying/failed/dead | dark on `#111316` | **8.6** (deutan) | **17.4** | all ≥3:1 |
| delivered/retrying/failed/dead | light on `#ffffff` | **14.9** (protan) | **24.2** | all ≥3:1 |
| teal ordinal q1→q3 | dark | monotone L, ΔL ≥0.06 | — | light-end 3.8:1 |
| teal ordinal q1→q3 | light | monotone L, ΔL ≥0.06 | — | light-end 2.4:1 |

The dark set sits at 8.6 — clear of the ≥8 target but not luxuriously. The 2px surface gap between stacked bands plus the legend plus direct labels are therefore **mandatory secondary encoding**, not optional polish.

**Why violet for `dead`.** DLQ is not "more red than failed" — it's a *different destination*. Making it violet (315°) rather than a darker red does three jobs at once: it reads as "shunted aside" rather than "worse," it survives protan/deutan where two reds collapse, and it gives the canvas's DLQ branch an unmistakable identity at a glance.

`--st-queued` is intentionally near-achromatic (C 0.028), which **fails the categorical chroma floor** — correctly, because it is not a chart series. Queued is never a band in a stacked area; it exists only as a pill and a canvas node. Absence of color *is* the meaning: no work has happened yet. It ships with an icon + label like every status.

### 2.2 Typography

**Geist Sans** for UI, **Geist Mono** for every number, ID, header, method, URL, and code. Self-hosted variable woff2.

Dense scale — base is **13px, not 16px**. This alone reads "instrument" instead of "marketing site."

| token | size / line-height | tracking | use |
|---|---|---|---|
| `--t-micro` | 10 / 12 | +0.08em, uppercase | status pill labels, column heads, axis ticks |
| `--t-xs` | 11 / 15 | +0.01em | table meta, timestamps, help text |
| `--t-sm` | 12 / 17 | 0 | dense table body, secondary nav |
| `--t-base` | 13 / 19 | 0 | body, form controls, primary table body |
| `--t-md` | 14 / 20 | −0.005em | panel titles, section heads |
| `--t-lg` | 16 / 22 | −0.01em | page title |
| `--t-metric` | 20 / 22 | −0.02em, mono | stat-tile values |
| `--t-hero` | 28 / 30 | −0.025em, mono | the single hero number per screen |

**The absolute rule, enforced globally:**

```css
.font-mono, [data-numeric] { font-variant-numeric: tabular-nums slashed-zero; }
```

Every digit that could ever sit above or below another digit is monospace and tabular. Counters that tick don't reflow; columns align on the decimal without hacks. This is the highest ratio of visual sophistication to effort in the entire document, and it is why nothing on screen jitters at 10Hz.

Timestamps render as `2026-07-31 14:22:07.418` in mono, always UTC by default, with a global "UTC / local" toggle in the top bar. Never "3 minutes ago" as the *only* representation — relative time in `--fg-3` beside the absolute, or on hover.

### 2.3 Density, radius, elevation

**Spacing:** 4px base grid. Allowed steps only: 2, 4, 6, 8, 12, 16, 24, 32, 48. No 10, no 14, no 20. Panel padding `12px`; page gutter `16px`; card-to-card gap `8px`.

**Density toggle:** `<html data-density="compact|comfortable">` persisted to localStorage, driving `--row-h: 32px | 40px` and one type-scale step. Compact is the default. Recruiters see 40 rows of data at once instead of 12.

**Radius ramp:** 4 / 5 / 6 / 8. `--radius: 6px`. Nothing in the app is rounder than 8px except avatars. shadcn's 10px default is consumer-app soft; 6px is Linear/Vercel tight. Sparklines, pills and chart marks get 2px rounded data-ends per the marks spec — never fully-round bar caps.

**Elevation without shadows.** Panels never have `box-shadow`. Depth is:

```
level 0  page          bg-canvas
level 1  panel         bg-surface + 1px var(--line)
level 2  hover/active  bg-hover   + 1px var(--line-strong)
level 3  overlay       bg-raised  + 1px var(--line-strong)
                       + inset 0 1px 0 oklch(1 0 0 / 4%)      ← top highlight (dark only)
                       + 0 16px 48px -12px oklch(0 0 0 / 60%)  ← the ONLY shadow in the app
```

Only popovers, dialogs, command palette and toasts get level 3. Everything else is surface-step + hairline. Hairlines everywhere are exactly `1px` (use `border` not `outline`, and `-webkit-font-smoothing: antialiased` on `body` so hairlines don't get eaten by subpixel rendering).

### 2.4 The signature primitives

These six are what people will remember. Build them by hand; do not reach for a shadcn component for any of them.

---

**① `<StatusPill>` — the identity of the product**

Not a Badge. It carries a **shape glyph** so status is never color-alone (this is what satisfies the accessibility rule *and* what makes it look designed):

| status | glyph (6×6 SVG) | meaning it encodes |
|---|---|---|
| queued | hollow ring | waiting, nothing spent |
| delivering | filled dot with a 2px trailing dash | motion |
| delivered | filled dot | done |
| retrying | half-filled ring, notched | partial / cycling |
| failed | ✕ (2 hairline strokes) | attempt lost |
| dead | ✕ inside a ring | terminal, contained |

```tsx
type Status = "queued" | "delivering" | "delivered" | "retrying" | "failed" | "dead";

interface StatusPillProps {
  status: Status;
  /** appends "· 3/5" for retry position */
  attempt?: { n: number; max: number };
  /** "solid" for canvas/legend chips, "soft" (default) for tables */
  tone?: "soft" | "solid" | "bare";
  size?: "xs" | "sm";
  /** true → renders a 1.5s ease-in-out opacity breathe on the glyph only */
  live?: boolean;
}
```

Visual: `h-5` (xs) / `h-6` (sm), `rounded-[4px]`, background `color-mix(in oklch, var(--st-X) 16%, var(--bg-surface))`, 1px border `color-mix(in oklch, var(--st-X) 32%, transparent)`, label in `--st-X-ink` at `--t-micro` uppercase mono. Full cva variant matrix over `status × tone × size`, driven by a `data-status` attribute so tables can style rows off it (`[data-status="dead"] &`).

---

**② `<Num>` — every number in the app goes through this**

```tsx
interface NumProps {
  value: number | null;
  /** "count" | "ms" | "rate" | "bytes" | "pct" */
  format?: NumFormat;
  /** 12,481 → "12.5k". "auto" abbreviates above 10k. */
  abbrev?: boolean | "auto";
  /** shows ▲/▼ + sign, colored by --st-delivered / --st-failed */
  delta?: boolean;
  /** flash the cell background on change; respects reduced-motion */
  flash?: boolean;
  /** aria-label override; defaults to the un-abbreviated spoken form */
  label?: string;
}
```

Rules it enforces: tabular mono; unit suffix rendered in `--fg-4` at 0.85em with `2px` gap; thousands separated by a **U+2009 thin space**, not a comma (mono + thin space is the single most "expensive-looking" typographic move available); `null` renders an em-dash in `--fg-4`, never `0`.

**The flash:** on value change, `background-color` goes to `--signal-wash` and transitions back to transparent over 240ms. **No odometer, no digit slide, no count-up.** Rolling digits in a 10Hz dashboard is motion sickness. A background tint is a color transition, costs no layout, and reads instantly as "this just changed."

---

**③ `<AttemptTimeline>` — the "they actually understand distributed systems" component**

The retry ladder for a single event, horizontal, with **the backoff gaps drawn to scale on a log axis**. You literally see the exponential widening.

```
 ingest        try 1        try 2              try 3                       try 4
   ●───────────▣────────────▣──────────────────▣───────────────────────────▣
   │   +12ms   │ 500  84ms  │ 500  91ms        │ 429  1.2s                 │ 200  73ms
               └── 1s ──────┘└─── 2s ──────────┘└──── 4s ──────────────────┘
                  backoff       backoff              backoff  (+jitter ▨)
```

```tsx
interface AttemptTimelineProps {
  attempts: Attempt[];              // ordered
  /** if delivery is still open, renders a live pulsing terminal node
      and a countdown to next_attempt_at */
  pending?: { nextAttemptAt: string; attemptNo: number };
  /** "log" (default) compresses 1s…6h into readable space; "linear" for ≤3 attempts */
  scale?: "log" | "linear";
  onSelect?(attemptId: string): void;
  selectedId?: string | null;
}
```

Each attempt node: a 14px rounded square filled with the outcome status color, HTTP status code in mono beneath, request duration as a small bar whose width maps to the latency scale. Jitter is drawn as a hatched half-band on the backoff gap — a tiny detail that says "I know backoff has jitter." Selecting a node drives the request/response viewer below it. Keyboard: it's a `role="tablist"` with arrow-key roving tabindex; each node is a tab, the body viewer is its panel. Screen readers get `"Attempt 2 of 5, HTTP 500, 91 milliseconds, retried after 2 seconds"`.

---

**④ `<Sparkline>` / `<Sparkbar>` — hand-rolled SVG, ~60 lines, no library**

72×20 in a table cell. A charting library in a virtualized table row is a performance catastrophe; this is one `<svg>` with one `<path>`.

```tsx
interface SparklineProps {
  points: number[];        // pre-bucketed by the server, fixed length (60)
  /** a second series drawn as a filled band beneath, always --st-failed */
  failures?: number[];
  /** terminal dot at the last point; --signal, 2.5px */
  cap?: boolean;
  /** min/max from a shared scale so rows are comparable — REQUIRED for tables */
  domain?: [number, number];
  ariaLabel: string;       // "throughput last 60 minutes, currently 412 per minute, trend up"
}
```

Non-negotiable: **`domain` is shared across all rows in a table.** Per-row auto-scaling makes a 4/min endpoint look identical to a 4000/min endpoint and is the single most common sparkline lie. Stroke 1.5px, `--q-2`, no fill for the main series; the failure band is `--st-failed` at 35% opacity. `vector-effect="non-scaling-stroke"`.

---

**⑤ `<IdChip>` — copyable identifier**

```tsx
interface IdChipProps {
  id: string;                  // "evt_01J8QK4M9XZ0FQ3B7RTKQ2"
  prefixLen?: number;          // default 8
  suffixLen?: number;          // default 4
  href?: string;               // makes it a router Link
  onCopy?(): void;
}
```

Renders `evt_01J8…KQ2` in mono at `--t-xs`, **middle-ellipsis** (never truncate the tail — the tail is the entropy you visually match against), 1px dotted underline in `--line-strong`. Hover reveals the full ID in a tooltip; a copy icon fades in at 60% width. Click copies + fires a sonner toast. It's a real `<button>` with `aria-label="Copy event ID evt_01J8QK4M9XZ0FQ3B7RTKQ2"`.

---

**⑥ `<LatencyBars>` — percentile triple + histogram strip**

```tsx
interface LatencyBarsProps {
  p50: number; p90: number; p99: number;   // ms
  /** 24 log-spaced buckets, counts */
  histogram?: number[];
  /** the endpoint's timeout, drawn as a vertical hairline + label */
  timeoutMs?: number;
  scale?: "log";   // always log; latency is log-normal
}
```

Three stacked 6px bars on a shared **log** x-axis, colored `--q-1` / `--q-2` / `--q-3` (the ordinal ramp — lighter = faster is the correct ordinal reading), each directly labeled with its value in mono at the bar end. Below them, the histogram as 24 hairline columns in `--q-1` at 40%. The timeout marker is a 1px `--st-failed` vertical rule — seeing p99 creep toward that line is the whole point.

---

### 2.5 Motion rules

**Animates:**
- Canvas particles (on `<canvas>`, outside React entirely).
- Overlay entry/exit: dialog, popover, sheet, command palette — framer-motion, **180ms**, `cubic-bezier(0.16, 1, 0.3, 1)`, opacity + 4px translate + 0.98→1 scale.
- `<Num>` background flash: 240ms color-only.
- Status glyph "breathe" on `delivering`/`retrying` only: 1.5s opacity 1→0.55→1.
- Skeleton shimmer: a 1.2s linear gradient sweep, opacity ≤ 0.06.
- Circuit breaker half-open countdown ring: a stroke-dashoffset sweep.

**Must NOT animate — call this out explicitly in the code:**
- **New rows in any live feed.** No slide-in, no height animation, no stagger, no `AnimatePresence` on list items. New rows appear instantly. Recency is signalled by a **recency wash**: a 2px left border in the row's status color at 100% opacity that fades to 0% over 600ms. Color-only, zero layout, and it survives 40 rows/sec appearing at once without inducing nausea.
- Numbers counting up.
- Charts tweening between datasets (uPlot redraws; no transition).
- Table sort/filter reordering.
- Anything on scroll.

**Reduced motion** — one global switch, not per-component:

```tsx
<MotionConfig reducedMotion="user">   // framer-motion honors prefers-reduced-motion
```

plus a `useReducedMotion()` read in the canvas engine that switches particle rendering to a **static flow-density gradient** along each edge (thickness + opacity proportional to rate, no movement) and disables the recency wash and the status breathe. The dashboard remains fully readable with zero moving pixels — which is also the correct state for the screenshot in the README.

---

## 3. The live flow canvas

### 3.1 Architecture: React Flow for chrome, one `<canvas>` for particles

**`@xyflow/react` renders the graph. It never renders a single particle.**

React Flow gives you, for free and already familiar from mosaic: pan/zoom with a managed viewport transform, node positioning, edge path geometry, minimap, keyboard-accessible nodes, and — critically — nodes as ordinary React components, so the live counters inside a node are just React subscribing to the store.

Particles go on an absolutely-positioned `<canvas>` mounted **inside** the React Flow `.react-flow__viewport` container's parent, with `pointer-events: none`, `z-index` above edges and below nodes. Each frame the engine reads the viewport transform via `useStore(s => s.transform)` and applies `ctx.setTransform(zoom, 0, 0, zoom, x, y)` — so particles pan and zoom in perfect lockstep with the graph, for free.

Edge geometry is stolen once and cached:

```ts
// on nodes/edges change, not per frame
const [d] = getBezierPath({ sourceX, sourceY, targetX, targetY, ... });
const path = new Path2D(d);
const svgPath = document.createElementNS(SVG_NS, "path");
svgPath.setAttribute("d", d);
const len = svgPath.getTotalLength();
// sample 64 points once → Float32Array lookup table
const lut = new Float32Array(64 * 2);
for (let i = 0; i < 64; i++) { const p = svgPath.getPointAtLength((i / 63) * len); lut[i*2] = p.x; lut[i*2+1] = p.y; }
```

Per frame you do a linear interpolation in the LUT, not a `getPointAtLength` call. 2,000 particles cost ~0.4ms.

Particle state is a **struct-of-arrays** in `Float32Array`s (`x`, `y`, `t`, `edgeIdx`, `statusIdx`, `weight`), with a free-list for recycling. Zero allocation in the hot loop, zero GC pauses, so the 60fps is real rather than aspirational.

### 3.2 The honesty problem: sampling at 1,000+ events/sec

At 1,000 events/sec with a ~2s traversal you'd need 2,000 simultaneous particles per edge and the eye would see an undifferentiated smear. That is both slow *and* dishonest — a smear communicates nothing.

**The rule: the particle count is a sample; the number is the truth.**

Per edge, per 100ms server frame, the client emits:

```ts
const CAP_PER_EDGE_PER_SEC = 12;              // ~1.2 particles per 100ms frame
const n = frame.edge[i].count;                 // real events in this 100ms window
const emit = Math.min(n, CAP_PER_EDGE_PER_SEC / 10);
const weight = n / Math.max(emit, 1);          // events represented per particle
```

Fractional emission uses a **token bucket** so 0.3 particles/frame becomes one particle every ~3 frames rather than being floored to zero — low-rate edges must not go visually dead. A stratified sample by status (proportional allocation across `delivered`/`retrying`/`failed`/`dead`) guarantees a rare 0.5% failure rate still produces a visible red particle roughly every 2 seconds instead of being rounded away. **A single failure at low volume is always emitted, never sampled out** — that's the event a user actually cares about.

`weight` is encoded, visibly and legibly:
- radius `2px + log2(weight) * 0.6`, clamped to 5px
- a faint 1px trailing streak whose length grows with weight
- and — this is the honest part — **every edge carries a permanent mono label with the real rate** (`1,240/s`) plus, when `weight > 1`, a `~1:100` sampling ratio chip in `--fg-4`.

Above ~200/s on an edge, particles cross-fade into a **flow-density mode**: the edge stroke thickens (2→6px), gains a scrolling dash pattern at fixed 60px/s (speed encodes *nothing* — it's texture, and the label says so), and 3–4 status-colored particles ride on top as "flavor" showing the *mix*. This is the same visual grammar Datadog and Kiali use for high-throughput service graphs and it's the only honest option: at that rate you're showing a distribution, not individuals.

Never encode rate as particle *speed*. Speed maps to latency and nothing else: traversal time = actual delivery latency, log-compressed into a 400ms–4s window. So a slow endpoint's particles visibly crawl. That is the single most legible thing on the screen during the "Slow consumer" demo scenario.

Budget: hard cap of **1,200 live particles**; over budget, halve every edge's cap and mark the canvas `SAMPLED` in the corner. Frame loop is a single `requestAnimationFrame` with a fixed 16.67ms accumulator; if a frame takes >20ms three times running, drop to 30fps rendering (physics stays at 60) and set the badge.

### 3.3 Node design

Six node types, all custom React Flow node components on `bg-surface` + hairline, `rounded-md`, 148–180px wide. Each has: a 10px uppercase mono type label in `--fg-4`, a hero number via `<Num>`, a secondary metric, and a 40×14 `<Sparkline>` footer.

| node | hero | secondary | live state |
|---|---|---|---|
| **INGEST** | `accepted/s` | `p50 ack latency`, `4xx rejects` | pulses `--signal` on rate spike |
| **QUEUE** | `depth` | `oldest age`, `enqueue/s` | a horizontal fill bar; ≥80% capacity → bar turns `--st-retrying`, ≥95% → `--st-failed` + `BACKPRESSURE` pill |
| **WORKERS** | `busy / total` | `in-flight`, `dispatch/s` | a **worker strip**: N 4×10px cells, one per worker, filled = busy, in `--st-delivering`. At 64+ workers it collapses to a utilization bar. This strip is the clearest picture of a bounded worker pool anyone will ever put in a portfolio. |
| **ENDPOINT** (one per, up to 6 shown + "N more") | `success %` | `p95 ms`, `attempts/s` | border color = health; **circuit open → the whole node desaturates to 40%, its inbound edge becomes a 4px dashed `--st-failed` line, and a countdown ring shows time to half-open** |
| **RETRY BUFFER** | `scheduled` | `next in 4.2s` | sits *above* the worker→endpoint edge as a loop-back |
| **DLQ** | `dead total` | `+N last 5m` | see below |

Layout is a fixed, hand-authored left-to-right dagre-free layout (positions are constants; the topology never changes). `nodesDraggable={false}`, `zoomOnScroll` with `panOnScroll` off, `fitView` on mount and on endpoint-count change.

### 3.4 How the DLQ branch reads

This is the storytelling moment, so it gets deliberate geometry:

- The DLQ node sits **below the main axis and offset right**, on its own baseline — visually "beneath the floor" of the happy path. The main ingest→queue→worker→endpoint spine is a straight horizontal line at y=0; DLQ is the only thing that breaks it.
- The retry loop-back (worker → retry buffer → worker) is drawn as an arc **above** the spine in `--st-retrying`. Retries go up and come back. Dead letters go down and stay.
- The failure edge leaves the worker node at 45° downward, in `--st-failed`, and **changes color to `--st-dead` at the midpoint** via a canvas gradient along the path — a literal picture of "an attempt that failed becomes an event that died."
- The DLQ node's inbound edge has **no return path**. Every other edge in the graph is part of a cycle or a forward chain; this one terminates. The absence is the message.
- When an event lands in DLQ, its particle **stops and accretes**: it settles into a small stack of dots inside the node (max 40 rendered, then a count). It doesn't vanish. Things in the DLQ are still there, and the picture should say so.
- The DLQ node is the only node with a button in it: `Replay N →`, which draws a temporary `--st-delivered` edge from DLQ back to the queue when clicked, and particles flow *back up*. Watching the graph heal is the payoff shot of the demo.

Accessibility for the canvas: `role="img"` with an `aria-label` regenerated at ≤1Hz summarizing the pipeline ("Ingest 412 per second. Queue depth 38. 12 of 32 workers busy. 4 endpoints, 1 circuit open. Dead-letter queue 118."), plus a visually-hidden `<table>` mirroring every node's counters, plus a `Tab`-reachable list of nodes where `Enter` opens that node's detail panel. An `aria-live="polite"` region announces **only threshold crossings** (circuit opened, backpressure engaged, DLQ crossed a multiple of 50) — never individual events.

---

## 4. Realtime transport

### 4.1 Envelope

Single WS at `wss://host/ws` (same origin, cookie-authed). All frames JSON; binary reserved as a later optimization. Versioned envelope, short keys because these go out at 10Hz.

```ts
// ── shared ────────────────────────────────────────────────────────────────
export type Status = "queued" | "delivering" | "delivered" | "retrying" | "failed" | "dead";

/** Server→client. `v` bumps only on breaking change; server refuses mismatched hello. */
export type ServerFrame =
  | Hello | Frame | Delta | Reset | Pong | ErrorFrame;

export interface Hello {
  t: "hello"; v: 1;
  epoch: string;      // server generation; changes on restart/failover
  seq: number;        // frame sequence this connection starts at
  hz: number;         // current frame rate the server will send (10 | 5 | 2)
  serverTime: number; // ms epoch — client computes clock skew ONCE here
}

/** The 10Hz aggregate. Lossy by design: dropping one is always safe. */
export interface Frame {
  t: "f"; seq: number; ts: number;      // window END, ms epoch
  win: number;                           // window width ms (100)
  /** counts per edge per status, in a fixed [edge][status] order.
   *  Flat array so a frame is ~200 bytes, not 4KB of JSON objects. */
  flow: number[];
  /** node gauges, fixed order: [accepted, qDepth, qOldestMs, busy, total,
   *  inflight, dispatched, dlqTotal, retryScheduled] */
  nodes: number[];
  /** per-endpoint rollup, only endpoints in the subscription */
  ep: Array<[epId: string, ok: number, retry: number, fail: number, p50: number, p95: number, breaker: 0|1|2]>;
  /** sampled event summaries for the live feed; NOT every event */
  ev?: EventTick[];
  /** events the server sampled away in this window (feed honesty) */
  evDropped?: number;
  /** frames the server dropped for THIS connection due to backpressure */
  lag?: number;
}

export interface EventTick {
  id: string; epId: string; type: string;
  st: Status; att: number; code: number | null; ms: number | null; ts: number;
}

/** Lossless, entity-level. Crosses into the TanStack Query cache. Never dropped. */
export interface Delta {
  t: "d"; seq: number;
  k: "event" | "endpoint" | "dlq" | "key";
  id: string;
  rev: number;                 // monotonic per entity — the reconciliation key
  patch: Record<string, unknown>;
}

/** Sequence gap the server can't backfill. Client must resync from REST. */
export interface Reset { t: "reset"; reason: "gap" | "epoch" | "resub"; }
export interface Pong  { t: "pong"; seq: number; rtt?: number; }
export interface ErrorFrame { t: "err"; code: string; msg: string; fatal: boolean; }

// ── client→server ─────────────────────────────────────────────────────────
export type ClientFrame = Subscribe | Unsubscribe | Ack | Ping | SetRate;

export interface Subscribe {
  t: "sub";
  ch: Channel[];
  /** resume attempt; server replays lossless deltas from its 10s ring if it can */
  resume?: { epoch: string; seq: number };
}
export interface Unsubscribe { t: "unsub"; ch: Channel[]; }

/** Flow-control: client reports the last seq it has PAINTED, not received. */
export interface Ack { t: "ack"; seq: number; }
export interface Ping { t: "ping"; ts: number; }
/** Client asks to be throttled — e.g. tab hidden → hz 1 */
export interface SetRate { t: "rate"; hz: 10 | 5 | 2 | 1 | 0; }

// ── channels: the filtering contract ──────────────────────────────────────
export type Channel =
  | `flow:${string}`                       // flow:proj_x       — canvas aggregate
  | `feed:${string}`                       // feed:proj_x       — sampled live feed
  | `feed:${string}:ep:${string}`          // one endpoint only
  | `feed:${string}:st:${Status}`          // one status only
  | `event:${string}`                      // one event's attempts (detail page)
  | `endpoint:${string}`                   // one endpoint's health/breaker
  | `dlq:${string}`
  | `demo:${string}`;                      // scenario narration ticks
```

### 4.2 Subscription and filtering — server-side, always

A user watching one endpoint must never receive the firehose. Filtering happens in the Go hub at fan-out time, not in the browser:

- Each connection holds a `map[Channel]struct{}`. The hub's per-project aggregator computes one canonical 100ms window, then **projects** it per subscription: `flow:proj` gets the full flow array; `feed:proj:ep:ep_9` gets only that endpoint's slice, and its `nodes`/`ep` arrays are filtered to that endpoint's row.
- Aggregation is done **once per project per window**, not once per connection. Projection is a cheap slice. 500 connections on one project = 1 aggregation + 500 projections.
- Channel subscriptions are re-derived declaratively from the mounted route by a single `useChannels(chans: Channel[])` hook that diffs against the connection's current set and sends one `sub`/`unsub` pair. Navigating from `/events` to `/endpoints/ep_9` swaps channels without touching the socket.
- Authorization is checked at `sub` time against the session's project membership, and the resulting allowed set is cached on the connection. Never trust a channel string.

### 4.3 Sampling and batching — coalesce to 10Hz frames

The hub **never** pushes per-event. A per-project `Aggregator` runs a 100ms ticker:

1. Dispatcher goroutines write outcomes into a lock-free per-shard ring (`atomic` counters per `[edge][status]` bucket).
2. On tick, the aggregator sums shards, snapshots node gauges, and drains an **event reservoir** — a fixed 24-slot Vitter reservoir sample per window, with a *priority override* that unconditionally admits any event whose status is `failed` or `dead`, and any event matching the connection's active filter. Non-terminal `delivered` events at high volume are the ones sampled away, and `evDropped` reports exactly how many.
3. One `Frame` is marshalled once per projection shape and `[]byte` is shared across connections. Marshal cost is O(distinct projections), not O(connections).

Feed honesty in the UI: when `evDropped > 0`, the live feed header shows `SAMPLED · showing 24 of 1,240/s` in `--st-retrying` mono. The Events *table* (REST, cursor-paginated) is always complete; the *feed* is explicitly a sample. Say so on screen.

### 4.4 Backpressure

Three layers, each doing one job:

**Server → socket.** Per-connection buffered send channel, cap 64. On full:
- Drop the **oldest** `Frame` in the buffer (snapshots supersede; dropping is lossless in meaning) and increment `lag`.
- **Never** drop a `Delta`, `Reset`, or `ErrorFrame` — if the buffer is full of those, close with code `1011` and let the client reconnect+resync.
- If `lag > 20` sustained over 2s, the hub **halves this connection's rate**: 10Hz → 5Hz → 2Hz, and sends a `Hello`-style rate note. Adaptive degradation beats a stalled socket.

**Client → server.** The client `Ack`s the last seq it *painted* (not received) every 10 frames. If the hub sees ack-lag > 2s, it applies the same rate halving. This is the honest signal: a browser on a busy laptop reports through rAF, not through the network stack.

**Client-internal.** The `StreamStore` drains the WS message queue in the rAF callback, not in `onmessage`. `onmessage` does exactly two things: `JSON.parse` and push into a bounded array (cap 30). If that array is full, drop the oldest `Frame`. So even if the browser tab is descheduled for 500ms and 50 frames arrive at once, the next paint processes at most the newest few and the UI catches up instantly rather than replaying history.

Tab visibility: `document.visibilitychange` → hidden sends `{t:"rate", hz:1}`; visible sends `hz:10` plus a full `queryClient.invalidateQueries()` for entity data. A backgrounded tab costs the server ~nothing.

### 4.5 Reconnect and resume

```
attempt n → delay = min(30_000, 500 * 2**n) * (0.5 + Math.random())   // full jitter
```

Full jitter (not equal jitter, not plain exponential) because 500 dashboards reconnecting after a hub rollout is a thundering herd, and this is exactly the class of bug the project is about. Reset `n` to 0 only after the socket has been open **5 continuous seconds** — otherwise a flapping socket hammers at 500ms forever.

Resume: on reconnect the client sends `sub` with `resume: { epoch, seq }` from the last frame it painted.
- Same `epoch` and `seq` within the hub's 10s lossless ring → the hub replays missed `Delta`s in order, then resumes frames. UI shows a brief `RESYNCED` chip.
- Different `epoch` (hub restarted / different pod), or `seq` too old → `{t:"reset"}`. Client calls `queryClient.invalidateQueries()`, clears every ring buffer, clears the canvas particle pool, and refetches. **Frames are never backfilled** — they're a live sample and stale ones are worse than none.

Connection status is a first-class UI element in the top bar: a `<StatusPill>`-styled chip showing `LIVE 10Hz` / `LIVE 2Hz · THROTTLED` / `RECONNECTING 3s` / `OFFLINE`, with the measured RTT in mono. Never hide the transport state from the user in a realtime product.

### 4.6 Reconciling WS deltas against REST state

**Every entity carries a monotonically increasing `rev` from Postgres.** That is the whole reconciliation protocol, and it removes every "did the WS or the fetch win?" race:

```ts
function applyDelta(qc: QueryClient, d: Delta) {
  if (d.k === "event") {
    qc.setQueryData<EventDetail>(["event", d.id], (prev) =>
      !prev || d.rev <= prev.rev ? prev : { ...prev, ...d.patch, rev: d.rev }
    );
    // list pages: patch in place if present; never reorder, never insert
    qc.setQueriesData<InfiniteData<EventPage>>({ queryKey: ["events"] }, (prev) =>
      prev && patchEventInPages(prev, d.id, d.patch, d.rev)
    );
  }
  // ...
}
```

Three rules that make this safe:

1. **`rev` guard on every write.** A REST response with a newer `rev` beats a stale delta and vice versa. No timestamps, no clock trust.
2. **Deltas patch, never insert or reorder.** A WS delta for an event not currently in a list page is ignored — that page's cursor window doesn't include it, and inserting would corrupt keyset pagination. New events reach the list only through the head-buffer path (§5.2) or a refetch.
3. **Aggregate frames never touch the query cache.** The canvas and the stat tiles read the `StreamStore`. The Events table reads TanStack Query. They are separately sourced, and the stat tiles show a `LIVE` chip while the table shows `as of 14:22:07` — different provenance, honestly labeled.

---

## 5. Screens

Shell: fixed 200px left rail (collapsible to 48px icon rail, `react-resizable-panels`, persisted), 44px top bar (project switcher · global time range · UTC/local · connection chip · density toggle · theme · `⌘K`), content region. Detail views open in a **right side panel** (`Sheet`, 520px, resizable) with a "open full page" affordance — so you never lose your place in a live list. Deep links work either way.

### 5.1 `/` — Mission Control

Top: a 5-tile stat row (`<Num>` heroes, `<Sparkline>` footers, all `LIVE`): **Accepted/s · Delivered/s · Success rate 5m · p95 delivery latency · DLQ depth**. Each tile is a link that filters the Events list.

Center, occupying ~55% of viewport height: **the live flow canvas**. This is the first thing on screen. No hero copy, no onboarding card.

Below, three panels on one row:
- **Throughput** (uPlot, single teal series, stepped, last 60m/24h per global range)
- **Outcomes** (uPlot stacked area, the four status colors, absolute/100% toggle)
- **Delivery latency** (uPlot, p50/p90/p99 in the teal ordinal ramp, log y)

Right rail (collapsible, 320px): **Live feed** — the sampled event stream, one 24px row per event: time · `<StatusPill>` · endpoint name · event type · code · latency. Auto-scrolls; **pauses the instant you hover or scroll up**, showing `↑ 214 new` as a click-to-resume chip.

Bottom strip: **Endpoint health grid** — one 96×56 tile per endpoint, success% + sparkline + breaker state, sorted by worst-first.

### 5.2 `/events` — the high-cardinality list

The hardest screen. Design:

- **Virtualized table** via `@tanstack/react-virtual` (`overscan: 12`, fixed `--row-h`, `getScrollElement` on the panel body). Fixed row height is a hard requirement — variable heights make the scrollbar lie during infinite load.
- **Keyset cursor pagination**, `useInfiniteQuery`, page size 100, cursor = base64 of `(occurred_at, id)`. Never `OFFSET` — page 400 of an offset scan is where webhook dashboards die.
- **Filter bar** as typed facet chips, not a form: `status:` (multi, StatusPill options) · `endpoint:` (combobox) · `type:` (combobox, from the project's observed event types) · `code:` (range or exact) · `after:`/`before:` · free-text `q` over payload keys. Filters live in the URL (`useSearchParams`) so every view is shareable — a filtered link in a Slack message is the killer feature of every good ops tool. Debounce 250ms on text, immediate on chips.
- **Columns:** recency-wash strip · time (mono, UTC) · `<StatusPill>` (with attempt `2/5`) · `<IdChip>` event id · event type · endpoint · HTTP code (mono, tinted) · latency (mono, right-aligned) · attempts · actions (`⋯` → replay / copy / open).
- **Live-at-head semantics.** The list streams only when scrollTop === 0 *and* no cursor is pinned. Incoming events land in a ref-held head buffer, flushed at **4Hz max, capped at 200 rows**, prepended. Scroll down by one row and the head freezes with a `↑ N new` chip. This single rule kills the entire class of "the row I was about to click moved."
- Row click → side panel. `j`/`k` move selection, `Enter` opens, `x` toggles select, `⇧+click` range-select, `r` replays selection.

### 5.3 `/events/:id` — Event detail

- Header: `<IdChip>`, `<StatusPill>`, event type, endpoint link, occurred-at, idempotency key, `Replay` button.
- **`<AttemptTimeline>`** across the full width. The centerpiece.
- Selected attempt drives a two-pane request/response viewer (`react-resizable-panels`, horizontal): **Request** (method, resolved URL, headers table with the `Ithaca-Signature`/`Ithaca-Timestamp` rows highlighted, JSON body) / **Response** (status, headers, body, or the error class if the attempt never got one — `dial tcp: i/o timeout`, `x509: certificate expired`, `context deadline exceeded`).
- JSON viewer: hand-built collapsible tree (~200 lines). Keys in `--signal-ink`, strings `--fg-2`, numbers/booleans/null in mono `--fg`. Copy-path on any node. Bodies over 64KB render the first 64KB with a `Download raw` action — never `JSON.parse` an unbounded payload on the main thread.
- **Signature verification panel**: shows the exact signed string, the computed HMAC, and a copy-pasteable verification snippet in Go/Node/Python. This is the detail that makes a backend engineer trust the product.
- Right column: metadata (source IP, ingest node, worker id, queue wait ms, `rev`), and a "Related" list of same-idempotency-key events.
- Subscribes `event:{id}`, so an in-flight retry updates the timeline live.

### 5.4 `/endpoints` and `/endpoints/:id`

**List:** URL · description · `<StatusPill>` health · success% (7d) · `<Sparkline>` throughput with a shared domain and a failure band · p95 · breaker state · subscribed event types (chips) · enabled toggle.

**Detail** — four tabs:
- **Overview:** `<LatencyBars>` with the timeout marker, outcomes stacked area, success-rate line with an SLO threshold rule, recent deliveries table.
- **Health / Circuit:** an explicit **state machine diagram** — `CLOSED → OPEN → HALF-OPEN` — with the current state lit, the trip threshold (`5 failures in 30s`), the cooldown countdown ring, live consecutive-failure count, and a transition log. Manual `Trip` / `Reset` buttons (which is how the demo forces the state without lying about it).
- **Configuration:** URL, timeout, max attempts, backoff policy (base, factor, max, jitter) with a **live preview of the resulting retry schedule** rendered as an `<AttemptTimeline>` in a "what-if" mode — you edit `factor: 2 → 3` and watch the ladder stretch. Event-type subscription filters. Custom headers.
- **Secrets:** current key (masked, `whsec_…`), created-at, last-rotated. **Rotation is a two-phase flow with a UI that teaches it**: `Generate new secret` creates a second active key, both verify for an overlap window (a countdown bar), then `Retire old`. Showing that dual-key overlap window is the kind of thing most portfolio projects get wrong.

### 5.5 `/dlq` — Dead letter queue

- Same virtualized table, filtered to `dead`, plus a **failure-reason grouping bar** at the top: horizontal bars by terminal cause (`connection refused`, `HTTP 500`, `timeout`, `HTTP 410`), each click-to-filter. Bars in `--st-dead` at graded opacity — one hue, ordinal by count.
- **Selection model:** checkbox column, header tri-state, `⇧`-range, and a `Select all N matching filter` escape hatch (selects a *query*, not 10,000 ids — the bulk endpoint takes the filter, not an id list).
- **Sticky bulk bar** at the bottom when selection is non-empty: `418 selected · Replay · Replay to different endpoint · Discard · Export NDJSON`.
- Replay opens a confirm dialog stating the real consequences: target endpoint, estimated rate, whether the receiver is currently healthy (with a live warning if its breaker is open — *"ep_9's circuit is OPEN; these will fail again"*), and a rate-limit selector so you can't self-DDoS.
- **Optimistic UI:** on confirm, selected rows immediately flip to `queued` with a `REPLAYING` chip and a per-row spinner; a toast shows `Replaying 418 · 96 done` driven by a `dlq:{project}` progress delta. Server rejection rolls each row back with an inline error and a persistent (non-auto-dismiss) toast.

### 5.6 `/playground` — Endpoint testing

Left: a request composer — endpoint selector, event type, payload editor (a plain `<textarea>` with a mono font, JSON validation on blur, and a set of realistic templates: `payment.succeeded`, `user.created`, `subscription.canceled`), header overrides, and a "send N copies" stepper.

Right: **the flow canvas, scoped to just this endpoint**, plus a live `<AttemptTimeline>` that builds itself in real time as the attempt happens. Below: the exact request Ithaca sent (copyable as `curl`), and the response.

`Send` → the event traverses the canvas → the timeline fills in → the response lands. One button, and the entire pipeline is legible in four seconds.

### 5.7 `/settings`

Tabs: **Project** (name, slug, retention, default retry policy, timezone) · **API keys** (create → one-time reveal in a dialog with an explicit "you will not see this again" and a copy button; list with prefix, last-4, scopes, created, last-used, revoke) · **Members** · **Sinks** (the demo receivers, see below) · **Appearance** (theme, density, UTC/local, "reduce motion" override independent of OS).

### 5.8 `/demo` — the Demo Console (the money shot)

A dedicated route, linked from an unmissable `▶ Run demo` button in the top bar. It exists to take a recruiter from "I don't know what a DLQ is" to "oh, that's what backpressure looks like" in ninety seconds.

**Layout:** the flow canvas at full bleed across the top 60%. Left rail (300px) = controls. Right rail (320px) = the narration log. Bottom = the live feed.

**Controls — receivers.** Ithaca hosts its own sink endpoints at `https://<host>/sink/{id}`, so nothing external is needed and nothing is faked. Per sink:

```
Sink A  ●──────○  latency        [====|--------]  120 ms
        response  [ 200 ▾ ] [ 500 ] [ 429 +Retry-After ] [ timeout ] [ RST ]
        failure rate                [========|----]  60 %
```

**Controls — traffic.** `Send [ 1 | 100 | 1,000 | 10,000 ]` at `[ ──|── ] 200 /s`, plus a `Sustain` toggle for continuous load.

**Scenarios** — five one-click buttons, each a server-side script (`POST /api/v1/demo/scenario { name }`). The Go backend drives every step; the UI only watches, and this is stated on screen (`scenario driven server-side`):

1. **Happy path** — 500 events at 200/s, all 200 OK. Establishes the visual baseline: green particles, queue depth near zero, workers cycling.
2. **Endpoint goes down** — starts healthy, flips Sink A to `500` at t+4s. The canvas turns amber as the retry arc lights up, the retry buffer counter climbs, attempts ladder 1→2→3, then red particles arc *down* to the DLQ and go violet. This is the whole product in fifteen seconds.
3. **Slow consumer** — Sink B latency → 3,000ms. Particles to B visibly crawl. Worker strip fills solid. Queue depth climbs, its bar goes amber then red, `BACKPRESSURE` pill appears on the queue node, and ingest's ack latency stays flat — the demonstration that ingest is decoupled from delivery.
4. **Circuit breaker** — sustained failures on Sink A. Consecutive-failure count ticks up, the breaker trips, the endpoint node desaturates, its edge goes dashed, and the cooldown ring counts down. Sink A is repaired mid-cooldown; at half-open a **single** probe particle is dispatched, succeeds, and the node snaps back to full color. Watching one probe decide the fate of a whole endpoint is the most legible explanation of a circuit breaker anyone has ever put on a screen.
5. **Recovery + replay** — repair all sinks, jump to `/dlq`, select all, replay. Particles flow *backward* from DLQ to the queue and then forward to green. The graph heals.

**Narration rail.** Server-emitted, timestamped, plain-English captions on the `demo:{project}` channel — because the backend knows when things actually happened and the frontend must not guess:

```
t+0.0s   Accepting 200 events/sec · 4 endpoints healthy
t+4.0s   Sink A now returning HTTP 500
t+4.3s   ⚠ ep_a attempt 1 failed — retrying in 1s (base 1s, ×2, ±20% jitter)
t+7.6s   ⚠ ep_a attempt 3 failed — 2 more attempts remain
t+9.1s   ✕ 47 events exhausted all attempts → dead-letter queue
t+11.0s  ⛔ Circuit breaker OPEN for ep_a — 5 failures in 30s. Cooldown 30s.
t+11.0s  ✓ Queue depth stable at 12 — ingest never slowed down
```

Each line is a real `Delta`. The last one is the point of the entire project, stated in words.

**Reset:** `Purge demo data` wipes the demo project and resets every sink. Rate-limited server-side, and the demo project is hard-capped (10k events, 5 min max scenario) so an open tab can't run up a cloud bill.

---

## 6. Data visualization

Prescriptive, since there's zero prior charting experience. **Three libraries' worth of chart types; one library.**

### 6.1 The picks

| chart | build with | why |
|---|---|---|
| Throughput timeseries | **uPlot** | canvas; 100k points at 60fps; `setData()` updates without touching React |
| Latency percentiles | **uPlot** (3 series, log y) | same instance shape, different config |
| Success/failure stacked area | **uPlot** (stacked, manual cumsum) | same |
| Per-row sparklines | **hand-rolled SVG** (`<Sparkline>`) | a chart library instance per virtualized row is a perf disaster |
| Latency percentile bars + histogram | **hand-rolled SVG/CSS** (`<LatencyBars>`) | it's four rectangles; a library adds 200KB to draw four rectangles |
| Attempt timeline | **hand-rolled SVG** (`<AttemptTimeline>`) | not a chart — a layout with a log-scaled axis |
| DLQ reason bars | **hand-rolled CSS** | flex + width% |

**Why uPlot over recharts.** Recharts is SVG and re-renders through React on every data change — at 1Hz with 1,440 points it drops frames, and above ~2k points it's unusable. It also has a strong default look, which is precisely the thing this project must avoid. uPlot is 45KB, canvas, and `u.setData(d)` bypasses React entirely — the same discipline as the WS ring buffers, applied to pixels. Cost: an imperative API and no React bindings.

**That cost is paid exactly once.** Build one wrapper, `src/components/charts/TimeSeries.tsx`, that owns every line of uPlot knowledge in the codebase:

```tsx
export interface TimeSeriesProps {
  /** uPlot's aligned-data shape: [timestamps, ...series]. Typed arrays. */
  data: uPlot.AlignedData;
  series: Array<{
    label: string;
    /** token name, resolved from CSS vars at draw time so theme swaps work */
    color: "q-1" | "q-2" | "q-3" | `st-${Status}` | "signal";
    kind: "line" | "area" | "bar" | "stacked";
    fmt: NumFormat;
  }>;
  yScale?: "linear" | "log";
  /** absolute vs 100% for stacked */
  normalize?: boolean;
  /** cursor sync group — all charts on a page share one crosshair */
  syncKey?: string;
  height?: number;
  /** annotation rules: SLO thresholds, deploy markers, breaker trips */
  markers?: Array<{ x?: number; y?: number; label: string; color: string; }>;
  emptyHint?: string;
}
```

Everything else in the app is a config object. `useEffect` creates the instance once; a second `useEffect` calls `setData` on data change; a `ResizeObserver` calls `setSize`; a `MutationObserver` on `<html class>` re-reads CSS vars and calls `redraw()` on theme change. `useRef` for the instance, never state.

Colors are read from CSS custom properties via `getComputedStyle(root).getPropertyValue('--st-failed')` at draw time, so light/dark is one source of truth and the tokens file stays canonical.

### 6.2 Chart-by-chart specification

**Throughput** — one series → **no legend** (the title names it). `--q-2` stroke at 2px, fill from `--q-2` at 12% to transparent. Stepped path (`uPlot.paths.stepped({align:1})`) because these are per-bucket counts, not a continuous signal — a smooth curve through discrete counts is a lie. y starts at 0, always. Recessive grid (`--grid`, 1px), no vertical gridlines. Crosshair + tooltip on hover.

**Latency percentiles** — three series, so **legend required**, plus direct labels on the right edge of each line (3 ≤ 4, so both). This is an **ordinal** encoding — p50 < p90 < p99 is an order — so it takes the one-hue teal ramp light→dark: `--q-1` p50, `--q-2` p90, `--q-3` p99. Never three categorical hues; the reader should see the ordering in the color. **Log y** (latency is log-normal; a linear axis with a 4s p99 flattens p50 to the baseline). A `--st-failed` horizontal rule at the endpoint's configured timeout. Validated as `--ordinal`: monotone L, ΔL ≥ 0.06, light end 3.8:1 dark / 2.4:1 light.

**Success/failure stacked area** — four bands, bottom-to-top **delivered → retrying → failed → dead**, in exactly the validated status order. The dark set's worst adjacent CVD ΔE is 8.6, so the mandated secondary encoding ships: a **2px stroke in the surface color between every band**, a legend, and direct labels on any band wider than 24px. Toggle absolute ↔ 100%. In 100% mode, keep a thin absolute-volume strip beneath — otherwise a 100% failure rate on 3 events looks identical to one on 30,000, which is the classic stacked-percentage lie.

**Sparklines** — see §2.4④. Shared domain across rows, always.

**Attempt timeline** — see §2.4③.

### 6.3 Color rules, held constant everywhere

1. **One status = one color, in every chart, table, pill, canvas particle and legend, forever.** `--st-failed` red never appears as "series 2" anywhere.
2. **Teal is the neutral quantitative channel** (throughput, latency, volume heat). Status hues are outcome. Nothing else in the product is colored.
3. **Ithaca ships zero categorical palettes.** There is no "compare 8 endpoints on one chart." Endpoint comparison is done with **small multiples** — one sparkline per table row, all in `--q-2`, on a shared domain, ranked by value. This sidesteps the categorical series cap entirely (the validator can't get 8 hues past the all-pairs floor and no ordering can), and small multiples are the better chart anyway.
4. **Text never wears a series color.** Values, axis labels and legend text stay in `--fg` / `--fg-2` / `--fg-3`. A colored 8px mark sits beside them and carries identity.
5. **≥2 series → legend, always.** 1 series → no legend box; the title names it.
6. **Status is never color-alone.** Glyph + text label on every pill, legend entry, and canvas node.
7. Light-mode `--st-retrying` (`#bd891d`) sits at 3.1:1 on white — right at the line. It always ships with a visible label, which is the relief channel, and it's mode-invariant so the two themes agree on what "amber" means.
8. Every chart has a **table view** toggle (`⌘⇧T`) rendering the same data as a real `<table>`. This is the accessibility escape hatch and it costs about 40 lines.

---

## 7. Performance and correctness

### 7.1 The StreamStore — no React state in the hot path

```ts
class StreamStore {
  // ring buffers, preallocated, never reallocated
  private flow = new Float32Array(EDGES * STATUSES * WINDOW);
  private nodes = new Float32Array(NODE_METRICS * WINDOW);
  private feed = new RingBuffer<EventTick>(500);
  private head = 0;
  private listeners = new Set<() => void>();
  private dirty = false;
  private rafId = 0;

  /** called from ws.onmessage — parse + write only, never notify */
  ingest(frame: Frame) {
    writeFlow(this.flow, this.head, frame.flow);
    writeNodes(this.nodes, this.head, frame.nodes);
    frame.ev?.forEach((e) => this.feed.push(e));
    this.head = (this.head + 1) % WINDOW;
    if (!this.dirty) { this.dirty = true; this.rafId = requestAnimationFrame(this.flush); }
  }

  private flush = () => { this.dirty = false; this.listeners.forEach((l) => l()); };

  subscribe = (l: () => void) => { this.listeners.add(l); return () => { this.listeners.delete(l); }; };
  getNodeSnapshot = (metric: NodeMetric): number => this.nodes[metric * WINDOW + prev(this.head)];
}
```

React reads it through `useSyncExternalStore` with **per-slice selectors**, so a stat tile re-renders only when its own number changes:

```ts
export function useNodeMetric(m: NodeMetric): number {
  return useSyncExternalStore(store.subscribe, () => store.getNodeSnapshot(m));
}
```

`useSyncExternalStore` returning a primitive gives free `Object.is` bailout — no memo, no equality function, no `useMemo` returning a new object every render (the classic mistake here). At most **one render pass per animation frame**, and only for components whose scalar actually moved.

The canvas engine doesn't subscribe at all — its own rAF loop reads `store` directly. Zero React involvement for the 60fps layer.

### 7.2 Virtualization

`@tanstack/react-virtual` v3 — headless, works with a plain `<div role="grid">` structure, ~4KB. (Not `react-window`: no dynamic measurement, no active React 19 story. Not TanStack Table: this app needs virtualization and cursor pagination, not column-model abstractions; the columns are a hand-written array of 9 render functions.)

- Fixed `estimateSize: () => rowH` — variable rows make the scrollbar lie mid-infinite-load.
- `overscan: 12`.
- Sentinel row at index `n-8` triggers `fetchNextPage`.
- Row components are `memo`'d and receive **primitives only**, never the event object — `<Row id status code ms .../>`. Passing an object defeats memo the moment a delta patches one field.
- Table is `role="grid"`, rows `role="row"`, cells `role="gridcell"`, with `aria-rowcount` reporting the *total* (not the rendered window) and `aria-rowindex` on each row. Without those three attributes a virtualized table tells a screen reader there are 20 events when there are 200,000.

### 7.3 States

Every data surface implements four states, and there's a `<DataBoundary>` wrapper that enforces it:

- **Skeleton** — shape-accurate: the events table renders 20 skeleton rows at exact `--row-h` with column-width-accurate bars. Never a spinner in place of a table. Charts render their axes and grid with a shimmer plot area, so the layout never jumps.
- **Empty** — distinguishes *no data yet* ("No events yet — send your first webhook" + a copyable `curl`) from *no matches* ("No events match these filters" + a `Clear filters` button). Conflating those two is the most common empty-state bug.
- **Error** — inline within the panel, never a full-page takeover. Shows the HTTP status, the server's `error.code`, a `Retry` button, and a `<IdChip>` of the request ID for support.
- **Degraded** — the state most dashboards skip: WS disconnected but REST fine → live tiles gray out, gain an `aria-label`'d `STALE · last update 14:22:07` chip, and stop pretending. **A live dashboard that silently freezes is worse than one that says it froze.**

### 7.4 Optimistic UI for replay

```ts
useMutation({
  mutationFn: (ids: string[]) => api.post("/events/replay", { ids }),
  onMutate: async (ids) => {
    await qc.cancelQueries({ queryKey: ["events"] });
    const prev = qc.getQueriesData({ queryKey: ["events"] });
    ids.forEach((id) =>
      patchEventEverywhere(qc, id, { status: "queued", replaying: true }, OPTIMISTIC_REV)
    );
    return { prev };
  },
  onError: (err, ids, ctx) => {
    ctx?.prev.forEach(([k, v]) => qc.setQueryData(k, v));
    toast.error(`Replay failed: ${err.message}`, { duration: Infinity, action: { label: "Retry", onClick: retry } });
  },
  onSettled: () => qc.invalidateQueries({ queryKey: ["events"] }),
});
```

`OPTIMISTIC_REV = Number.MAX_SAFE_INTEGER - 1` so a stale in-flight delta can't undo the optimistic patch, but the real server delta (rev ≥ that? no —) — concretely: optimistic patches are flagged `optimistic: true` and the `rev` guard is skipped for the *first* server delta on that entity after a mutation, then normal `rev` ordering resumes. Bulk replay shows a determinate progress bar fed by `dlq:{project}` deltas, never a fake one.

### 7.5 Accessibility

- **Focus:** `:focus-visible` only, 2px `--focus-ring` offset 2px, never removed. Visible on the dark surface at ≥3:1 (the teal is 11:1).
- **Keyboard map:** `⌘K` palette · `/` focus filter · `j`/`k` row nav · `Enter` open · `Esc` close panel · `x` select · `⇧+click` range · `g` then `o|e|n|d|p|s` go-to · `⌘⇧T` chart→table · `?` shortcut sheet. Roving tabindex in the grid (exactly one row is tabbable), so `Tab` doesn't walk 200,000 rows.
- **Live regions:** the feed is `aria-live="off"` by default with an explicit opt-in toggle — announcing 40 rows/sec is hostile. Only threshold events (breaker open, backpressure, DLQ milestone) go to a single `aria-live="polite"` region, rate-limited to one announcement per 3s.
- **Canvas:** `role="img"` + a ≤1Hz-regenerated `aria-label`, a `sr-only` mirror table of every node counter, and Tab-reachable node buttons.
- **Color independence:** status glyphs (§2.4①), direct labels on charts, chart→table toggle everywhere, and `@media (forced-colors: active)` mapping status to `Highlight`/`GrayText` plus enabling the SVG hatch textures.
- **Motion:** `prefers-reduced-motion` handled globally (§2.5), plus an in-app override in Settings for people whose OS setting doesn't match their preference here.
- **Contrast:** every text token above was validated; `--fg-4` (2.9:1 light / 3.3:1 dark) is restricted by lint to ≥16px or non-text decoration.

---

## 8. File layout, and what to install vs. build

```
web/
  index.html                      # inline no-flash theme script + font preloads
  vite.config.ts                  # @tailwindcss/vite, proxy /api + /ws → :8080
  components.json                 # style: "base-nova", css: src/styles/globals.css
  public/fonts/Geist[wght].woff2, GeistMono[wght].woff2
  src/
    main.tsx  App.tsx  router.tsx
    styles/
      globals.css                 # @import tailwindcss + @theme inline
      tokens.css                  # §2.1 — the single source of color truth
      uplot.css                   # override uPlot defaults onto our tokens
    lib/
      api/
        client.ts                 # apiFetch: CSRF header, typed ApiError, abort
        events.ts endpoints.ts dlq.ts keys.ts demo.ts   # per-resource fns + query keys
      ws/
        protocol.ts               # §4.1 types — mirrors Go internal/hub/protocol.go
        connection.ts             # socket, backoff+jitter, resume, ack, rate
        stream-store.ts           # §7.1 ring buffers + rAF flush
        use-channels.ts           # declarative sub/unsub per route
        reconcile.ts              # §4.6 rev-guarded delta → queryClient
      format.ts  time.ts  keyboard.ts  utils.ts   # cn() lives here
    hooks/
      use-events.ts               # useInfiniteQuery + head buffer
      use-endpoints.ts use-dlq.ts use-live-metric.ts use-density.ts
    components/
      ui/                         # shadcn base-nova, customized
      primitives/                 # §2.4 — StatusPill Num Sparkline IdChip
                                  #        AttemptTimeline LatencyBars
                                  #        Panel HairlineRule DataBoundary
      charts/                     # TimeSeries.tsx (the one uPlot wrapper)
                                  # Throughput OutcomeArea LatencyPercentiles
                                  # ChartFrame ChartLegend ChartTableView
      canvas/
        FlowCanvas.tsx            # ReactFlow + custom nodes
        ParticleLayer.tsx         # the <canvas> overlay
        engine/  particles.ts  edges.ts  sampler.ts  raf.ts
        nodes/   IngestNode QueueNode WorkerNode EndpointNode DlqNode RetryNode
      table/
        VirtualTable.tsx  Row.tsx  FilterBar.tsx  BulkBar.tsx  ColumnDefs.tsx
      layout/
        AppShell Sidebar TopBar ConnectionChip CommandPalette DetailSheet
      json/  JsonTree.tsx  BodyViewer.tsx
    routes/
      overview/ events/ endpoints/ dlq/ playground/ settings/ demo/ login/
```

`protocol.ts` is generated from the Go types (`go run ./cmd/tsgen` or `tygo`) and committed — a hand-maintained copy of a wire protocol drifts within two weeks.

### 8.1 shadcn `base-nova` components to install

`button · input · textarea · label · select · combobox · checkbox · switch · radio-group · dialog · alert-dialog · sheet · popover · dropdown-menu · context-menu · tooltip · tabs · separator · scroll-area · command · skeleton · sonner · resizable · avatar · collapsible · toggle-group · progress`

Customize every one of them the way `mosaic/src/components/ui/button.tsx` already does: full cva matrices, `data-slot` attributes, `aria-*`/`data-*`-driven states, `color-mix(in oklch, …)` for hover steps. Take the shadcn output as a starting skeleton, then re-tune sizes to the dense scale (`h-7` default instead of `h-8`), radii to 6px, and rings to `--focus-ring`.

### 8.2 Hand-build — do not install a package for these

`badge` → **`<StatusPill>`** · `table` → **`<VirtualTable>`** · `card` → **`<Panel>`** (hairline, no shadow) · `chart` → **`<TimeSeries>`** + hand-rolled SVG · `breadcrumb`, `pagination` (cursor-based, not page-numbered) · `<Num>` · `<IdChip>` · `<Sparkline>` · `<AttemptTimeline>` · `<LatencyBars>` · `<JsonTree>` · `<ThemeProvider>` · the density provider.

---

## Critical Files for Implementation

- `/Users/jay/Documents/Programming/ithaca/web/src/styles/tokens.css` — the validated oklch token set; every other visual decision resolves through it
- `/Users/jay/Documents/Programming/ithaca/web/src/lib/ws/protocol.ts` — the wire contract, generated from Go; the boundary the whole realtime design rests on
- `/Users/jay/Documents/Programming/ithaca/web/src/lib/ws/stream-store.ts` — ring buffers + rAF flush; the reason the 10Hz feed doesn't melt React
- `/Users/jay/Documents/Programming/ithaca/web/src/components/canvas/engine/sampler.ts` — the honest-sampling logic that makes the hero canvas both truthful and 60fps
- `/Users/jay/Documents/Programming/ithaca/web/src/components/charts/TimeSeries.tsx` — the single uPlot wrapper; all charting knowledge in the codebase lives here

**Reference files worth reading before starting** (the author's own prior art that this plan deliberately extends):
- `/Users/jay/Documents/Programming/mosaic/frontend/src/lib/ws.ts` — the existing WS hook; Ithaca's `connection.ts` is this plus resume, ack-based backpressure, jittered backoff, and cookie auth instead of a querystring token
- `/Users/jay/Documents/Programming/mosaic/frontend/src/components/ui/button.tsx` — the cva/`data-slot` customization style to match across all of `components/ui/`
- `/Users/jay/Documents/Programming/mosaic/frontend/src/app/globals.css` — the `@theme inline` + CSS-variable structure that `tokens.css` follows