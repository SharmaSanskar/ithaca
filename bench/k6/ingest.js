// Ingest load for every benchmark.
//
// OPEN model (constant-arrival-rate), not closed VUs: requests arrive on a
// schedule whether or not the server keeps up. A closed model hides a slow
// server by quietly sending less; here a slow server shows up as latency and
// as dropped_iterations, which is what a real producer would experience.
//
//   MODE=steps     E1: stepped rates (STEPS="250,500,1000"), STEP_S seconds each
//   MODE=constant  RATE events/s for DURATION_S; optional PHASES="30,60,60"
//                  splits the run into before/during/after tagged windows
//
// Other env: TARGET (ingest base URL), KEY (API key), KEYS (number of
// distinct ordering keys; 0 = none), OUT (summary JSON path), HOST (a Host
// header, for reaching an ingress by name through a port on the host).
import http from 'k6/http'
import { check } from 'k6'

const TARGET = __ENV.TARGET || 'http://host.docker.internal:8081'
const KEY = __ENV.KEY
const MODE = __ENV.MODE || 'constant'
const KEYS = parseInt(__ENV.KEYS || '0', 10)
const TYPES = ['order.created', 'order.paid', 'invoice.paid', 'payment.succeeded', 'user.signup']

function constantScenario(rate, duration, start, tags) {
  return {
    executor: 'constant-arrival-rate',
    rate,
    timeUnit: '1s',
    duration: `${duration}s`,
    startTime: `${start}s`,
    preAllocatedVUs: Math.min(2000, Math.max(50, rate)),
    maxVUs: 4000,
    tags,
  }
}

const scenarios = {}
const thresholds = {}
let labels = []
if (MODE === 'steps') {
  const step = parseInt(__ENV.STEP_S || '30', 10)
  const rates = (__ENV.STEPS || '250,500,1000,2000').split(',').map(Number)
  rates.forEach((r, i) => {
    scenarios[`r${r}`] = constantScenario(r, step, i * step, { step: String(r) })
    labels.push(['step', String(r)])
  })
} else {
  const rate = parseInt(__ENV.RATE || '100', 10)
  if (__ENV.PHASES) {
    const names = ['before', 'during', 'after']
    let t = 0
    __ENV.PHASES.split(',').map(Number).forEach((d, i) => {
      scenarios[names[i]] = constantScenario(rate, d, t, { phase: names[i] })
      labels.push(['phase', names[i]])
      t += d
    })
  } else {
    scenarios.main = constantScenario(rate, parseInt(__ENV.DURATION_S || '60', 10), 0, { phase: 'main' })
    labels.push(['phase', 'main'])
  }
}
// Thresholds on tagged sub-metrics make k6 compute per-step / per-phase
// percentiles. The limits are generous on purpose: these runs MEASURE; they
// do not pass or fail.
for (const [k, v] of labels) {
  thresholds[`http_req_duration{${k}:${v}}`] = ['p(99)<60000']
  thresholds[`http_reqs{${k}:${v}}`] = ['count>=0']
  thresholds[`checks{${k}:${v}}`] = ['rate>=0']
  thresholds[`dropped_iterations{${k}:${v}}`] = ['count>=0']
}

export const options = {
  scenarios,
  thresholds,
  summaryTrendStats: ['p(50)', 'p(95)', 'p(99)', 'max'],
  discardResponseBodies: true,
  noConnectionReuse: false,
}

export default function () {
  const body = {
    type: TYPES[Math.floor(Math.random() * TYPES.length)],
    payload: { id: `${__VU}-${__ITER}`, amount: Math.floor(Math.random() * 500), at: Date.now() },
  }
  if (KEYS > 0) body.ordering_key = `k${Math.floor(Math.random() * KEYS)}`
  const res = http.post(`${TARGET}/v1/events`, JSON.stringify(body), {
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${KEY}`, ...(__ENV.HOST ? { Host: __ENV.HOST } : {}) },
  })
  check(res, { 'accepted (202)': (r) => r.status === 202 })
}

// A compact, machine-readable summary per step/phase.
export function handleSummary(data) {
  const out = { mode: MODE, groups: {} }
  for (const [k, v] of labels) {
    const d = data.metrics[`http_req_duration{${k}:${v}}`]
    const n = data.metrics[`http_reqs{${k}:${v}}`]
    const c = data.metrics[`checks{${k}:${v}}`]
    const dr = data.metrics[`dropped_iterations{${k}:${v}}`]
    out.groups[v] = {
      requests: n ? n.values.count : 0,
      rate: n ? n.values.rate : 0,
      ok_ratio: c ? c.values.rate : null,
      dropped: dr ? dr.values.count : 0,
      p50_ms: d ? d.values['p(50)'] : null,
      p95_ms: d ? d.values['p(95)'] : null,
      p99_ms: d ? d.values['p(99)'] : null,
      max_ms: d ? d.values.max : null,
    }
  }
  return { [__ENV.OUT || '/out/summary.json']: JSON.stringify(out, null, 2), stdout: JSON.stringify(out.groups) + '\n' }
}
