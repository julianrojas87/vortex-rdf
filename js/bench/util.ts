// Pure bench helpers, shared by every instrument in this directory:
// codspeed.bench.ts, compare.bench.ts + shared.ts (and its workers), and
// dict-memory.bench.ts + its worker.
//
// PURITY CONTRACT: this module must import nothing but node builtins and types.
// No store library (oxigraph, rdf-stores, or the Vortex wasm module) may be
// reachable from here — codspeed.bench.ts imports it and runs under CodSpeed's
// Valgrind instrumentation, where loading a foreign multi-MB wasm module would
// pollute every measurement. That constraint is why this module exists at all
// instead of these helpers living in shared.ts, which does import all three.

import { spawnSync } from 'node:child_process';
import { readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import type { Quad } from '@rdfjs/types';

/** Force every term of every quad to be materialized.
 *
 * Counting rows (or taking `.length`) never decodes a single term under the
 * Dictionary layout — the lazy read model hands back term *codes* and only
 * resolves them when `.value` is read. Without this the whole term-decoding
 * path, and the per-distinct-term boundary crossing the on-demand dictionary
 * makes, is invisible to any instrument here. */
export function decodeAll(quads: readonly Quad[]): number {
    let chars = 0;
    for (const q of quads) {
        chars += q.subject.value.length + q.predicate.value.length
            + q.object.value.length + q.graph.value.length;
    }
    return chars;
}

/** Human-readable duration from nanoseconds. */
export function fmtNs(ns: number): string {
    if (ns < 1e3) return ns.toFixed(0) + ' ns';
    if (ns < 1e6) return (ns / 1e3).toPrecision(3) + ' µs';
    if (ns < 1e9) return (ns / 1e6).toPrecision(3) + ' ms';
    return (ns / 1e9).toPrecision(3) + ' s';
}

/** Deterministically drop a wasm-bindgen value's Rust-side allocation.
 *
 * Vortex's and oxigraph's public `.d.ts` are hand-curated and deliberately omit
 * `free()` — ordinary consumers are meant to lean on the FinalizationRegistry.
 * A benchmark is not an ordinary consumer: V8 has no visibility into wasm
 * linear-memory pressure and can defer finalizers arbitrarily long, so every
 * store must be disposed as soon as its measurement is done. Hence the reach
 * past the curated type, rather than widening what real consumers see. */
export function freeWasm(h: unknown): void {
    (h as { free(): void }).free();
}

const here = dirname(fileURLToPath(import.meta.url));
const tsxBin = resolve(here, '..', 'node_modules', '.bin', 'tsx');
/** `--expose-gc` is required by shared.ts's memory readings; the heap ceiling
 *  covers the multi-million-quad datasets the workers generate. */
const NODE_FLAGS = ['--expose-gc', '--max-old-space-size=8192'];

/** Wall-clock backstop for one worker process, ms; `0` disables it.
 *
 *  The last line of defense behind `consumeQuads`' in-loop budget: that check
 *  can only fire while JavaScript is running, so a phase that stalls *inside* a
 *  single wasm call (or any future pathology outside our own loops) would hang
 *  the run indefinitely. The default is far above any legitimate worker — the
 *  slowest on record is oxigraph's query role at ~10 minutes. */
const WORKER_TIMEOUT_MS = Number(process.env.WORKER_TIMEOUT_MS ?? 30 * 60_000);

/**
 * Run one bench worker to completion in its own process and return its parsed
 * JSON output, or null if it could not produce one.
 *
 * The worker receives `args` followed by the path it must write its JSON to;
 * that file is this function's to name and to remove. A non-zero exit is
 * reported and skipped rather than thrown, so one failed point never costs the
 * whole run. A worker that outlives `WORKER_TIMEOUT_MS` is killed the same way
 * — SIGKILL, because a wasm-bound death march does not answer SIGTERM.
 *
 * Process-per-worker is a correctness requirement, not an optimization — see
 * the headers of compare.bench.ts (cross-adapter memory contamination) and
 * dict-memory.worker.ts (wasm linear memory never shrinks).
 */
export function runWorkerProcess<T>(workerPath: string, args: string[], label: string): T | null {
    const stem = label.replace(/[^A-Za-z0-9._-]+/g, '-');
    const outFile = join(tmpdir(), `vortex-bench-${stem}-${process.pid}.json`);
    const res = spawnSync(tsxBin, [...NODE_FLAGS, workerPath, ...args, outFile], {
        stdio: 'inherit',
        env: process.env,
        ...(WORKER_TIMEOUT_MS > 0 ? { timeout: WORKER_TIMEOUT_MS, killSignal: 'SIGKILL' as const } : {}),
    });
    try {
        if (res.status !== 0) {
            const why = res.signal
                ? `was killed (${res.signal}${res.signal === 'SIGKILL'
                    ? `, likely the ${WORKER_TIMEOUT_MS / 60_000}-minute WORKER_TIMEOUT_MS backstop` : ''})`
                : `exited ${res.status}`;
            console.error(`\n[${label}] worker ${why} — skipping; the rest of the run continues.`);
            return null;
        }
        return JSON.parse(readFileSync(outFile, 'utf8')) as T;
    } catch (e) {
        console.error(`[${label}] failed to read worker output:`, e);
        return null;
    } finally {
        rmSync(outFile, { force: true });
    }
}
