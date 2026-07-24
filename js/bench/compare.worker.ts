// Runs ONE adapter's full lifecycle in its own process, spawned by compare.bench.ts.
// Process-level isolation, not just in-process dispose+gc: a crash in one adapter
// can't take down the whole run, and each adapter gets a clean, uncontaminated
// peak-memory reading (see shared.ts's `peakRssMb`) instead of one polluted by
// whatever every earlier adapter left resident.
import { Bench } from 'tinybench';
import { withCodSpeed } from '@codspeed/tinybench-plugin';
import { writeFileSync } from 'node:fs';

import {
    ADAPTERS, MUT_ADAPTERS, D, DQ, MUT_BATCH,
    genTriples, genQuads, genFresh, genTriplesPrefix,
    TRIPLE_PATTERNS, QUAD_PATTERNS, FULL_SCAN_PATTERN, QUERY_OPTS, HEAVY_OPTS, FULL_SCAN_OPTS,
    reclaim, collect, peakRssMb, type Row, type StoreAdapter,
} from './shared.js';

const [, , slug, role, outFile] = process.argv;
if (!slug || !role || !outFile) {
    console.error('usage: compare.worker.ts <slug> <query|mutate> <out-file>');
    process.exit(1);
}

async function runBench(rows: Row[], opts: typeof QUERY_OPTS, add: (b: Bench) => void): Promise<void> {
    const bench = withCodSpeed(new Bench(opts));
    add(bench);
    await bench.run();
    collect(bench, rows);
}

async function runQuery(a: StoreAdapter): Promise<Row[]> {
    const rows: Row[] = [];

    console.log(`[${a.label}] query (triples)…`);
    const triples = genTriples(D);
    let th: unknown = await a.build(triples);
    await runBench(rows, QUERY_OPTS, (b) => {
        for (const p of TRIPLE_PATTERNS) b.add(`${a.slug}::${p.name}`, async () => { await a.countMatch(th, p); });
    });
    await runBench(rows, FULL_SCAN_OPTS, (b) => {
        b.add(`${a.slug}::full`, async () => { await a.countMatch(th, FULL_SCAN_PATTERN); });
    });
    reclaim(a, th);
    th = null;

    console.log(`[${a.label}] query (quads)…`);
    const quads = genQuads(DQ);
    let qh: unknown = await a.build(quads);
    await runBench(rows, QUERY_OPTS, (b) => {
        for (const p of QUAD_PATTERNS) b.add(`${a.slug}::${p.name}`, async () => { await a.countMatch(qh, p); });
    });
    reclaim(a, qh);
    qh = null;

    console.log(`[${a.label}] ingest…`);
    await runBench(rows, HEAVY_OPTS, (b) => {
        // Dispose in `afterEach` (untimed) rather than inside the timed function, so
        // freeing the previous store never pollutes the measured ingest cost.
        let h: unknown;
        b.add(`ingest::${a.slug}`, async () => { h = await a.build(triples); }, {
            afterEach: () => { reclaim(a, h); h = undefined; },
        });
    });

    return rows;
}

async function runMutate(a: StoreAdapter): Promise<Row[]> {
    const rows: Row[] = [];
    const fresh = genFresh(MUT_BATCH);
    const delSlice = genTriplesPrefix(D, MUT_BATCH);

    console.log(`\n[${a.label}] add (${MUT_BATCH})…`);
    await runBench(rows, HEAVY_OPTS, (b) => {
        let h: unknown;
        b.add(`add::${a.slug}`, async () => {
            h = await a.newEmpty();
            await a.addAll(h, fresh);
        }, { afterEach: () => { reclaim(a, h); h = undefined; } });
        if (a.addBatch) {
            let hb: unknown;
            b.add(`add_batch::${a.slug}`, async () => {
                hb = await a.newEmpty();
                await a.addBatch!(hb, fresh);
            }, { afterEach: () => { reclaim(a, hb); hb = undefined; } });
        }
    });

    console.log(`[${a.label}] delete (${MUT_BATCH})…`);
    let dh: unknown = null;
    await runBench(rows, HEAVY_OPTS, (b) => {
        b.add(`delete::${a.slug}`, async () => { await a.deleteAll(dh, delSlice); }, {
            beforeEach: async () => { dh = await a.build(delSlice); },
            afterEach: () => { reclaim(a, dh); dh = null; },
        });
    });

    return rows;
}

async function main(): Promise<void> {
    const list = role === 'query' ? ADAPTERS : MUT_ADAPTERS;
    const a = list.find((x) => x.slug === slug);
    if (!a) {
        console.error(`unknown adapter slug '${slug}' for role '${role}'`);
        process.exit(1);
    }

    const rows = role === 'query' ? await runQuery(a) : await runMutate(a);
    writeFileSync(outFile, JSON.stringify({ rows, peakRssMb: peakRssMb() }));
}

main().catch((e) => {
    console.error(e);
    process.exit(1);
});
