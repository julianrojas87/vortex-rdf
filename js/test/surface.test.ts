import { describe, test, expect } from 'vitest';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

// The generated pkg/web/vortex_rdf.d.ts lists every `#[wasm_bindgen]` method
// as an `InitOutput` key (`vortexrdfstore_<name>` / `termdict_<name>`); the
// hand-written src/api.d.ts must declare each of them on its class.

const read = (rel: string) => readFileSync(fileURLToPath(new URL(rel, import.meta.url)), 'utf8');

function wasmMethods(generated: string, prefix: string): string[] {
    const block = generated.slice(generated.indexOf('export interface InitOutput'));
    const re = new RegExp(`readonly ${prefix}_([A-Za-z0-9_]+):`, 'g');
    return [...block.matchAll(re)].map((m) => m[1]).filter((n) => !n.startsWith('__wbg'));
}

function declaredMethods(api: string, className: string): Set<string> {
    const start = api.indexOf(`export class ${className} {`);
    const end = api.indexOf('\n}', start);
    const block = api.slice(start, end);
    const names = new Set<string>();
    for (const m of block.matchAll(/^\s+(?:static\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*\(/gm)) names.add(m[1]);
    return names;
}

describe('api.d.ts covers the wasm exports', () => {
    const generated = read('../pkg/web/vortex_rdf.d.ts');
    const api = read('../src/api.d.ts');

    for (const [prefix, className] of [['vortexrdfstore', 'VortexRdfStore'], ['termdict', 'TermDict']] as const) {
        test(className, () => {
            const exported = wasmMethods(generated, prefix);
            expect(exported.length).toBeGreaterThan(0);
            const declared = declaredMethods(api, className);
            const missing = exported.filter((name) => !declared.has(name));
            expect(missing).toEqual([]);
        });
    }
});
