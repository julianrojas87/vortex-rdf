#!/usr/bin/env python3
"""Slice the first N lines of an N-Triples file, optionally rewriting each
triple into an N-Quads line with a fixed graph.

Used to prepare DBBench datasets of different sizes from a full DBpedia dump:

    python3 dbpedia_gen.py --input dbpedia.nt --limit 5000000 --output dbpedia_5M.nt
    python3 dbpedia_gen.py --input dbpedia.nt --limit 5000000 \
        --output dbpedia_5M.nq --graph http://example.org/g0
"""

import argparse


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Source N-Triples file")
    parser.add_argument("--limit", type=int, required=True,
                        help="Number of triples to keep")
    parser.add_argument("--output", required=True, help="Destination file")
    parser.add_argument(
        "--graph", default=None,
        help="Graph IRI (no angle brackets); when given, output is N-Quads")
    args = parser.parse_args()

    written = 0
    with open(args.input, "r", encoding="utf-8", errors="ignore") as infile, \
         open(args.output, "w", encoding="utf-8") as outfile:

        for line in infile:
            if written >= args.limit:
                break

            line = line.strip()
            if not line:
                continue

            if args.graph is None:
                outfile.write(f"{line}\n")
            else:
                if line.endswith("."):
                    line = line[:-1].strip()
                outfile.write(f"{line} <{args.graph}> .\n")
            written += 1

    print(f"Done: wrote {written} triples to {args.output}")


if __name__ == "__main__":
    main()
