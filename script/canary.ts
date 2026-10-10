// LiveSync canary (Nova, 2026-10-10): writes one note DIRECTLY into CouchDB -- with the
// same library and passphrase as the bridge -- so a wrapper can check whether the
// CouchDB -> filesystem direction still delivers. That direction died silently on
// 2026-09-22 (~20 h, no log line). Usage: deno run -A script/canary.ts <vault-path> <text>
import { DirectFileManipulator } from "@vrtmrz/livesync-commonlib";
import { createTextBlob } from "@vrtmrz/livesync-commonlib/compat/common/utils";

const [path, text] = Deno.args;
if (!path || text === undefined) {
    console.error("usage: canary.ts <vault-path> <text>");
    Deno.exit(2);
}
const cfg = JSON.parse(await Deno.readTextFile(new URL("../dat/config.json", import.meta.url)));
const peer = cfg.peers.find((p: { type: string }) => p.type === "couchdb");
if (!peer) {
    console.error("no couchdb peer in dat/config.json");
    Deno.exit(2);
}
const man = new DirectFileManipulator(peer, { fetch: (r, i) => globalThis.fetch(r, i) });
const timeout = new Promise<never>((_, rej) => setTimeout(() => rej(new Error("CouchDB init timed out")), 30000));
await Promise.race([man.ready.promise as Promise<void>, timeout]);
const now = Date.now();
const size = new TextEncoder().encode(text).length;
const ok = await man.put(path, createTextBlob(text), { ctime: now, mtime: now, size }, "plain");
console.log(ok ? `put ok: ${path}` : `put returned false: ${path}`);
Deno.exit(ok ? 0 : 1);
