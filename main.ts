import { defaultLoggerEnv } from "./lib/src/common/logger.ts";
import { LOG_LEVEL_DEBUG } from "./lib/src/common/logger.ts";
import { Hub } from "./Hub.ts";
import { Config } from "./types.ts";
import { parseArgs } from "jsr:@std/cli";

const KEY = "LSB_"
defaultLoggerEnv.minLogLevel = LOG_LEVEL_DEBUG;
const configFile = Deno.env.get(`${KEY}CONFIG`) || "./dat/config.json";

console.log("LiveSync Bridge is now starting...");

// Resilience backstop: a WAN blip reloads Unbound on the router; the internal
// CouchDB name (docker.developage.lan, TTL~1) is then briefly unresolvable and
// an in-flight fetch rejects. Previously such an "Uncaught (in promise)
// FetchError" killed the whole process -> systemd restart loop -> missed
// offline changes / data loss. Keep the daemon alive and let the per-operation
// retries (PeerCouchDB) recover once the network returns.
globalThis.addEventListener("unhandledrejection", (e: PromiseRejectionEvent) => {
    e.preventDefault();
    console.error("[resilience] Unhandled rejection suppressed to keep bridge alive:", e.reason);
});

let config: Config = { peers: [] };
const flags = parseArgs(Deno.args, {
    boolean: ["reset"],
    // string: ["version"],
    default: { reset: false },
});
if (flags.reset) {
    localStorage.clear();
}
try {
    const confText = await Deno.readTextFile(configFile);
    config = JSON.parse(confText);
} catch (ex) {
    console.error("Could not parse configuration!");
    console.error(ex);
}
console.log("LiveSync Bridge is now started!");
const hub = new Hub(config);
hub.start();