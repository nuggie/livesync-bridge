import { assertEquals } from "jsr:@std/assert@^1";
import { PeerCouchDB } from "./PeerCouchDB.ts";
import type { PeerCouchDBConf } from "./types.ts";

function makePeer(name: string): PeerCouchDB {
    const conf = {
        type: "couchdb", group: "test", name, url: "http://127.0.0.1:1", database: "x",
        username: "u", password: "p", passphrase: "", obfuscatePassphrase: "", baseDir: ".",
    } as unknown as PeerCouchDBConf;
    const peer = new PeerCouchDB(conf, () => Promise.resolve());
    // The real manipulator is only built in start(); a stand-in is enough here.
    (peer as unknown as { man: { since: string } }).man = { since: "now" };
    return peer;
}

Deno.test("_advanceSince stores the real feed position, not 'now'", () => {
    const peer = makePeer(`since-test-${crypto.randomUUID()}`);
    peer._advanceSince("42-g1AAAABxeJzLYWBgYMpgTmHgz8tPSTV0MDQ");
    assertEquals(peer.man.since, "42-g1AAAABxeJzLYWBgYMpgTmHgz8tPSTV0MDQ");
    assertEquals(peer.getSetting("since"), "42-g1AAAABxeJzLYWBgYMpgTmHgz8tPSTV0MDQ");
    peer._advanceSince(43);
    assertEquals(peer.getSetting("since"), "43");
});

Deno.test("_advanceSince ignores a missing seq", () => {
    const peer = makePeer(`since-test-${crypto.randomUUID()}`);
    peer.setSetting("since", "7");
    peer._advanceSince(undefined);
    peer._advanceSince("");
    assertEquals(peer.getSetting("since"), "7");
    assertEquals(peer.man.since, "now");
});
