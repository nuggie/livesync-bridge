import { DirectFileManipulator, FileInfo, MetaEntry, ReadyEntry } from "./lib/src/API/DirectFileManipulatorV2.ts";
import { FilePathWithPrefix, LOG_LEVEL_NOTICE, MILESTONE_DOCID, TweakValues } from "./lib/src/common/types.ts";
import { PeerCouchDBConf, FileData } from "./types.ts";
import { decodeBinary } from "./lib/src/string_and_binary/convert.ts";
import { isPlainText } from "./lib/src/string_and_binary/path.ts";
import { DispatchFun, Peer } from "./Peer.ts";
import { createBinaryBlob, createTextBlob, isDocContentSame, unique } from "./lib/src/common/utils.ts";
import { withRetry } from "./util.ts";

// export class PeerInstance()

export class PeerCouchDB extends Peer {
    man: DirectFileManipulator;
    declare config: PeerCouchDBConf;
    constructor(conf: PeerCouchDBConf, dispatcher: DispatchFun) {
        super(conf, dispatcher);
        this.man = new DirectFileManipulator(conf);
        // Fetch remote since.
        this.man.since = this.getSetting("since") || "now";
    }
    async delete(pathSrc: string): Promise<boolean> {
        const path = this.toLocalPath(pathSrc);
        if (await this.isRepeating(pathSrc, false)) {
            return false;
        }
        try {
            const r = await withRetry(() => this.man.delete(path), {
                label: `delete ${path}`,
                attempts: 10,
                onRetry: (m) => this.normalLog(m, LOG_LEVEL_NOTICE),
            });
            if (r) {
                this.receiveLog(` ${path} deleted`);
            } else {
                this.receiveLog(` ${path} delete failed`, LOG_LEVEL_NOTICE);
            }
            return r;
        } catch (ex) {
            // Exhausted retries: never throw (that would kill the daemon). Clear the
            // dedup cache so a later re-dispatch / offline scan re-attempts this path.
            this.cache.set(pathSrc, "");
            this.receiveLog(` ${path} DELETE gave up after retries: ${(ex as Error)?.message ?? ex}`, LOG_LEVEL_NOTICE);
            return false;
        }
    }
    async put(pathSrc: string, data: FileData): Promise<boolean> {
        const path = this.toLocalPath(pathSrc);
        if (await this.isRepeating(pathSrc, data)) {
            return false;
        }
        const type = isPlainText(path) ? "plain" : "newnote";
        const info: FileInfo = {
            ctime: data.ctime,
            mtime: data.mtime,
            size: data.size
        };
        const saveData = (data.data instanceof Uint8Array) ? createBinaryBlob(data.data) : createTextBlob(data.data);
        try {
            // Retry only the network block (get old -> compare -> put). The isRepeating()
            // dedup guard above must NOT be re-run on retry, or the retry would be
            // swallowed as a duplicate and the change silently lost.
            return await withRetry(async () => {
                const old = await this.man.get(path as FilePathWithPrefix, true) as false | MetaEntry;
                // const old = await this.getMeta(path as FilePathWithPrefix);
                if (old && Math.abs(this.compareDate(info, old)) < 3600) {
                    const oldDoc = await this.man.getByMeta(old);
                    if (oldDoc && ("data" in oldDoc)) {
                        const d = oldDoc.type == "plain" ? createTextBlob(oldDoc.data) : createBinaryBlob(new Uint8Array(decodeBinary(oldDoc.data)));
                        if (await isDocContentSame(d, saveData)) {
                            this.normalLog(` Skipped (Same) ${path} `);
                            return false;
                        }
                    }
                }
                const r = await this.man.put(path, saveData, info, type);
                if (r) {
                    this.receiveLog(` ${path} saved`);
                } else {
                    this.receiveLog(` ${path} ignored`);
                }
                return r;
            }, {
                label: `put ${path}`,
                attempts: 10,
                onRetry: (m) => this.normalLog(m, LOG_LEVEL_NOTICE),
            });
        } catch (ex) {
            // Exhausted retries: never throw (that would kill the daemon). Clear the
            // dedup cache so a later re-dispatch / offline scan re-attempts this path.
            this.cache.set(pathSrc, "");
            this.receiveLog(` ${path} PUT gave up after retries: ${(ex as Error)?.message ?? ex}`, LOG_LEVEL_NOTICE);
            return false;
        }
    }
    async get(pathSrc: FilePathWithPrefix): Promise<false | FileData> {
        const path = this.toLocalPath(pathSrc) as FilePathWithPrefix;
        const ret = await this.man.get(path) as false | ReadyEntry;
        if (ret === false) {
            return false;
        }
        return {
            ctime: ret.ctime,
            mtime: ret.mtime,
            data: ret.type == "newnote" ? new Uint8Array(decodeBinary(ret.data)) : ret.data,
            size: ret.size,
            deleted: ret.deleted
        };
    }
    async getMeta(pathSrc: FilePathWithPrefix): Promise<false | FileData> {
        const path = this.toLocalPath(pathSrc) as FilePathWithPrefix;
        const ret = await this.man.get(path, true) as false | MetaEntry;
        if (ret === false) {
            return false;
        }
        return {
            ctime: ret.ctime,
            mtime: ret.mtime,
            data: [],
            size: ret.size,
            deleted: ret.deleted
        };
    }
    /** Wait until the manipulator's DB connection is ready. If the initial
     * connection died (e.g. DNS/WAN was down at boot), its `ready` promise never
     * resolves, so recreate the manipulator and try again instead of hanging or
     * crashing. */
    async ensureReady(): Promise<void> {
        for (let attempt = 1; ; attempt++) {
            const ready = await Promise.race([
                this.man.ready.promise.then(() => true, () => false),
                new Promise<boolean>((r) => setTimeout(() => r(false), 30000)),
            ]);
            if (ready) return;
            this.normalLog(
                `CouchDB connection not ready (attempt ${attempt}) — recreating manipulator and retrying…`,
                LOG_LEVEL_NOTICE,
            );
            this.man = new DirectFileManipulator(this.config);
            this.man.since = this.getSetting("since") || "now";
        }
    }
    async start(): Promise<void> {
        // start() is invoked un-awaited by Hub.start(); any throw here would be an
        // unhandled rejection and kill the daemon. Retry indefinitely instead — a
        // WAN/DNS outage just delays startup until the network returns.
        for (let attempt = 1; ; attempt++) {
            try {
                await this._start();
                return;
            } catch (ex) {
                this.normalLog(
                    `start() failed (attempt ${attempt}): ${(ex as Error)?.message ?? ex}. Retrying in 15s`,
                    LOG_LEVEL_NOTICE,
                );
                await new Promise((r) => setTimeout(r, 15000));
            }
        }
    }
    async _start(): Promise<void> {
        const baseDir = this.toLocalPath("");
        await this.ensureReady();
        const w = await withRetry(() => this.man.rawGet<Record<string, any>>(MILESTONE_DOCID), {
            label: "fetch milestone",
            attempts: 10,
            onRetry: (m) => this.normalLog(m, LOG_LEVEL_NOTICE),
        });
        if (w && "tweak_values" in w) {
            if (this.config.useRemoteTweaks) {
                const tweaks = Object.values(w["tweak_values"])[0] as TweakValues;
                // console.log(tweaks)
                const orgConf = { ...this.config } as Record<string, any>;
                this.config.customChunkSize = tweaks.customChunkSize ?? this.config.customChunkSize;
                this.config.minimumChunkSize = tweaks.minimumChunkSize ?? this.config.minimumChunkSize;
                if (tweaks.encrypt && !this.config.passphrase) {
                    throw new Error("Remote database is encrypted but no passphrase provided.");
                }
                if (tweaks.usePathObfuscation && !this.config.obfuscatePassphrase) {
                    throw new Error("Remote database is obfuscated but no obfuscate passphrase provided.");
                }
                this.config.hashAlg = tweaks.hashAlg ?? this.config.hashAlg;
                this.config.maxAgeInEden = tweaks.maxAgeInEden ?? this.config.maxAgeInEden;
                this.config.maxTotalLengthInEden = tweaks.maxTotalLengthInEden ?? this.config.maxTotalLengthInEden;
                this.config.maxChunksInEden = tweaks.maxChunksInEden ?? this.config.maxChunksInEden;
                this.config.useEden = tweaks.useEden ?? this.config.useEden;
                if (!this.config.enableCompression != !tweaks.enableCompression) {
                    throw new Error("Compression setting mismatched.");
                }
                this.config.useDynamicIterationCount = tweaks.useDynamicIterationCount ?? this.config.useDynamicIterationCount;
                this.config.enableChunkSplitterV2 = tweaks.enableChunkSplitterV2 ?? this.config.enableChunkSplitterV2;
                this.config.chunkSplitterVersion = tweaks.chunkSplitterVersion ?? this.config.chunkSplitterVersion;
                this.config.E2EEAlgorithm = tweaks.E2EEAlgorithm ?? this.config.E2EEAlgorithm;
                this.config.minimumChunkSize = tweaks.minimumChunkSize ?? this.config.minimumChunkSize;
                this.config.customChunkSize = tweaks.customChunkSize ?? this.config.customChunkSize;
                this.config.doNotUseFixedRevisionForChunks = tweaks.doNotUseFixedRevisionForChunks ?? this.config.doNotUseFixedRevisionForChunks;
                this.config.handleFilenameCaseSensitive = tweaks.handleFilenameCaseSensitive ?? this.config.handleFilenameCaseSensitive;
                const newConf = { ...this.config } as Record<string, any>;
                this.man.options = this.config;
                await this.man.liveSyncLocalDB.initializeDatabase()
                // await this.man.managers.initManagers();
                const diff = unique([...Object.keys(orgConf), ...Object.keys(tweaks)]).filter(k => orgConf[k] != newConf[k]);
                if (diff.length > 0) {
                    this.normalLog(`Remote tweaks changed --->`);
                    for (const diffKey of diff) {
                        this.normalLog(`${diffKey}\t: ${orgConf[diffKey]} \t : ${newConf[diffKey]}`);
                    }
                    this.normalLog(`<--- Remote tweaks changed`);
                }
            }
        }
        if (!w) {
            this.normalLog(`Remote database looks like empty. fetch from the first.`);
            this.setSetting("remote-created", "0");
            return;
        }
        const created = w.created;
        if (this.getSetting("remote-created") !== `${created}`) {
            this.man.since = "";
            this.normalLog(`Remote database looks like rebuilt. fetch from the first again.`);
            this.setSetting("remote-created", `${created}`);
        } else {
            this.normalLog(`Watch starting from ${this.man.since}`);
        }
        this.man.beginWatch(async (entry) => {
            const d = entry.type == "plain" ? entry.data : new Uint8Array(decodeBinary(entry.data));
            let path = entry.path.substring(baseDir.length);
            if (path.startsWith("/")) {
                path = path.substring(1);
            }
            if (entry.deleted || entry._deleted) {
                this.sendLog(`${path} delete detected`);
                await this.dispatchDeleted(path);
            } else {
                const docData = { ctime: entry.ctime, mtime: entry.mtime, size: entry.size, deleted: entry.deleted || entry._deleted, data: d };
                this.sendLog(`${path} change detected`);
                await this.dispatch(path, docData);
            }
        }, (entry) => {
            this.setSetting("since", this.man.since);
            if (entry.path.indexOf(":") !== -1) return false;
            return entry.path.startsWith(baseDir);
        });
    }
    async dispatch(path: string, data: FileData | false) {
        if (data === false) return;
        if (!await this.isRepeating(path, data)) {
            await this.dispatchToHub(this, this.toGlobalPath(path), data);
        }
        // else {
        //     this.receiveLog(`${path} dispatch repeating`);
        // }
    }
    async dispatchDeleted(path: string) {
        if (!await this.isRepeating(path, false)) {
            await this.dispatchToHub(this, this.toGlobalPath(path), false);
        }
    }
    async stop(): Promise<void> {
        this.man.endWatch();
        return await Promise.resolve();
    }
}
