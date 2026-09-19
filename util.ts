import { uint8ArrayToHexString } from "./lib/src/string_and_binary/convert.ts";
import { createTextBlob } from "./lib/src/common/utils.ts";


export async function computeHashUInt8Array(key: Uint8Array) {
    const digest = await crypto.subtle.digest('SHA-256', key);
    return uint8ArrayToHexString(new Uint8Array(digest));
}

export const computeHash = async (key: string[] | Uint8Array) => {
    if (key instanceof Uint8Array) return computeHashUInt8Array(key);
    const dx = createTextBlob(key);
    const buf = await dx.arrayBuffer();
    return computeHashUInt8Array(new Uint8Array(buf));

}

/** Heuristic: is this a transient network/DNS/connection failure worth retrying?
 * (WAN blips reload Unbound on the router; the TTL~1 internal CouchDB name is
 * then briefly unresolvable -> getaddrinfo ENOTFOUND / fetch failed.) */
export function isTransientNetworkError(ex: unknown): boolean {
    const e = ex as { name?: string; message?: string; code?: string };
    const msg = `${e?.name ?? ""} ${e?.code ?? ""} ${e?.message ?? ex}`.toLowerCase();
    return /enotfound|getaddrinfo|eai_again|econnrefused|econnreset|etimedout|ehostunreach|enetunreach|epipe|network|fetch failed|failed to fetch|socket hang|socket|timed out|timeout|connection|dns|\b50[234]\b/.test(
        msg,
    );
}

/**
 * Retry an async network operation on transient failures with capped exponential
 * backoff. Non-transient errors are rethrown immediately (no point retrying a
 * logic/auth error). After exhausting all attempts the last error is rethrown;
 * callers decide whether to swallow it (a sync daemon must never die on a blip).
 */
export async function withRetry<T>(
    fn: () => Promise<T>,
    opts: {
        label: string;
        attempts?: number;
        baseDelayMs?: number;
        maxDelayMs?: number;
        onRetry?: (msg: string) => void;
    },
): Promise<T> {
    const attempts = opts.attempts ?? 8;
    const base = opts.baseDelayMs ?? 2000;
    const max = opts.maxDelayMs ?? 30000;
    let lastErr: unknown;
    for (let attempt = 1; attempt <= attempts; attempt++) {
        try {
            return await fn();
        } catch (ex) {
            if (!isTransientNetworkError(ex)) throw ex;
            lastErr = ex;
            if (attempt >= attempts) break;
            const wait = Math.min(max, base * (2 ** (attempt - 1)));
            opts.onRetry?.(
                `${opts.label} failed (attempt ${attempt}/${attempts}, transient): ${
                    (ex as Error)?.message ?? ex
                }. Retry in ${Math.round(wait / 1000)}s`,
            );
            await new Promise((r) => setTimeout(r, wait));
        }
    }
    throw lastErr;
}

export function makeUniqueString() {
    const randomStrSrc = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    const temp = [...Array(30)]
        .map(() => Math.floor(Math.random() * randomStrSrc.length))
        .map((e) => randomStrSrc[e])
        .join("");
    return `${Date.now()}-${temp}`;
}