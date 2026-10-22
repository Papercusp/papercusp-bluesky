'use strict';
const { createHash } = require('node:crypto');
const manifest = require('./papercusp.json');
const API = 'https://bsky.social/xrpc/';
const COLLECTION = 'app.bsky.feed.post';
const STREAMS = ['notifications', 'feed'];
const str = (v) => typeof v === 'string' ? v : '';
const object = (v) => !!v && typeof v === 'object' && !Array.isArray(v);
const fault = (kind, message, data) => Object.assign(new Error(`bluesky:${message}`), { kind }, data || {}, data ? { data } : {});
function recordRef(uri) {
  const m = /^at:\/\/(did:[^/\s?#]+)\/app\.bsky\.feed\.post\/([A-Za-z0-9._~:-]+)$/.exec(str(uri));
  if (!m) throw fault('transient', 'invalid_post_uri');
  return { repo: m[1], collection: COLLECTION, rkey: m[2] };
}
function strongRef(raw) {
  if (!object(raw) || !str(raw.cid)) throw fault('transient', 'invalid_strong_ref');
  recordRef(raw.uri); return { uri: raw.uri, cid: raw.cid };
}
function configOf(raw) {
  if (!/^did:[a-z0-9]+:[^\s/?#]+$/.test(str(raw?.accountId))) throw fault('auth', 'connected_account_missing');
  const bounded = (key, max, fallback) => { const n = raw[key] ?? fallback; if (!Number.isInteger(n) || n < 1 || n > max) throw fault('transient', `invalid_${key}`); return n; };
  return { accountId: raw.accountId, pageSize: bounded('pageSize', 100, 50), maxPages: bounded('maxPages', 1000, 200) };
}
async function xrpc(host, source, method, params, body) {
  const url = new URL(method, API);
  for (const [key, value] of Object.entries(params || {})) url.searchParams.set(key, value);
  let response;
  try { response = await host.fetch({ source, request: { url: url.href, method: body ? 'POST' : 'GET',
    ...(body ? { headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) } : {}) } }); }
  catch (e) { if (body) throw fault('transient', 'write_unconfirmed_do_not_retry', { retrySafe: false }); throw e; }
  let result; try { result = JSON.parse(response.body); } catch { result = null; }
  if (response.status === 401 || response.status === 403) throw fault('auth', 'session_refused');
  if (response.status === 429) {
    const delay = Number(response.headers?.['retry-after']);
    throw fault('rate-limited', 'throttled', Number.isFinite(delay) && delay > 0 ? { retryAfterSeconds: Math.ceil(delay) } : {});
  }
  if (result?.error === 'InvalidSwap') throw fault('transient', 'invalid_swap', { xrpc: 'InvalidSwap' });
  if (response.status < 200 || response.status >= 300 || result?.error || (!result && response.body)) {
    throw fault('transient', body ? 'write_unconfirmed_do_not_retry' : 'request_failed', body ? { retrySafe: false } : {});
  }
  return result;
}
function marker(row, stream) {
  const value = stream === 'feed' ? row?.post : row;
  if (!object(value) || !str(value.cid) || !/^at:\/\/[^\s]+$/.test(str(value.uri))) throw fault('transient', 'malformed_history_identity');
  return `${value.uri}@${value.cid}`;
}
function normalize(row, stream, accountId) {
  if (stream === 'notifications' && !['mention', 'reply'].includes(row.reason)) return null;
  if (stream === 'feed' && row.reason) return null; // A repost is not a post authored by this account.
  const value = stream === 'feed' ? row.post : row;
  const ref = strongRef(value); const record = value.record;
  if (!object(record) || record.$type !== COLLECTION || typeof record.text !== 'string' || !str(value.author?.did)) throw fault('transient', 'malformed_post');
  if (stream === 'feed' && value.author.did !== accountId) throw fault('transient', 'foreign_author_feed');
  const occurredAt = value.indexedAt || record.createdAt;
  if (!str(occurredAt) || !Number.isFinite(Date.parse(occurredAt))) throw fault('transient', 'malformed_timestamp');
  const reply = record.reply ? { root: strongRef(record.reply.root), parent: strongRef(record.reply.parent) } : null;
  const event = stream === 'notifications' ? row.reason : reply ? 'reply' : 'post';
  const payload = { id: ref.uri, uri: ref.uri, cid: ref.cid, text: record.text, platform: 'bluesky', kind: event,
    author: str(value.author.handle) || value.author.did, occurredAt,
    parentUri: ref.uri, parentCid: ref.cid, rootUri: reply?.root.uri || ref.uri, rootCid: reply?.root.cid || ref.cid,
    ...(reply ? { replyToId: reply.parent.uri } : {}),
    url: `https://bsky.app/profile/${encodeURIComponent(value.author.did)}/post/${recordRef(ref.uri).rkey}` };
  return { datatype: 'social-post', nativeId: ref.uri, version: `${ref.uri}@${ref.cid}`, payload, event, occurredAt };
}
function stateOf(cursor, accountId) {
  if (!cursor) return { v: 1, accountId, phase: 0, after: null, pages: 0, heads: {}, newest: null, anchors: [], emitted: [] };
  let s; try { s = JSON.parse(cursor); } catch { throw fault('cursor-expired', 'invalid_cursor'); }
  if (!object(s) || s.v !== 1 || !Number.isInteger(s.phase) || s.phase < 0 || s.phase > 2 || !object(s.heads) ||
    !Array.isArray(s.anchors) || !Array.isArray(s.emitted) || !Number.isInteger(s.pages) || s.pages < 0 ||
    !(s.after === null || typeof s.after === 'string') || !(s.newest === null || typeof s.newest === 'string')) throw fault('cursor-expired', 'invalid_cursor');
  if (s.accountId !== accountId) return stateOf(null, accountId);
  if (s.phase === 2) return { ...s, phase: 0, after: null, pages: 0, newest: null, anchors: [], emitted: [] };
  return s;
}
async function syncPage(request, host) {
  const c = configOf(request.config); const s = stateOf(request.cursor, c.accountId); const stream = STREAMS[s.phase];
  const body = await xrpc(host, request.source, stream === 'feed' ? 'app.bsky.feed.getAuthorFeed' : 'app.bsky.notification.listNotifications', {
    limit: String(c.pageSize), ...(stream === 'feed' ? { actor: c.accountId, filter: 'posts_with_replies', includePins: 'false' } : {}),
    ...(s.after ? { cursor: s.after } : {}),
  });
  const rows = body?.[stream];
  if (!Array.isArray(rows) || (body.cursor !== undefined && typeof body.cursor !== 'string')) throw fault('transient', 'malformed_page');
  const records = []; const emitted = new Set(s.emitted); let found = false;
  for (const row of rows) {
    const key = marker(row, stream); if (!s.newest) s.newest = key;
    if (s.heads[stream] === key) { found = true; break; }
    const record = normalize(row, stream, c.accountId);
    if (record && !emitted.has(record.version)) { records.push(record); emitted.add(record.version); }
  }
  const next = found ? null : body.cursor || null;
  if (next && (s.anchors.includes(next) || !rows.length)) throw fault('transient', 'paging_made_no_progress');
  if (next && s.pages + 1 >= c.maxPages) throw fault('transient', 'page_bound_exceeded');
  let lossRisk;
  if (!next && !found && s.heads[stream]) {
    const oldest = rows.at(-1); const at = (stream === 'feed' ? oldest?.post?.indexedAt : oldest?.indexedAt);
    if (!str(at) || !Number.isFinite(Date.parse(at))) throw fault('cursor-expired', 'history_marker_unreachable');
    lossRisk = { kind: 'retention-window-exceeded', oldestReachableAt: at, detail: `${stream} no longer contains the saved URI/CID; historical edits and missing records cannot be recovered from this REST window.` };
  }
  if (next) { s.after = next; s.pages += 1; s.anchors.push(next); }
  else { if (s.newest) s.heads[stream] = s.newest; s.phase += 1; s.after = null; s.pages = 0; s.newest = null; s.anchors = []; }
  s.emitted = s.phase === 2 ? [] : [...emitted];
  return { records, nextCursor: JSON.stringify(s), hasMore: s.phase < 2,
    catchUp: request.cursor ? { path: 'backfill', reason: 'no-replay-supported' } : { path: 'cold-start' }, ...(lossRisk ? { lossRisk } : {}) };
}
function targetOf(target) {
  if (target?.platform !== 'bluesky') throw fault('transient', 'wrong_target');
  if (!('externalId' in target)) return target;
  if (!object(target.payload) || typeof target.externalId !== 'string') throw fault('transient', 'invalid_stored_target');
  const p = target.payload;
  const parentUri = str(p.parentUri) || str(p.uri) || target.externalId;
  const parentCid = str(p.parentCid) || str(p.cid);
  if (!!str(p.rootUri) !== !!str(p.rootCid)) throw fault('transient', 'invalid_strong_ref');
  return { parentUri, parentCid, rootUri: str(p.rootUri) || parentUri, rootCid: str(p.rootCid) || parentCid };
}
async function invoke(request, host) {
  const c = configOf(request.config); const a = request.args || {};
  if (!manifest.provider.capabilities.includes(request.capability)) throw fault('transient', 'unsupported_capability');
  if (request.capability === 'social.delete') {
    const t = targetOf(a.target);
    const target = strongRef({ uri: t.parentUri, cid: t.parentCid }); const ref = recordRef(target.uri);
    if (ref.repo !== c.accountId) throw fault('auth', 'cannot_delete_another_account');
    try { await xrpc(host, request.source, 'com.atproto.repo.deleteRecord', null, { ...ref, swapRecord: target.cid }); return { deleted: true }; }
    catch (e) { if (e.xrpc === 'InvalidSwap') return { deleted: false, detail: 'The exact record version is absent or changed.' }; throw e; }
  }
  if (a.visibility && a.visibility !== 'public') throw fault('transient', 'visibility_unsupported');
  if (typeof a.text !== 'string' || !a.text.trim() || a.text.length > 3000 || [...new Intl.Segmenter(undefined, { granularity: 'grapheme' }).segment(a.text)].length > 300) throw fault('transient', 'invalid_post_text');
  if (!str(a.idempotencyKey)) throw fault('transient', 'host_idempotency_key_required');
  if (a.media?.length) throw fault('transient', 'media_unsupported');
  let reply;
  if (request.capability === 'social.reply') {
    const t = targetOf(a.target);
    reply = { root: strongRef({ uri: t.rootUri, cid: t.rootCid }), parent: strongRef({ uri: t.parentUri, cid: t.parentCid }) };
  }
  const rkey = `pc${createHash('sha256').update(JSON.stringify([request.source, a.idempotencyKey, a.text, reply || null])).digest('hex')}`;
  const record = { $type: COLLECTION, text: a.text, createdAt: a.createdAt || new Date().toISOString(), ...(reply ? { reply } : {}) };
  if (!Number.isFinite(Date.parse(record.createdAt))) throw fault('transient', 'invalid_created_at');
  const created = await xrpc(host, request.source, 'com.atproto.repo.createRecord', null, { repo: c.accountId, collection: COLLECTION, rkey, validate: true, record });
  const ref = strongRef(created);
  if (ref.uri !== `at://${c.accountId}/${COLLECTION}/${rkey}`) throw fault('transient', 'created_record_mismatch', { retrySafe: false });
  return { externalId: ref.uri, ref: `bluesky:${ref.uri}`, url: `https://bsky.app/profile/${encodeURIComponent(c.accountId)}/post/${rkey}` };
}
module.exports.providerAdapter = { describe: () => manifest.provider, syncPage, invoke };
