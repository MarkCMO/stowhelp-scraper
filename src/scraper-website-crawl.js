// scraper-website-crawl.js
//
// Given a business website URL, deep-crawl a handful of high-signal paths
// to extract contact email, phone, owner/manager name, and social links.
// No headless browser - plain fetch + cheerio. Works within Netlify sync
// function limits (10s, 50MB).
//
// Exported as both:
//   - `crawlBusinessWebsite(url, opts)` for direct use by the scraper
//     orchestrator (scraper-run-background.js)
//   - a Netlify HTTP handler so we can one-off crawl a URL from the admin
//     dashboard (POST /api/scraper-website-crawl { url }).
//
// Rate-limited at the caller level (scraper-run sets the cadence). Here
// we only enforce a per-target courtesy: at most 2 concurrent requests to
// the same domain and a 1-2s jitter between hops within a single crawl.

const cheerio = require('cheerio');
const {
  randomUserAgent,
  politeDelay,
  extractEmails,
  extractPhones,
  extractOwners,
  pickBestEmail,
  formatPhone,
  extractDomain,
  CONTACT_PATHS
} = require('./scraper-shared');

// A single crawl visits at most this many paths before giving up. Keeps
// us inside the 10-second function timeout even for slow origins.
const MAX_PATHS_PER_SITE = 6;

// Per-request timeout. If a page takes longer than this, move on.
const REQUEST_TIMEOUT_MS = 6000;

// Max bytes we'll buffer from any one page. 2MB is plenty for an HTML
// doc and protects us from PDF/zip traps that happen to match a path.
const MAX_BYTES = 2 * 1024 * 1024;

// --------------------------------------------------------------------
// HTTP with timeout + size cap + UA rotation
// --------------------------------------------------------------------
async function fetchHtml(url) {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), REQUEST_TIMEOUT_MS);
  try {
    const res = await fetch(url, {
      method: 'GET',
      headers: {
        'User-Agent': randomUserAgent(),
        'Accept': 'text/html,application/xhtml+xml',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Cache-Control': 'no-cache'
      },
      redirect: 'follow',
      signal: ac.signal
    });
    if (!res.ok) return { ok: false, status: res.status, html: '' };
    // Content-type gate: skip PDFs/images/archives even if the path looks
    // innocent. Some small biz sites use /about as an image gallery.
    const ct = (res.headers.get('content-type') || '').toLowerCase();
    if (!ct.includes('html') && !ct.includes('text')) {
      return { ok: false, status: res.status, html: '', skippedContentType: ct };
    }
    // Read with a byte cap to avoid hangs on streaming endpoints.
    const reader = res.body && res.body.getReader ? res.body.getReader() : null;
    if (!reader) {
      const txt = await res.text();
      return { ok: true, status: res.status, html: txt.slice(0, MAX_BYTES) };
    }
    let received = 0;
    const chunks = [];
    while (received < MAX_BYTES) {
      const { done, value } = await reader.read();
      if (done) break;
      received += value.byteLength;
      chunks.push(value);
    }
    try { reader.cancel(); } catch (_) {}
    const buf = Buffer.concat(chunks.map((c) => Buffer.from(c)));
    return { ok: true, status: res.status, html: buf.toString('utf8') };
  } catch (err) {
    return { ok: false, status: 0, html: '', error: err.message };
  } finally {
    clearTimeout(t);
  }
}

// --------------------------------------------------------------------
// Page parser: pull everything we can from a single fetched HTML doc
// --------------------------------------------------------------------
function parsePage(html, pageUrl) {
  const $ = cheerio.load(html);
  const result = {
    emails: [],
    phones: [],
    owners: [],
    socials: {},
    foundOn: pageUrl
  };
  if (!html) return result;

  // Mailto / tel links are the most reliable source (zero false positives).
  $('a[href^="mailto:"]').each((_, el) => {
    const href = $(el).attr('href') || '';
    const email = href.replace(/^mailto:/i, '').split('?')[0].trim();
    if (email) result.emails.push(email.toLowerCase());
  });
  $('a[href^="tel:"]').each((_, el) => {
    const href = $(el).attr('href') || '';
    const raw = href.replace(/^tel:/i, '').trim();
    const formatted = formatPhone(raw);
    if (formatted) result.phones.push(formatted);
  });

  // Social links: grab href of any <a> whose hostname matches a known
  // social domain. Used for listing social_* fields.
  const socialMap = {
    facebook: /(facebook\.com|fb\.com|fb\.me)/i,
    instagram: /(instagram\.com|instagr\.am)/i,
    youtube: /(youtube\.com|youtu\.be)/i,
    tiktok: /tiktok\.com/i,
    twitter: /(twitter\.com|x\.com)/i,
    linkedin: /(linkedin\.com|lnkd\.in)/i,
    yelp: /(yelp\.com|yelp\.to)/i,
    googleBusiness: /(g\.page|maps\.app\.goo\.gl|maps\.google\.com)/i
  };
  $('a[href]').each((_, el) => {
    const href = $(el).attr('href') || '';
    if (!href.startsWith('http')) return;
    for (const key of Object.keys(socialMap)) {
      if (socialMap[key].test(href) && !result.socials[key]) {
        result.socials[key] = href;
      }
    }
  });

  // Strip script/style before text extraction - those contain a lot of
  // fake "emails" (analytics tokens, schema.org snippets) that we don't
  // want to consider even if they match the regex.
  $('script,style,noscript').remove();
  const bodyText = $('body').text().replace(/\s+/g, ' ').trim();

  // Regex-extracted candidates on top of the link-based finds.
  result.emails.push(...extractEmails(bodyText));
  result.phones.push(...extractPhones(bodyText));
  result.owners.push(...extractOwners(bodyText));

  // JSON-LD LocalBusiness schema often includes the owner/founder and a
  // cleaner email/phone than the body text. Parse every ld+json block.
  $('script[type="application/ld+json"]').each((_, el) => {
    try {
      const data = JSON.parse($(el).contents().text());
      const nodes = Array.isArray(data) ? data : [data];
      nodes.forEach((node) => walkLd(node, result));
    } catch (_) { /* ignore malformed JSON-LD */ }
  });

  // De-dupe
  result.emails = Array.from(new Set(result.emails.map((e) => e.toLowerCase())));
  result.phones = Array.from(new Set(result.phones));
  return result;
}

// Recursively walk a JSON-LD tree and pull any email/telephone/founder
// fields we find. schema.org LocalBusiness uses a @graph wrapper often.
function walkLd(node, out) {
  if (!node || typeof node !== 'object') return;
  if (Array.isArray(node)) { node.forEach((n) => walkLd(n, out)); return; }
  if (node['@graph']) walkLd(node['@graph'], out);
  if (typeof node.email === 'string') out.emails.push(node.email.toLowerCase());
  if (typeof node.telephone === 'string') {
    const f = formatPhone(node.telephone);
    if (f) out.phones.push(f);
  }
  if (node.founder) {
    const f = node.founder;
    const name = typeof f === 'string' ? f : f.name;
    if (name) out.owners.push({ name: String(name).trim(), confidence: 0.95, source: 'jsonld-founder' });
  }
  if (node.employee) walkLd(node.employee, out);
  if (node.contactPoint) walkLd(node.contactPoint, out);
}

// --------------------------------------------------------------------
// Orchestrate: fetch homepage + top contact/about paths until we have
// enough signal or run out of budget.
// --------------------------------------------------------------------
async function crawlBusinessWebsite(websiteUrl, opts) {
  opts = opts || {};
  const maxPaths = Math.min(MAX_PATHS_PER_SITE, opts.maxPaths || MAX_PATHS_PER_SITE);
  const started = Date.now();
  const domain = extractDomain(websiteUrl);
  if (!domain) return { ok: false, error: 'invalid-url' };

  const base = (websiteUrl.startsWith('http') ? websiteUrl : 'https://' + websiteUrl).replace(/\/+$/, '');
  const seen = new Set();
  const aggregate = {
    ok: true,
    domain,
    homepageUrl: base,
    emails: new Set(),
    phones: new Set(),
    owners: [],
    socials: {},
    visited: [],
    errors: []
  };

  // Homepage first - sometimes the footer has everything we need and no
  // deeper fetches are required.
  const pathsToTry = ['/'].concat(CONTACT_PATHS);
  for (const p of pathsToTry) {
    if (aggregate.visited.length >= maxPaths) break;
    // Early-exit if we already have a good email + owner + phone. Saves
    // the per-site budget for sites that are harder to extract from.
    if (aggregate.emails.size >= 1 && aggregate.owners.length >= 1 && aggregate.phones.size >= 1) break;
    if (seen.has(p)) continue;
    seen.add(p);

    const url = base + p;
    const { ok, status, html, error } = await fetchHtml(url);
    aggregate.visited.push({ url, status, ok, bytes: (html || '').length });
    if (!ok) {
      if (error) aggregate.errors.push({ url, error });
      continue;
    }
    const parsed = parsePage(html, url);
    parsed.emails.forEach((e) => aggregate.emails.add(e));
    parsed.phones.forEach((ph) => aggregate.phones.add(ph));
    parsed.owners.forEach((o) => aggregate.owners.push({ ...o, source: o.source || 'text-regex', url }));
    Object.keys(parsed.socials).forEach((k) => {
      if (!aggregate.socials[k]) aggregate.socials[k] = parsed.socials[k];
    });

    // Polite pause between hops on the same domain
    await politeDelay(800, 1800);

    // Global function-timeout guard: leave at least 1.5s buffer for the
    // caller to finish post-processing and respond to Netlify.
    if (Date.now() - started > 8000) break;
  }

  // Rank owner candidates by confidence, collapse duplicates.
  const ownerByName = new Map();
  aggregate.owners.forEach((o) => {
    const key = (o.name || '').toLowerCase();
    if (!key) return;
    const existing = ownerByName.get(key);
    if (!existing || existing.confidence < o.confidence) ownerByName.set(key, o);
  });
  const owners = Array.from(ownerByName.values()).sort((a, b) => b.confidence - a.confidence);

  return {
    ok: true,
    domain,
    homepageUrl: base,
    email: pickBestEmail(Array.from(aggregate.emails)),
    allEmails: Array.from(aggregate.emails),
    phone: Array.from(aggregate.phones)[0] || null,
    allPhones: Array.from(aggregate.phones),
    owner: owners[0] || null,
    allOwners: owners,
    socials: aggregate.socials,
    pagesVisited: aggregate.visited.length,
    errors: aggregate.errors.slice(0, 3),
    tookMs: Date.now() - started
  };
}

// --------------------------------------------------------------------
// Netlify HTTP handler - lets the admin dashboard manually re-crawl a
// business's website by slug. Auth gated by admin permission check.
// --------------------------------------------------------------------
exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') return { statusCode: 200, headers: cors(), body: '' };
  if (event.httpMethod !== 'POST' && event.httpMethod !== 'GET') {
    return { statusCode: 405, headers: cors(), body: JSON.stringify({ error: 'POST or GET only' }) };
  }
  try {
    // Admin-only; reuse existing auth.
    const { requirePermission } = require('./admin-auth');
    const auth = await requirePermission(event, 'listings.edit');
    if (auth && auth.reject) return auth.reject;

    let url;
    if (event.httpMethod === 'GET') {
      url = (event.queryStringParameters || {}).url;
    } else {
      const body = JSON.parse(event.body || '{}');
      url = body.url;
    }
    if (!url) return { statusCode: 400, headers: cors(), body: JSON.stringify({ error: 'url required' }) };

    const result = await crawlBusinessWebsite(url, { maxPaths: 6 });
    return { statusCode: 200, headers: cors(), body: JSON.stringify(result) };
  } catch (err) {
    console.error('scraper-website-crawl error:', err);
    return { statusCode: 500, headers: cors(), body: JSON.stringify({ error: err.message || 'Server error' }) };
  }
};

function cors() {
  return {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type, X-Admin-Key',
    'Content-Type': 'application/json'
  };
}

module.exports.crawlBusinessWebsite = crawlBusinessWebsite;
