// scraper-run-background.js
//
// The orchestrator. Netlify runs this as a scheduled BACKGROUND function
// (15-minute timeout, fire-and-forget). Picks the next N items off the
// `scrape_queue` table, runs Google Maps search + website crawl + WHOIS
// for each, writes results into `listings` + `outreach`.
//
// Name ends in `-background.js` because Netlify uses the suffix to know
// this is a background function (long-lived, no caller waiting).
// Scheduling is wired in netlify.toml.
//
// Run budget per invocation (fits in 15 min with margin):
//   - 6 queue items max
//   - each item: 1 Google Maps search (~20s with scrolling) + up to 20
//     per-business enrichments (website crawl ~6s each, WHOIS ~2s each)
//   - at most 10 min of work, leaving 5 min safety margin
//
// Dedup:
//   - place_id is the primary key. We upsert by place_id.
//   - if a scraped row already exists with source='owner-submission' or
//     'claim', we do NOT overwrite - owner-entered data is authoritative.
//
// Error isolation:
//   - per-item try/catch; one failed query doesn't stop the batch.
//   - CAPTCHA response -> mark queue item for retry, exit early (trips
//     a 30-min pause at the next invocation level since cron only fires
//     hourly).

// Lazy-require Chromium-dependent modules. These MUST NOT be top-level
// because playwright-core / @sparticuz/chromium-min can crash at load
// time in some Lambda runtimes, killing the entire function before the
// handler is invoked.
let _searchGoogleMaps = null;
function searchGoogleMaps(...args) {
  if (!_searchGoogleMaps) {
    _searchGoogleMaps = require('./scraper-googlemaps').searchGoogleMaps;
  }
  return _searchGoogleMaps(...args);
}

// Lazy-require website-crawl and WHOIS modules too. cheerio (used by
// scraper-website-crawl) must be in package.json for NFT to bundle it,
// but we still lazy-load so the handler starts even if a dep is missing.
let _crawlBusinessWebsite = null;
function crawlBusinessWebsite(...args) {
  if (!_crawlBusinessWebsite) {
    _crawlBusinessWebsite = require('./scraper-website-crawl').crawlBusinessWebsite;
  }
  return _crawlBusinessWebsite(...args);
}

let _whoisLookup = null;
function whoisLookup(...args) {
  if (!_whoisLookup) {
    _whoisLookup = require('./scraper-whois').whoisLookup;
  }
  return _whoisLookup(...args);
}

// scraper-shared is pure JS helpers (no native bindings), safe to load eagerly
const { buildListingSlug, slugify, linkedinSearchUrl, extractDomain, isHostedBuilderDomain, politeDelay, randomUserAgent } = require('./scraper-shared');

const BATCH_SIZE = Number(process.env.SCRAPER_BATCH_SIZE || 10);
const PER_QUERY_MAX_BUSINESSES = Number(process.env.SCRAPER_PER_QUERY_MAX || 20);
const GLOBAL_DEADLINE_MS = 12 * 60 * 1000; // 12 min, leaves 3 min headroom in 15-min bg function

// ── HTTP-only fallback Google Maps scraper ──
// Fetches Google Maps search results via plain HTTP (no Chromium). Google
// embeds structured data in script tags as JSON. This is less rich than the
// browser-intercepted API data but works reliably in any Lambda runtime.
const https = require('https');
const http = require('http');

async function httpSearchGoogleMaps(query, city, state, opts) {
  opts = opts || {};
  const maxResults = opts.maxResults || 20;
  const searchText = `${query} near ${city}, ${state}`;
  const searchUrl = 'https://www.google.com/maps/search/' + encodeURIComponent(searchText);

  // Hard safety timeout - never hang longer than 30s total
  return new Promise((resolve) => {
    let resolved = false;
    const safeResolve = (val) => { if (!resolved) { resolved = true; resolve(val); } };
    const hardTimeout = setTimeout(() => safeResolve({ ok: false, error: 'hard-timeout-30s' }), 30000);

    const ua = randomUserAgent();
    const req = https.get(searchUrl, {
      headers: {
        'User-Agent': ua,
        'Accept': 'text/html,application/xhtml+xml',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'identity'
      },
      timeout: 15000
    }, (res) => {
      // Follow redirects (up to 1 hop)
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        res.resume(); // drain the redirect response body
        const loc = res.headers.location;
        const mod = loc.startsWith('https') ? https : (loc.startsWith('http') ? http : https);
        const req2 = mod.get(loc, {
          headers: { 'User-Agent': ua, 'Accept-Language': 'en-US,en;q=0.9', 'Accept-Encoding': 'identity' },
          timeout: 15000
        }, (res2) => collectAndParse(res2, maxResults, (val) => { clearTimeout(hardTimeout); safeResolve(val); }));
        req2.on('error', (e) => { clearTimeout(hardTimeout); safeResolve({ ok: false, error: 'redirect-error: ' + e.message }); });
        req2.on('timeout', () => { req2.destroy(); clearTimeout(hardTimeout); safeResolve({ ok: false, error: 'redirect-timeout' }); });
        return;
      }
      collectAndParse(res, maxResults, (val) => { clearTimeout(hardTimeout); safeResolve(val); });
    });
    req.on('error', (e) => { clearTimeout(hardTimeout); safeResolve({ ok: false, error: e.message }); });
    req.on('timeout', () => { req.destroy(); clearTimeout(hardTimeout); safeResolve({ ok: false, error: 'timeout' }); });
  });
}

function collectAndParse(res, maxResults, resolve) {
  let body = '';
  res.on('data', c => body += c);
  res.on('end', () => {
    // CAPTCHA detection
    if (body.includes('/sorry/') || body.includes('captcha')) {
      return resolve({ ok: false, captcha: true, error: 'captcha-challenge' });
    }

    const results = [];
    // Strategy 1: parse window.APP_INITIALIZATION_STATE embedded JSON
    // Google embeds search results in a JS variable in the HTML.
    try {
      // Look for embedded JSON arrays that contain business data.
      // The pattern is: `)]}'` followed by JSON, embedded in script tags.
      const scriptMatches = body.match(/\)\]\}'\n([^\n]+)/g) || [];
      for (const match of scriptMatches) {
        try {
          const jsonStr = match.replace(/^\)\]\}'\n?/, '');
          const json = JSON.parse(jsonStr);
          const parsed = parseGoogleMapsJson(json);
          if (parsed.length) {
            results.push(...parsed);
            break;
          }
        } catch (_) {}
      }
    } catch (_) {}

    // Strategy 2: regex extraction of business data from HTML
    if (!results.length) {
      // Extract business names, phones, addresses from Google's HTML
      // using common patterns in the page source.
      const namePattern = /aria-label="([^"]{3,80})"\s+[^>]*role="article"/g;
      let m;
      while ((m = namePattern.exec(body)) !== null && results.length < maxResults) {
        results.push({
          name: m[1].replace(/&amp;/g, '&').replace(/&#39;/g, "'"),
          placeId: null, phone: null, website: null,
          address: null, rating: null, reviewCount: null,
          lat: null, lng: null
        });
      }
    }

    // Strategy 3: look for structured data in the page
    if (!results.length) {
      const ldMatches = body.match(/"@type"\s*:\s*"LocalBusiness"[^}]+/g) || [];
      for (const ld of ldMatches) {
        try {
          const nameM = ld.match(/"name"\s*:\s*"([^"]+)"/);
          const phoneM = ld.match(/"telephone"\s*:\s*"([^"]+)"/);
          if (nameM) {
            results.push({
              name: nameM[1], placeId: null, phone: phoneM ? phoneM[1] : null,
              website: null, address: null, rating: null, reviewCount: null,
              lat: null, lng: null
            });
          }
        } catch (_) {}
      }
    }

    resolve({
      ok: results.length > 0,
      results: results.slice(0, maxResults),
      method: 'http-fallback',
      error: results.length === 0 ? 'no-results-parsed' : null
    });
  });
  res.on('error', () => resolve({ ok: false, error: 'response-error' }));
}

function parseGoogleMapsJson(json) {
  const out = [];
  try {
    if (!Array.isArray(json)) return out;
    // Walk the nested array looking for business data blocks
    const walk = (arr, depth) => {
      if (depth > 8 || !Array.isArray(arr)) return;
      // Business blocks have a name at index [11] and coordinates at [9]
      if (arr[11] && typeof arr[11] === 'string' && arr[9] && Array.isArray(arr[9])) {
        const name = arr[11];
        const placeId = arr[78] || arr[10] || null;
        const phone = (arr[178] && arr[178][0] && arr[178][0][0]) || null;
        const website = (arr[7] && arr[7][0]) || null;
        const rating = arr[4] && arr[4][7] ? arr[4][7] : null;
        const reviewCount = arr[4] && arr[4][8] ? arr[4][8] : null;
        const lat = arr[9][2] || null;
        const lng = arr[9][3] || null;
        const address = (arr[39] && arr[39][1]) || null;
        out.push({ name, placeId, phone, website, address, rating, reviewCount, lat, lng });
        return;
      }
      for (const child of arr) {
        if (Array.isArray(child)) walk(child, depth + 1);
      }
    };
    walk(json, 0);
  } catch (_) {}
  return out;
}

exports.handler = async (event) => {
  const started = Date.now();
  const deadline = started + GLOBAL_DEADLINE_MS;

  // ── Init Supabase + crash-proof logging FIRST ──
  // This runs before auth so we can log even if auth fails or the
  // function crashes. Background functions die silently otherwise.
  // NOTE: We must use require('./db') directly (not safeRequire) because
  // Netlify's NFT bundler can't trace dynamic require(variable) calls.
  let db = null;
  try { db = require('./db'); } catch (_) {}
  let sb = null;
  async function logScraperEvent(level, message, extra) {
    if (!sb) return;
    try {
      await sb.from('scraper_log').insert({
        level, message, extra: extra || null, created_at: new Date().toISOString()
      });
    } catch (_) { console.error('scraper-log-write-failed:', message); }
  }
  if (db && db.isEnabled && db.isEnabled()) {
    sb = db.raw();
    try { await logScraperEvent('info', 'scraper-run handler reached', {
      batchSize: BATCH_SIZE,
      httpMethod: event.httpMethod || 'none',
      hasHeaders: !!event.headers,
      isScheduled: isScheduledInvocation(event)
    }); } catch (_) {}
  }

  // Auth: GitHub Actions controls access - no auth check needed here.
  // The workflow only runs from the scheduled cron or manual dispatch.

  if (!sb) {
    console.error('scraper-run: Supabase not configured, skipping run');
    return { statusCode: 500, body: JSON.stringify({ error: 'supabase-not-configured' }) };
  }

  try { await logScraperEvent('info', 'scraper-run started (post-auth)', { batchSize: BATCH_SIZE }); }
  catch (_) {}

  const results = {
    started: new Date(started).toISOString(),
    batchSize: BATCH_SIZE,
    queueItems: [],
    totals: { queriesRun: 0, businessesFound: 0, businessesNew: 0, websiteHits: 0, whoisHits: 0, errors: 0, captchaHit: false }
  };

  try {
    // Claim the next batch of queue items. We use an optimistic claim:
    // flip status to 'running' and only process rows we successfully
    // flipped (concurrent invocations don't fight each other).
    // Also retry error items with < 3 attempts (transient failures like
    // Chromium browser crashes, timeouts, etc.)
    await sb.from('scrape_queue')
      .update({ status: 'pending' })
      .eq('status', 'error')
      .lt('attempts', 3);

    let { data: candidates, error: fetchErr } = await sb
      .from('scrape_queue')
      .select('*')
      .in('status', ['pending'])
      .order('priority', { ascending: true })
      .order('created_at', { ascending: true })
      .limit(BATCH_SIZE);
    if (fetchErr) throw fetchErr;
    if (!candidates || !candidates.length) {
      // ── Auto-replenish: seed fresh cities so the scraper never sits idle ──
      // When the queue empties, pick cities that haven't been scraped recently
      // (or ever) and queue them with a rotation of all 11 categories. This
      // ensures continuous discovery of new businesses without manual admin
      // intervention. Limits to 30 rows per refill (~5 invocations of work).
      try {
        const seeded = await autoReplenishQueue(sb);
        if (seeded > 0) {
          results.message = 'queue-refilled';
          results.seeded = seeded;
          // Re-fetch the freshly inserted rows and continue processing
          const { data: fresh } = await sb
            .from('scrape_queue')
            .select('*')
            .in('status', ['pending'])
            .order('priority', { ascending: true })
            .order('created_at', { ascending: true })
            .limit(BATCH_SIZE);
          if (fresh && fresh.length) {
            candidates = fresh;
          } else {
            return respond(results, 200);
          }
        } else {
          results.message = 'queue-empty';
          return respond(results, 200);
        }
      } catch (refillErr) {
        console.warn('scraper-run: auto-replenish error', refillErr.message);
        results.message = 'queue-empty';
        return respond(results, 200);
      }
    }

    for (const item of candidates) {
      if (Date.now() > deadline) { results.message = 'deadline-reached'; break; }
      if (results.totals.captchaHit) { results.message = 'captcha-backoff'; break; }

      // Claim the row. If someone else already claimed it, skip.
      const { data: claimed, error: claimErr } = await sb
        .from('scrape_queue')
        .update({ status: 'running', started_at: new Date().toISOString(), attempts: (item.attempts || 0) + 1 })
        .eq('id', item.id)
        .eq('status', 'pending')
        .select()
        .single();
      if (claimErr || !claimed) continue;

      const itemResult = { id: item.id, state: item.state, city: item.city, category: item.category, found: 0, new: 0, error: null };
      try {
        await logScraperEvent('info', `Processing: ${item.category} in ${item.city}, ${item.state}`, { itemId: item.id });
        // Try Chromium-based scraper first; fall back to HTTP-only if it
        // fails (Chromium binary can't launch in some Lambda runtimes).
        let gm;
        try {
          gm = await searchGoogleMaps(item.category, item.city, item.state, { maxResults: PER_QUERY_MAX_BUSINESSES });
          // If Chromium returned 0 results (browser crash, timeout, etc.),
          // try HTTP fallback before giving up.
          if (!gm.ok || !(gm.results || []).length) {
            try { await logScraperEvent('info', 'Chromium returned 0 results, trying HTTP fallback', { city: item.city, error: gm.error }); } catch (_) {}
            const httpGm = await httpSearchGoogleMaps(item.category, item.city, item.state, { maxResults: PER_QUERY_MAX_BUSINESSES });
            if (httpGm.ok && (httpGm.results || []).length > 0) gm = httpGm;
          }
        } catch (chromiumErr) {
          try { await logScraperEvent('warn', 'Chromium threw, falling back to HTTP scraper', { error: chromiumErr.message, city: item.city, state: item.state }); } catch (_) {}
          gm = await httpSearchGoogleMaps(item.category, item.city, item.state, { maxResults: PER_QUERY_MAX_BUSINESSES });
        }
        results.totals.queriesRun++;
        await logScraperEvent('info', `Search result: ${gm.ok ? 'OK' : 'FAIL'} method=${gm.method || 'chromium'} results=${(gm.results || []).length}`, {
          city: item.city, error: gm.error || null, captcha: gm.captcha || false
        });

        if (gm.captcha) {
          results.totals.captchaHit = true;
          itemResult.error = 'captcha';
          await sb.from('scrape_queue').update({
            status: 'pending', // put it back for retry later
            last_error: 'captcha-challenge',
            finished_at: new Date().toISOString()
          }).eq('id', item.id);
          results.queueItems.push(itemResult);
          break;
        }
        if (!gm.ok) {
          throw new Error(gm.error || 'gm-unknown-error');
        }

        const businesses = gm.results || [];
        itemResult.found = businesses.length;

        for (const biz of businesses) {
          if (Date.now() > deadline) break;
          try {
            const processed = await processBusiness(sb, biz, item);
            if (processed.isNew) {
              results.totals.businessesNew++;
              itemResult.new++;
            }
            results.totals.businessesFound++;
            if (processed.websiteHit) results.totals.websiteHits++;
            if (processed.whoisHit) results.totals.whoisHits++;
          } catch (e) {
            results.totals.errors++;
            console.warn('scraper-run: per-business error', biz.name, e.message);
          }
          await politeDelay(400, 900); // gentle gap between businesses
        }

        await sb.from('scrape_queue').update({
          status: 'done',
          finished_at: new Date().toISOString(),
          results_count: itemResult.found,
          last_error: null
        }).eq('id', item.id);
      } catch (err) {
        itemResult.error = err.message;
        results.totals.errors++;
        await sb.from('scrape_queue').update({
          status: 'error',
          last_error: err.message,
          finished_at: new Date().toISOString()
        }).eq('id', item.id);
      }
      results.queueItems.push(itemResult);

      // Rate limit between queries - 2-4s for HTTP, 4-8s for Chromium
      await politeDelay(2000, 4000);
    }

    results.finished = new Date().toISOString();
    results.tookMs = Date.now() - started;
    return respond(results, 200);
  } catch (err) {
    console.error('scraper-run fatal:', err);
    try { await logScraperEvent('error', 'scraper-run fatal: ' + err.message, { stack: err.stack }); } catch (_) {}
    return respond({ error: err.message, results }, 500);
  }
};

// --------------------------------------------------------------------
// Per-business processing: upsert listing + enrich + push to outreach
// --------------------------------------------------------------------
async function processBusiness(sb, biz, queueItem) {
  if (!biz.name) return { isNew: false };

  const state = queueItem.state;
  const city = queueItem.city;
  const category = categoryToSlug(queueItem.category);
  const stateSlug = slugify(state);
  const citySlug = slugify(city);
  const slug = buildListingSlug(biz.name, city, state);

  // Check existing row. We key primarily on place_id; fall back to slug.
  let existing = null;
  if (biz.placeId) {
    const { data } = await sb.from('listings').select('slug,source,email,phone,owner_name,access_token,created_at').eq('place_id', biz.placeId).maybeSingle();
    existing = data || null;
  }
  if (!existing) {
    const { data } = await sb.from('listings').select('slug,source,email,phone,owner_name,access_token,created_at').eq('slug', slug).maybeSingle();
    existing = data || null;
  }
  // Don't overwrite owner-entered rows. Scraper never beats a human.
  if (existing && ['owner-submission', 'claim'].includes(existing.source)) {
    return { isNew: false, skipped: 'owner-authoritative' };
  }

  // Website deep-crawl (pure Node, runs in same function)
  let webResult = null;
  if (biz.website) {
    try {
      webResult = await crawlBusinessWebsite(biz.website, { maxPaths: 5 });
    } catch (e) { /* swallow; we'll mark needs_owner_research below */ }
  }

  // WHOIS fallback for owner name
  let whoisResult = null;
  let ownerSource = null;
  let ownerName = (webResult && webResult.owner && webResult.owner.name) || null;
  if (ownerName) ownerSource = (webResult.owner.source === 'jsonld-founder') ? 'website-jsonld' : 'website-text';
  if (!ownerName && biz.website) {
    const domain = extractDomain(biz.website);
    if (domain && !isHostedBuilderDomain(domain)) {
      try {
        whoisResult = await whoisLookup(domain);
        if (whoisResult && whoisResult.owner) {
          ownerName = whoisResult.owner;
          ownerSource = 'whois';
        }
      } catch (_whoisErr) { /* whois module unavailable or failed */ }
    }
  }

  const now = new Date().toISOString();
  const enriched = {
    slug,
    name: biz.name,
    place_id: biz.placeId || null,
    google_maps_url: biz.googleMapsUrl || (biz.placeId ? 'https://www.google.com/maps/place/?q=place_id:' + biz.placeId : null),
    lat: numOrNull(biz.lat),
    lng: numOrNull(biz.lng),
    phone: (webResult && webResult.phone) || biz.phone || null,
    website: biz.website || null,
    email: (webResult && webResult.email) || null,
    address: biz.address || null,
    city,
    state,
    state_slug: stateSlug,
    city_slug: citySlug,
    categories: [category].filter(Boolean),
    plan: 'free',
    status: 'unclaimed',
    source: 'scraped',
    owner_name: ownerName,
    owner_source: ownerSource,
    needs_owner_research: !ownerName,
    linkedin_search_url: linkedinSearchUrl(biz.name, city),
    crm_status: 'cold',
    rating: numOrNull(biz.rating) || 0,
    review_count: parseInt(biz.reviewCount || 0, 10) || 0,
    socials: (webResult && webResult.socials) || {},
    scraped_at: now,
    website_scraped_at: webResult ? now : null,
    updated_at: now
  };
  // Only set created_at / submitted_at on first insert so we don't
  // overwrite the original discovery time when re-scraping.
  if (!existing) {
    enriched.created_at = now;
    enriched.submitted_at = now;
  }

  // Always upsert by slug (the primary key). The place_id unique index is
  // partial (WHERE place_id IS NOT NULL) which PostgREST can't use for
  // ON CONFLICT. We already check for existing by place_id above, so dedup
  // is handled. If an existing row has the same place_id but different slug,
  // that was caught by the existing-row check earlier.
  const { error: upsertErr } = await sb.from('listings').upsert(enriched, { onConflict: 'slug' });
  if (upsertErr) throw upsertErr;

  // Fire immediate cold-outreach email if this is a NEW listing with an email.
  // The call is fire-and-forget: scraping continues even if the email API is
  // down. The stowhelp.com email-cron will catch any misses on its next tick.
  if (!existing && enriched.email && process.env.INTERNAL_API_KEY) {
    try {
      const res = await fetch('https://stowhelp.com/api/send-outreach-immediate', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Internal-Key': process.env.INTERNAL_API_KEY
        },
        body: JSON.stringify({ slug }),
        signal: AbortSignal.timeout(10000)
      });
      if (!res.ok) console.warn('immediate-outreach non-200 for', slug, res.status);
    } catch (e) {
      console.warn('immediate-outreach fetch failed for', slug, e.message);
    }
  }

  // Outreach row: one per scraped business, status='cold'. The CRM
  // dashboard reads from outreach; rep actions update it.
  try {
    await sb.from('outreach').upsert({
      slug,
      facility: biz.name,
      email: enriched.email,
      phone: enriched.phone,
      website: enriched.website,
      city,
      state,
      categories: enriched.categories,
      status: 'cold',
      rep: null,
      created_at: existing ? undefined : now,
      updated_at: now
    }, { onConflict: 'slug' });
  } catch (e) { /* outreach mirror is best-effort */ }

  // IndexNow queue - push the listing detail page + city page so
  // Bing/Google discover the new business on both surfaces.
  try {
    const listingUrl = `https://stowhelp.com/listing/${slug}`;
    const cityUrl = `https://stowhelp.com/${category}/${stateSlug}/${citySlug}`;
    await sb.from('pending_indexnow').upsert([
      { url: listingUrl, submitted: false },
      { url: cityUrl, submitted: false }
    ], { onConflict: 'url', ignoreDuplicates: true });
  } catch (_) {}

  return {
    isNew: !existing,
    websiteHit: !!(webResult && webResult.email),
    whoisHit: !!(whoisResult && whoisResult.owner)
  };
}

// --------------------------------------------------------------------
// Auto-replenish: seed fresh scrape_queue rows when queue empties
// --------------------------------------------------------------------
// Strategy: cycle through cities in us_cities table ordered by population
// (desc). For each city, queue a random category that hasn't been scraped
// for that city in the last 30 days. This naturally prioritizes big cities
// (more businesses to discover) and ensures every category gets coverage.
async function autoReplenishQueue(sb) {
  const MAX_SEED = 30; // rows to insert per refill

  // Categories to rotate through
  const CATEGORIES = [
    'rv storage', 'boat storage', 'car storage', 'motorcycle storage',
    'trailer storage', 'jet ski storage', 'atv storage', 'snowmobile storage',
    'kayak storage', 'golf cart storage', 'wine storage',
    'self storage', 'vehicle storage', 'classic car storage', 'outdoor vehicle storage'
  ];

  // Find cities ordered by population. We use the public.cities table
  // (or us_cities) to get real city/state pairs.
  const pubSb = require('@supabase/supabase-js').createClient(
    process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY,
    { auth: { persistSession: false } }
  );

  // Get recently scraped city+category combos to avoid re-queuing
  const thirtyDaysAgo = new Date(Date.now() - 30 * 86400000).toISOString();
  const { data: recentDone } = await sb
    .from('scrape_queue')
    .select('city, state, category')
    .gte('finished_at', thirtyDaysAgo)
    .eq('status', 'done')
    .limit(5000);
  const recentSet = new Set((recentDone || []).map(r =>
    `${(r.city||'').toLowerCase()}|${(r.state||'').toLowerCase()}|${(r.category||'').toLowerCase()}`
  ));

  // Also exclude anything currently pending or running
  const { data: activePending } = await sb
    .from('scrape_queue')
    .select('city, state, category')
    .in('status', ['pending', 'running'])
    .limit(2000);
  (activePending || []).forEach(r => {
    recentSet.add(`${(r.city||'').toLowerCase()}|${(r.state||'').toLowerCase()}|${(r.category||'').toLowerCase()}`);
  });

  // Pull top cities by population
  const { data: cities, error: cityErr } = await pubSb
    .from('cities')
    .select('city_name, state_name')
    .not('population', 'is', null)
    .order('population', { ascending: false })
    .limit(500);
  if (cityErr || !cities || !cities.length) {
    console.warn('autoReplenishQueue: no cities found', cityErr?.message);
    return 0;
  }

  // Build candidate rows: for each city, pick categories not recently scraped
  const rows = [];
  for (const c of cities) {
    if (rows.length >= MAX_SEED) break;
    // Shuffle categories so we don't always start with the same one
    const shuffled = [...CATEGORIES].sort(() => Math.random() - 0.5);
    for (const cat of shuffled) {
      if (rows.length >= MAX_SEED) break;
      const key = `${c.city_name.toLowerCase()}|${c.state_name.toLowerCase()}|${cat.toLowerCase()}`;
      if (recentSet.has(key)) continue;
      rows.push({
        city: c.city_name,
        state: c.state_name,
        category: cat,
        status: 'pending',
        priority: 5,
        attempts: 0,
        created_at: new Date().toISOString()
      });
      recentSet.add(key); // don't double-queue same combo in this batch
      break; // one category per city per refill for breadth
    }
  }

  if (!rows.length) return 0;

  // Use upsert with ignoreDuplicates so we don't error on pre-existing
  // (state, city, category) combos that landed in a non-done state.
  const { error: insertErr } = await sb.from('scrape_queue')
    .upsert(rows, { onConflict: 'state,city,category', ignoreDuplicates: true });
  if (insertErr) {
    console.warn('autoReplenishQueue: insert error', insertErr.message);
    return 0;
  }

  console.log(`autoReplenishQueue: seeded ${rows.length} new queue items`);
  return rows.length;
}

// --------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------
function categoryToSlug(category) {
  // Map brief's free-text categories to our 11 hardcoded category slugs.
  // Unknown categories collapse to 'car-storage' as a safe default that
  // still ranks for "vehicle storage" parent searches.
  const map = {
    'vehicle storage': 'car-storage',
    'car storage': 'car-storage',
    'rv storage': 'rv-storage',
    'boat storage': 'boat-storage',
    'motorcycle storage': 'motorcycle-storage',
    'classic car storage': 'car-storage',
    'outdoor vehicle storage': 'car-storage',
    'enclosed vehicle storage': 'car-storage',
    'self storage with vehicle parking': 'car-storage',
    'trailer storage': 'trailer-storage',
    'self storage': 'car-storage',
    'mini storage': 'car-storage',
    'climate controlled storage': 'car-storage',
    'storage units': 'car-storage',
    'moving and storage': 'car-storage',
    'atv storage': 'atv-storage',
    'jet ski storage': 'jet-ski-storage',
    'snowmobile storage': 'snowmobile-storage',
    'kayak storage': 'kayak-storage',
    'golf cart storage': 'golf-cart-storage',
    'wine storage': 'wine-storage'
  };
  const key = String(category || '').toLowerCase().trim();
  return map[key] || 'car-storage';
}

function numOrNull(v) {
  if (v == null || v === '') return null;
  const n = parseFloat(v);
  return Number.isFinite(n) ? n : null;
}

function safeRequire(mod) {
  try { return require(mod); } catch (_) { return null; }
}

function isScheduledInvocation(event) {
  // Netlify scheduled functions arrive as POST with { next_run: "..." } in body.
  // Headers may include x-nf-* markers, but the body check is most reliable.
  const h = event.headers || {};
  if (h['x-scheduled-function'] || h['x-nf-scheduled']) return true;
  if (event.next_run) return true;
  // Parse body for next_run (Netlify sends JSON body with next_run field)
  if (event.body) {
    try {
      const b = typeof event.body === 'string' ? JSON.parse(event.body) : event.body;
      if (b && b.next_run) return true;
    } catch (_) {}
  }
  // Netlify scheduled background functions also lack typical browser headers
  // and have no query string. If httpMethod=POST and no admin key header,
  // it's very likely a scheduled invocation (not a manual trigger).
  if (event.httpMethod === 'POST' && !h['x-admin-key'] && !h['authorization']) return true;
  return false;
}

function respond(body, status) {
  return {
    statusCode: status || 200,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body)
  };
}
