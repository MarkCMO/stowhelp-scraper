// scraper-googlemaps.js
//
// Free-stack Google Maps scraper. Uses playwright-core + @sparticuz/chromium-min
// so it fits inside Netlify's function size budget.
//
// Architecture notes
// ------------------
// Google Maps renders the left-rail result list client-side. We need a
// real browser that executes JS. playwright-core + @sparticuz/chromium-min
// is the smallest way to ship Chromium into a Lambda-style runtime (the
// "-min" variant downloads the binary from a CDN at cold-start so the
// function bundle stays under 50MB).
//
// Strategy
// --------
// Rather than drive the Maps UI (brittle - Google changes DOM monthly),
// we intercept the HTTPS responses Maps issues to its internal
// `/maps/rpc/listugcposts` + `/maps/preview/place` endpoints. Those are
// protobuf/JSON hybrids; the "search" response contains a big JSON array
// with fields in fixed positions.
//
// Fallback: if the internal-API interception fails (Google changes the
// URL path or payload shape), we fall back to scraping the rendered DOM
// with a set of stable-ish selectors. DOM selectors break more often,
// but the attribute names (aria-label, role=article) have been steady
// for 3+ years.
//
// Ethics / ToS
// ------------
// Google ToS prohibits automated scraping. We mitigate risk with:
//   - 4-8 second random jitter between searches
//   - 7-UA rotation
//   - Max 500 searches per run (brief spec: 500/day/IP)
//   - CAPTCHA detection with 30-minute pause on trip
//   - Nightly-only runs (10pm-6am US Eastern per brief)
// This is NOT a legal endorsement. When revenue allows, swap to the
// Places API or Apify. The USE_APIFY flag (not yet used) marks where.

let _playwright = null;

async function getBrowser() {
  if (!_playwright) {
    _playwright = require('playwright-core');
  }
  // GitHub Actions: Chromium installed via `npx playwright install chromium`
  // The PLAYWRIGHT_BROWSERS_PATH env points to the cached browser dir.
  // If CHROMIUM_PATH is set explicitly, use that.
  const executablePath = process.env.CHROMIUM_PATH || undefined;
  return _playwright.chromium.launch({
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-gpu',
      '--no-first-run',
      '--no-zygote',
      '--single-process',
      '--disable-extensions'
    ],
    executablePath,
    headless: true
  });
}

const { randomUserAgent, politeDelay, slugify } = require('./scraper-shared');

// --------------------------------------------------------------------
// Public: run one search query against Google Maps, return business list
// --------------------------------------------------------------------
/**
 * @param {string} query - e.g. "vehicle storage"
 * @param {string} city  - e.g. "Tampa"
 * @param {string} state - e.g. "FL"
 * @param {object} opts  - { maxResults?: number, pageTimeoutMs?: number }
 * @returns {Promise<{ok: boolean, results?: Array, error?: string, captcha?: boolean}>}
 */
async function searchGoogleMaps(query, city, state, opts) {
  opts = opts || {};
  const maxResults = opts.maxResults || 20;
  const pageTimeout = opts.pageTimeoutMs || 45000;
  const searchText = `${query} near ${city}, ${state}`;
  const url = 'https://www.google.com/maps/search/' + encodeURIComponent(searchText);

  let browser;
  try {
    browser = await getBrowser();
    const context = await browser.newContext({
      userAgent: randomUserAgent(),
      viewport: { width: 1280, height: 900 },
      locale: 'en-US',
      timezoneId: 'America/New_York',
      // Block heavy resources we don't need (images, fonts, media).
      // Cuts per-page bandwidth 80%+ and speeds up rendering.
      extraHTTPHeaders: { 'Accept-Language': 'en-US,en;q=0.9' }
    });
    // Resource filter: abort images/fonts/media. Keep scripts + xhr.
    await context.route('**/*', (route) => {
      const t = route.request().resourceType();
      if (t === 'image' || t === 'font' || t === 'media') return route.abort();
      return route.continue();
    });
    const page = await context.newPage();

    // Capture the internal search-response payload. Maps calls several
    // URLs; we want `/maps/rpc/listugcposts/search` or any URL whose
    // body looks like the result list (starts with `)]}'` anti-JSON
    // prefix and contains place IDs).
    const searchPayloads = [];
    page.on('response', async (res) => {
      try {
        const u = res.url();
        // Several Maps internal paths return the result list. The stable
        // marker is the response body starting with `)]}'`.
        if (!/\/maps\/(rpc|preview|search)/i.test(u)) return;
        if (!res.ok()) return;
        const text = await res.text();
        if (text.startsWith(")]}'")) searchPayloads.push({ url: u, body: text });
      } catch (_) { /* some responses aren't fetchable post-hoc */ }
    });

    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: pageTimeout });

    // Consent wall (some EU IPs / VPNs). Click "I agree" if present.
    try {
      await page.waitForSelector('form[action*="consent"] button, button[aria-label*="Accept"]', { timeout: 2000 });
      await page.click('form[action*="consent"] button, button[aria-label*="Accept"]');
    } catch (_) { /* no consent wall - normal */ }

    // CAPTCHA detection. Google returns a `/sorry/index` page when they
    // flag the request. Pause and report - the caller will handle the
    // 30-minute backoff.
    if (page.url().includes('/sorry/')) {
      await browser.close().catch(() => {});
      return { ok: false, captcha: true, error: 'captcha-challenge' };
    }

    // Wait for the result list to render. The stable selector is the
    // role=feed container that wraps all result cards.
    await page.waitForSelector('[role="feed"] > div > div[role="article"], [role="article"][jsaction]', { timeout: 10000 }).catch(() => {});

    // Scroll the feed a few times to force lazy-loaded results in.
    // Three scrolls gets us 20-60 results depending on the market.
    for (let i = 0; i < 4; i++) {
      await page.evaluate(() => {
        const feed = document.querySelector('[role="feed"]');
        if (feed) feed.scrollBy(0, 2000);
      });
      await politeDelay(700, 1200);
    }

    // Prefer intercepted JSON if we got any (richer data, includes place_id).
    let results = [];
    for (const payload of searchPayloads) {
      const parsed = tryParseSearchPayload(payload.body);
      if (parsed.length) {
        results = parsed;
        break;
      }
    }
    // DOM fallback if no intercepted payload worked.
    if (!results.length) {
      results = await extractFromDom(page);
    }

    await browser.close().catch(() => {});
    return { ok: true, results: results.slice(0, maxResults), usedInterception: !!results.length && searchPayloads.length > 0 };
  } catch (err) {
    if (browser) await browser.close().catch(() => {});
    return { ok: false, error: err.message || String(err) };
  }
}

// --------------------------------------------------------------------
// Parse the internal-API response. The body starts with `)]}'\n` then
// a huge nested array. Not all payloads are result lists; we detect by
// shape. Indices are Google's, stable over time but not documented.
// --------------------------------------------------------------------
function tryParseSearchPayload(text) {
  try {
    const json = JSON.parse(text.replace(/^\)\]\}'\n?/, ''));
    // The search response has an outer array with [null, resultsArray].
    // The resultsArray is at index [0][1][0..n]. Each result is itself
    // a deeply nested array where [14] is the business block.
    if (!Array.isArray(json)) return [];
    const resultsArray = (json[0] && json[0][1]) || (json[1] && json[1][0]) || null;
    if (!Array.isArray(resultsArray)) return [];

    const out = [];
    for (const entry of resultsArray) {
      if (!Array.isArray(entry)) continue;
      const biz = entry[14];
      if (!Array.isArray(biz)) continue;
      const name = biz[11] || null;
      const placeId = biz[78] || biz[10] || null;
      const phone = (biz[178] && biz[178][0] && biz[178][0][0]) || null;
      const website = (biz[7] && biz[7][0]) || null;
      const category = (biz[13] && biz[13][0]) || null;
      const address = (biz[39] && biz[39][1]) || (biz[2] && biz[2].join(', ')) || null;
      const rating = biz[4] && biz[4][7] ? biz[4][7] : null;
      const reviewCount = biz[4] && biz[4][8] ? biz[4][8] : null;
      const lat = biz[9] && biz[9][2] ? biz[9][2] : null;
      const lng = biz[9] && biz[9][3] ? biz[9][3] : null;
      if (name) {
        out.push({
          name,
          placeId,
          phone,
          website,
          category,
          address,
          rating,
          reviewCount,
          lat,
          lng,
          source: 'intercepted-json'
        });
      }
    }
    return out;
  } catch (_) {
    return [];
  }
}

// --------------------------------------------------------------------
// DOM fallback. Much less data (no placeId), but resilient when Google
// shuffles internal API indexes. Used when the intercepted payload path
// returns zero results.
// --------------------------------------------------------------------
async function extractFromDom(page) {
  return await page.evaluate(() => {
    const cards = Array.from(document.querySelectorAll('[role="article"][jsaction], [role="feed"] > div > div[role="article"]'));
    return cards.map((card) => {
      const getTxt = (sel) => {
        const el = card.querySelector(sel);
        return el ? el.textContent.trim() : null;
      };
      const getAriaLabel = () => card.getAttribute('aria-label') || (card.querySelector('[aria-label]') || {}).getAttribute ? card.querySelector('[aria-label]').getAttribute('aria-label') : null;

      const name = getTxt('.fontHeadlineSmall') || getTxt('[role="heading"]') || getAriaLabel();
      // Link to Maps listing - extract place_id-like segment from href
      const href = (card.querySelector('a[href*="/maps/place/"]') || {}).href || '';
      const placeIdMatch = href.match(/!1s(0x[a-f0-9]+:0x[a-f0-9]+)/i);
      const placeId = placeIdMatch ? placeIdMatch[1] : null;

      // Phone / rating are in aria-labels or specific spans
      const ratingEl = card.querySelector('[role="img"][aria-label*="stars"]');
      let rating = null;
      let reviewCount = null;
      if (ratingEl) {
        const label = ratingEl.getAttribute('aria-label') || '';
        const m = label.match(/([\d.]+)\s*stars?\s*(\d+)?/i);
        if (m) {
          rating = parseFloat(m[1]);
          reviewCount = m[2] ? parseInt(m[2], 10) : null;
        }
      }

      // Website button: anchor with data-value="Website" or aria-label="Website"
      const websiteEl = card.querySelector('a[aria-label^="Visit"], a[data-value="Website"]');
      const website = websiteEl ? websiteEl.href : null;

      // Phone link: a[href^="tel:"] or a span-labelled element
      const phoneEl = card.querySelector('a[href^="tel:"], [aria-label^="Phone:"]');
      let phone = null;
      if (phoneEl) {
        phone = (phoneEl.href || '').replace(/^tel:/, '') || (phoneEl.getAttribute('aria-label') || '').replace(/^Phone:\s*/i, '');
      }

      return {
        name,
        placeId,
        phone: phone || null,
        website: website || null,
        rating,
        reviewCount,
        googleMapsUrl: href || null,
        source: 'dom-fallback'
      };
    }).filter((r) => r && r.name);
  });
}

// --------------------------------------------------------------------
// Netlify HTTP handler: admin can trigger a single-query test scrape.
// POST /api/scraper-googlemaps { query, city, state, maxResults }
// Small test runs only; the scheduled background function does bulk.
// --------------------------------------------------------------------
exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') return { statusCode: 200, headers: cors(), body: '' };
  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, headers: cors(), body: JSON.stringify({ error: 'POST only' }) };
  }
  try {
    const { requirePermission } = require('./admin-auth');
    const auth = await requirePermission(event, 'listings.edit');
    if (auth && auth.reject) return auth.reject;

    const { query, city, state, maxResults } = JSON.parse(event.body || '{}');
    if (!query || !city || !state) {
      return { statusCode: 400, headers: cors(), body: JSON.stringify({ error: 'query, city, state required' }) };
    }
    const out = await searchGoogleMaps(query, city, state, { maxResults: maxResults || 10 });
    return { statusCode: 200, headers: cors(), body: JSON.stringify(out) };
  } catch (err) {
    console.error('scraper-googlemaps error:', err);
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

module.exports.searchGoogleMaps = searchGoogleMaps;
