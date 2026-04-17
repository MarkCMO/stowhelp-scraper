// scraper-shared.js - utilities used across the free-stack scraper.
//
// No runtime dependencies beyond what's already in package.json. These
// helpers are pure functions or thin wrappers so they're safe to import
// from both sync functions and the 15-min scheduled background function.

// ----------------------------------------------------------------------
// User-agent rotation
// Small curated pool of current desktop Chrome/Firefox/Safari UAs. A
// 10-line hardcoded list is more reliable than fetching a remote UA list
// every run. Rotate by picking one at random per request.
// ----------------------------------------------------------------------
const USER_AGENTS = [
  // Chrome 131 (Windows / Mac / Linux)
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
  // Firefox 133
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 14.7; rv:133.0) Gecko/20100101 Firefox/133.0',
  // Safari 18
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15',
  // Edge 131
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0'
];

function randomUserAgent() {
  return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
}

// ----------------------------------------------------------------------
// Delays with jitter
// Every request gets a random pause in [minMs, maxMs]. Brief spec:
// 3-7s for Google Maps, 8s+ for Google Search, 1-2s for website crawls.
// ----------------------------------------------------------------------
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function jitter(minMs, maxMs) {
  return Math.floor(minMs + Math.random() * (maxMs - minMs));
}

async function politeDelay(minMs, maxMs) {
  await sleep(jitter(minMs, maxMs));
}

// ----------------------------------------------------------------------
// Regex patterns (single source of truth)
// ----------------------------------------------------------------------
// RFC 5322-lite. Good enough for the "find an email on this page" job.
// Deliberately excludes query strings and common false-positives like
// "@2x" in image filenames.
const EMAIL_RE = /\b([a-zA-Z0-9._%+-]+)@([a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)+)\b/g;

// North American phone. Accepts (xxx) xxx-xxxx, xxx-xxx-xxxx, xxx.xxx.xxxx,
// with optional +1 prefix. Rejects single long runs like social security
// numbers by requiring the three-part break pattern.
const PHONE_RE = /(?:\+?1[-.\s]?)?\(?([2-9]\d{2})\)?[-.\s]?([2-9]\d{2})[-.\s]?(\d{4})\b/g;

// Owner/title pattern. Finds 2-3 word proper-case names adjacent to a
// title keyword. Intentionally strict to avoid "Contact our Owner's
// Circle" false positives. Returns name in capture group 1.
const OWNER_RE = /\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})[,\s]+(?:Owner|Founder|Co-?Founder|President|Manager|Director|Principal|CEO|CFO|COO|General\s+Manager)\b/g;
const TITLE_FIRST_RE = /\b(?:Owner|Founder|Co-?Founder|President|Manager|Director|Principal|CEO|CFO|COO|General\s+Manager)[:\s,-]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})\b/g;

// "Founded by X" / "Started by X" / "Run by X"
const FOUNDED_BY_RE = /\b(?:Founded|Started|Run|Owned|Operated|Led)\s+by\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})\b/g;

// ----------------------------------------------------------------------
// Text extraction helpers
// ----------------------------------------------------------------------

// Emails sometimes hide as "name [at] example [dot] com" or "name(at)example.com"
// to dodge scrapers. Normalize those before regex matching. We also strip
// zero-width joiners and similar unicode tricks.
function normalizeObfuscatedEmails(text) {
  if (!text) return '';
  return String(text)
    .replace(/\s*\[at\]\s*/gi, '@')
    .replace(/\s*\(at\)\s*/gi, '@')
    .replace(/\s+at\s+(?=[a-zA-Z0-9-]+\s*(?:\.|\[dot\]|\(dot\)))/gi, '@')
    .replace(/\s*\[dot\]\s*/gi, '.')
    .replace(/\s*\(dot\)\s*/gi, '.')
    .replace(/[\u200b-\u200d\ufeff]/g, '');
}

// Filter out emails that are clearly not business contacts: image filenames,
// sentry/bugsnag/intercom tokens, wordpress admin, tracking pixels. Kept
// conservative - if an address looks human, we keep it.
const EMAIL_BLOCKLIST = [
  /sentry\.io$/i,
  /wixpress\.com$/i,
  /godaddy\.com$/i,
  /wordpress\.com$/i,
  /\.png$/i,
  /\.jpg$/i,
  /\.webp$/i,
  /\.svg$/i,
  /example\./i,
  /domain\./i,
  /yoursite\./i,
  /placeholder/i,
  /noreply/i,
  /no-reply/i,
  /mailer-daemon/i,
  /postmaster/i,
  /webmaster@/i // usually a catch-all, not a contact
];

// Malformed-address patterns produced by websites that inline contact info
// without whitespace separators ("Phone 555-5555 Email info@x.com" glues
// together after normalization). These produced a real 11% bounce rate in
// production. Filter aggressively: a single bounce costs us deliverability
// reputation, a missed valid email just means one lead we don't capture.
const MALFORMED_LOCAL_PATTERNS = [
  /^\d{5,}/,                 // leading 5+ digits (ZIP or phone fragment)
  /^\d+[-.]\d+[-.]\d+/,      // phone fragments like "1.424.252.5075info"
  /\d{4,}[a-z]{4,}/i,        // digit run then letter run (e.g. "60616info", "9165sunport")
  /(^|\.)email[a-z]/i,       // "emailbrian", "anytime.emailphone..."
  /(^|\.)phone[a-z]/i,       // "phoneinfo", "emailphone..."
  /(^|\.)fax[a-z]/i,         // "faxinfo..."
  /[a-z]{3,}info$/i,         // "managementinfo", "kirklandstorageinfo"
  /(contact|call|text|anytime|weekend|weekday)[a-z]{3,}/i  // glued prefix words
];

function isUsableEmail(email) {
  if (!email || typeof email !== 'string') return false;
  const low = email.toLowerCase();
  if (low.length > 100) return false;
  // local-part sanity - the most common corruption source
  const at = low.indexOf('@');
  if (at < 1 || at > 64) return false;
  const local = low.slice(0, at);
  if (local.length < 1 || local.length > 64) return false;
  if (MALFORMED_LOCAL_PATTERNS.some((rx) => rx.test(local))) return false;
  return !EMAIL_BLOCKLIST.some((rx) => rx.test(low));
}

// De-dupe and score emails so we can pick the "best" contact. Scoring
// order: a named address (info/contact/sales/hello) beats a gmail-style
// personal, which beats anything with numbers trailing. Used to pick
// email when a page has several.
function scoreEmail(email) {
  const low = (email || '').toLowerCase();
  let score = 0;
  if (/^(info|contact|hello|sales|support|office|admin|team)@/i.test(low)) score += 40;
  if (/^(owner|gm|manager|booking|reservations)@/i.test(low)) score += 30;
  // custom domain (not gmail/yahoo/hotmail) is almost always the business's own
  if (!/(gmail|yahoo|hotmail|outlook|aol|icloud|me\.com|live\.com)\./i.test(low)) score += 20;
  if (!/\d/.test(low.split('@')[0] || '')) score += 5; // fewer numbers = cleaner
  return score;
}

function pickBestEmail(emails) {
  const usable = (emails || []).filter(isUsableEmail);
  if (!usable.length) return null;
  const unique = Array.from(new Set(usable.map((e) => e.toLowerCase())));
  unique.sort((a, b) => scoreEmail(b) - scoreEmail(a));
  return unique[0];
}

// Normalize a phone to (xxx) xxx-xxxx. Returns null if not a valid NA number.
function formatPhone(raw) {
  if (!raw) return null;
  const digits = String(raw).replace(/\D/g, '').replace(/^1(\d{10})$/, '$1');
  if (digits.length !== 10) return null;
  return '(' + digits.slice(0, 3) + ') ' + digits.slice(3, 6) + '-' + digits.slice(6);
}

function extractEmails(text) {
  const normalized = normalizeObfuscatedEmails(text);
  const out = new Set();
  let m;
  while ((m = EMAIL_RE.exec(normalized)) !== null) {
    if (isUsableEmail(m[0])) out.add(m[0].toLowerCase());
  }
  return Array.from(out);
}

function extractPhones(text) {
  const out = new Set();
  let m;
  while ((m = PHONE_RE.exec(String(text || ''))) !== null) {
    const formatted = formatPhone(m[0]);
    if (formatted) out.add(formatted);
  }
  return Array.from(out);
}

function extractOwners(text) {
  const found = [];
  let m;
  while ((m = OWNER_RE.exec(String(text || ''))) !== null) {
    found.push({ name: m[1].trim(), confidence: 0.9 });
  }
  while ((m = TITLE_FIRST_RE.exec(String(text || ''))) !== null) {
    found.push({ name: m[1].trim(), confidence: 0.85 });
  }
  while ((m = FOUNDED_BY_RE.exec(String(text || ''))) !== null) {
    found.push({ name: m[1].trim(), confidence: 0.75 });
  }
  // Dedup by name, keep highest confidence
  const byName = new Map();
  found.forEach((o) => {
    const existing = byName.get(o.name.toLowerCase());
    if (!existing || existing.confidence < o.confidence) byName.set(o.name.toLowerCase(), o);
  });
  return Array.from(byName.values()).sort((a, b) => b.confidence - a.confidence);
}

// ----------------------------------------------------------------------
// Slug helpers (match slugify semantics used in facility-submit.js so
// scraped rows get the same URL shape as owner-submitted ones)
// ----------------------------------------------------------------------
function slugify(text, maxLen) {
  if (!text) return '';
  const max = maxLen || 80;
  return String(text)
    .toLowerCase()
    .trim()
    .replace(/['’]/g, '')             // drop apostrophes: O'Brien -> obrien
    .replace(/[^a-z0-9\s-]/g, '')
    .replace(/\s+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '')
    .slice(0, max);
}

// Build the canonical listing slug the same way facility-submit.js does:
//   {namesSlug}-{citySlug}-{stateSlug}
// This keeps scraped + owner-submitted listings in one URL namespace.
function buildListingSlug(name, city, state) {
  return [slugify(name), slugify(city, 40), slugify(state, 20)]
    .filter(Boolean)
    .join('-')
    .replace(/-+/g, '-');
}

// LinkedIn People-search URL for a business owner hunt. Reps click from
// the CRM. Value of a pre-built URL is saving 30 seconds per lead.
function linkedinSearchUrl(businessName, city) {
  const q = [businessName, city].filter(Boolean).join(' ');
  return 'https://www.linkedin.com/search/results/people/?keywords=' + encodeURIComponent(q);
}

// ----------------------------------------------------------------------
// Domain helpers
// ----------------------------------------------------------------------
function extractDomain(url) {
  if (!url) return null;
  try {
    const u = new URL(url.startsWith('http') ? url : 'https://' + url);
    return u.hostname.replace(/^www\./, '');
  } catch (_) {
    return null;
  }
}

// True if domain is a free hosted-site builder where WHOIS returns the
// host, not the owner (Wix, Squarespace, WordPress.com, etc.). Skip WHOIS
// for these to save the lookup roundtrip.
const HOSTED_DOMAINS = [
  'wixsite.com', 'squarespace.com', 'weebly.com', 'wordpress.com',
  'godaddysites.com', 'site123.com', 'jimdofree.com', 'webnode.com',
  'blogspot.com', 'strikingly.com', 'business.site'
];

function isHostedBuilderDomain(domain) {
  if (!domain) return false;
  return HOSTED_DOMAINS.some((h) => domain === h || domain.endsWith('.' + h));
}

// Common paths where small businesses put contact info. Order matters -
// we hit /contact first because that's the fastest path to an email.
const CONTACT_PATHS = [
  '/contact',
  '/contact-us',
  '/contact.html',
  '/contactus',
  '/get-in-touch',
  '/about',
  '/about-us',
  '/about.html',
  '/aboutus',
  '/our-team',
  '/team',
  '/staff',
  '/leadership',
  '/meet-the-team',
  '/who-we-are'
];

module.exports = {
  randomUserAgent,
  sleep,
  jitter,
  politeDelay,
  EMAIL_RE,
  PHONE_RE,
  OWNER_RE,
  normalizeObfuscatedEmails,
  isUsableEmail,
  scoreEmail,
  pickBestEmail,
  formatPhone,
  extractEmails,
  extractPhones,
  extractOwners,
  slugify,
  buildListingSlug,
  linkedinSearchUrl,
  extractDomain,
  isHostedBuilderDomain,
  CONTACT_PATHS
};
