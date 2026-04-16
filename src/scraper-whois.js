// scraper-whois.js
//
// WHOIS fallback for owner-name discovery. Small storage businesses
// frequently register domains under a personal name; when the website
// deep-crawl fails to turn up an owner, WHOIS is free, public, and often
// the fastest path to "Mark Gabrielli, owner of Acme RV Storage."
//
// Uses the `whois-json` npm package (depends on the OS `whois` binary in
// some environments, but whois-json also has a pure-JS TCP fallback).
// If the lookup fails or the record is privacy-protected, return null
// and let the caller flag the record as needs_owner_research.

let _whois = null;
function getWhois() {
  if (_whois) return _whois;
  try {
    _whois = require('whois-json');
  } catch (e) {
    _whois = false; // cache the failure so we don't retry require() per lookup
  }
  return _whois;
}

// Privacy-proxy registrants we should ignore. These strings appear in the
// "name" field when the domain uses Domains By Proxy, WhoisGuard, etc.
const PRIVACY_PATTERNS = [
  /domains\s*by\s*proxy/i,
  /whoisguard/i,
  /privacy\s*protect/i,
  /privacyguardian/i,
  /withheld\s*for\s*privacy/i,
  /redacted\s*for\s*privacy/i,
  /privacy\s*service/i,
  /data\s*protected/i,
  /contact\s*privacy/i,
  /gdpr\s*masked/i,
  /perfect\s*privacy/i,
  /identity\s*protect/i,
  /anonymous/i,
  /^n\/?a$/i,
  /not\s*disclosed/i,
  /see\s*privacypost/i,
  /registration\s*private/i,
  /statutory\s*masking/i,
  /proxy\s*protection/i
];

function isPrivacyMasked(name) {
  if (!name) return true;
  const s = String(name).trim();
  if (!s) return true;
  return PRIVACY_PATTERNS.some((rx) => rx.test(s));
}

// Org vs person heuristic. If WHOIS returns "Acme Storage LLC" we want
// to treat that as the business name, NOT the owner. An owner field is
// valuable when it's a PERSON.
function looksLikePerson(name) {
  if (!name) return false;
  const s = String(name).trim();
  if (s.length > 60) return false; // full business names run long
  if (/\b(llc|inc|corp|ltd|gmbh|co\.?|company|group|holdings|partners|properties|storage|rentals?|services)\b/i.test(s)) return false;
  // Needs at least two space-separated tokens, each starting uppercase.
  const parts = s.split(/\s+/).filter(Boolean);
  if (parts.length < 2 || parts.length > 4) return false;
  return parts.every((p) => /^[A-Z][a-z'’-]+$/.test(p));
}

/**
 * Look up owner info for a domain via WHOIS.
 * @param {string} domain - e.g. "acmestorage.com" (no scheme, no path)
 * @returns {Promise<{owner: string|null, org: string|null, raw: object|null, skipped: string|null}>}
 */
async function whoisLookup(domain) {
  if (!domain) return { owner: null, org: null, raw: null, skipped: 'no-domain' };
  const whois = getWhois();
  if (!whois) return { owner: null, org: null, raw: null, skipped: 'whois-module-missing' };

  try {
    const timeoutPromise = new Promise((_, reject) => setTimeout(() => reject(new Error('whois-timeout')), 6000));
    const data = await Promise.race([whois(domain, { follow: 2, verbose: false }), timeoutPromise]);

    // whois-json returns different key casings per registry. Scan for the
    // usual suspects. Registrar fields are least useful (they're the
    // registrar, not the owner), so come last.
    const candidates = [
      data && data.registrantName,
      data && data.registrant_name,
      data && data.registrantOrganization,
      data && data.registrant_organization,
      data && data.adminName,
      data && data.admin_name,
      data && data.techName,
      data && data.registrantContactName,
      data && data.name,
      data && data.owner
    ].filter(Boolean);

    for (const candidate of candidates) {
      if (isPrivacyMasked(candidate)) continue;
      if (looksLikePerson(candidate)) {
        return { owner: String(candidate).trim(), org: null, raw: data, skipped: null };
      }
    }
    // No person-like name, but maybe a usable org (the registrant's
    // business name). Return that as `org` so the caller can at least
    // populate contact_name if no owner turns up.
    for (const candidate of candidates) {
      if (!isPrivacyMasked(candidate)) {
        return { owner: null, org: String(candidate).trim(), raw: data, skipped: 'privacy-or-org-only' };
      }
    }
    return { owner: null, org: null, raw: data, skipped: 'privacy-masked' };
  } catch (err) {
    return { owner: null, org: null, raw: null, skipped: 'whois-error:' + err.message };
  }
}

// --------------------------------------------------------------------
// Netlify HTTP handler: admin can look up a domain from the CRM UI.
// GET /api/scraper-whois?domain=example.com
// --------------------------------------------------------------------
exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') return { statusCode: 200, headers: cors(), body: '' };
  try {
    const { requirePermission } = require('./admin-auth');
    const auth = await requirePermission(event, 'listings.edit');
    if (auth && auth.reject) return auth.reject;

    const domain = (event.queryStringParameters || {}).domain || '';
    if (!domain) return { statusCode: 400, headers: cors(), body: JSON.stringify({ error: 'domain required' }) };

    const result = await whoisLookup(domain);
    return { statusCode: 200, headers: cors(), body: JSON.stringify(result) };
  } catch (err) {
    return { statusCode: 500, headers: cors(), body: JSON.stringify({ error: err.message }) };
  }
};

function cors() {
  return {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type, X-Admin-Key',
    'Content-Type': 'application/json'
  };
}

module.exports.whoisLookup = whoisLookup;
module.exports.isPrivacyMasked = isPrivacyMasked;
module.exports.looksLikePerson = looksLikePerson;
