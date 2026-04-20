#!/usr/bin/env node
// run.js - Thin entry point for GitHub Actions.
// Calls the scraper handler with a fake scheduled event.

const path = require('path');

// Make require('./db') etc. resolve from src/
const srcDir = path.join(__dirname, '..', 'src');
const Module = require('module');
const origResolve = Module._resolveFilename;
Module._resolveFilename = function (request, parent, isMain, options) {
  // Rewrite relative requires from src/ modules (e.g. require('./db'))
  // so they resolve within src/ even when called from scripts/
  if (request.startsWith('./') && parent && parent.filename && parent.filename.includes(path.sep + 'src' + path.sep)) {
    const resolved = path.join(srcDir, request);
    return origResolve.call(this, resolved, parent, isMain, options);
  }
  return origResolve.call(this, request, parent, isMain, options);
};

async function main() {
  console.log('=== StowHelp Scraper (GitHub Actions) ===');
  console.log('Time:', new Date().toISOString());

  // ── Diagnostic: confirm which Supabase project this scraper talks to ──
  // Safe to log: project URL is public info (unlike the service_role key).
  const url = process.env.SUPABASE_URL || '(unset)';
  const host = url.replace(/^https?:\/\//, '').split('/')[0];
  const projectRef = host.split('.')[0];
  console.log('[diag] SUPABASE_URL host:', host);
  console.log('[diag] Supabase project ref:', projectRef);
  console.log('[diag] Expected ref for StowHelp admin: xldwrgezrpeiccmhuxlk');
  console.log('[diag] Match:', projectRef === 'xldwrgezrpeiccmhuxlk' ? 'YES' : 'NO - MISMATCH');

  // ── Diagnostic: write+read probe against scrape_queue ──
  // Inserts one unique probe row, reads it back by PK, reports the total
  // count. Confirms whether the service_role key can see what it wrote
  // (rules out RLS) and whether the table is empty from the scraper's view.
  try {
    const { createClient } = require('@supabase/supabase-js');
    const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false } });
    const probeCity = '__probe_' + Date.now();
    const { data: ins, error: insErr } = await sb.from('scrape_queue')
      .insert({ state: 'probe', city: probeCity, category: 'probe', status: 'pending', priority: 99 })
      .select()
      .single();
    if (insErr) {
      console.log('[diag] probe insert FAILED:', insErr.message);
    } else {
      console.log('[diag] probe insert OK, id:', ins.id);
      const { data: rd, error: rdErr } = await sb.from('scrape_queue')
        .select('id,status').eq('id', ins.id).maybeSingle();
      console.log('[diag] probe read-back:', rdErr ? ('ERR ' + rdErr.message) : (rd ? ('OK status=' + rd.status) : 'NOT FOUND'));
      await sb.from('scrape_queue').delete().eq('id', ins.id);
    }
    const { count } = await sb.from('scrape_queue').select('*', { count: 'exact', head: true });
    console.log('[diag] scrape_queue total row count:', count);
  } catch (e) {
    console.log('[diag] probe exception:', e.message);
  }

  // Fake a Netlify scheduled event
  const event = {
    httpMethod: 'POST',
    headers: {},
    body: JSON.stringify({ next_run: new Date(Date.now() + 900000).toISOString() }),
    queryStringParameters: {}
  };

  try {
    const { handler } = require(path.join(srcDir, 'scraper-run-background.js'));
    const result = await handler(event);
    const body = typeof result.body === 'string' ? JSON.parse(result.body) : result.body;
    console.log('\n=== Results ===');
    console.log('Status:', result.statusCode);
    if (body.results) {
      const t = body.results.totals || {};
      console.log('Queries run:', t.queriesRun || 0);
      console.log('Businesses found:', t.businessesFound || 0);
      console.log('New listings:', t.businessesNew || 0);
      console.log('Website hits:', t.websiteHits || 0);
      console.log('WHOIS hits:', t.whoisHits || 0);
      console.log('Errors:', t.errors || 0);
    }
    console.log('\nFull response:', JSON.stringify(body, null, 2));
    process.exit(body.results && body.results.totals && body.results.totals.captchaHit ? 1 : 0);
  } catch (err) {
    console.error('FATAL:', err.message);
    console.error(err.stack);
    process.exit(1);
  }
}

main();
