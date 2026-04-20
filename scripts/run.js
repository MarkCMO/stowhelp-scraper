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
