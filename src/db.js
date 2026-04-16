// db.js - Supabase adapter with a Netlify Blobs-compatible KV fallback.
//
// Why this exists:
//   The codebase has ~20 functions calling `getStore(name).get(key)` /
//   `.setJSON(key, val)` / `.list()` / `.delete(key)`. Rewriting all of
//   them at once is risky. This module lets us flip one function at a
//   time from Blobs to Supabase without touching the others.
//
// Two layers:
//   1. `db.kv(storeName)` - drop-in replacement for Blobs getStore().
//      Uses the kv_store table. Same method shape, zero code changes
//      at call sites besides swapping the import.
//   2. `db.listings`, `db.outreach`, `db.leads`, ... - typed helpers
//      that use real Postgres columns and indexes. This is where the
//      speed comes from - use these in new/rewritten code.
//
// Activation:
//   Set these in Netlify env vars:
//     SUPABASE_URL               = https://<project>.supabase.co
//     SUPABASE_SERVICE_ROLE_KEY  = <service-role key, NOT anon key>
//
//   With both set, db.isEnabled() returns true. Without them, anyone
//   who calls `db.*` gets a clear error - safer than silently falling
//   back to blobs from a code path that expected Postgres.
//
// Security:
//   The service-role key bypasses RLS. Never expose it to the browser.
//   It lives only in Netlify function env. Rotate immediately if leaked.

let _client = null;

function getClient() {
  if (_client) return _client;
  const url = process.env.SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!url || !key) {
    throw new Error(
      'Supabase not configured. Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in Netlify env vars.'
    );
  }
  const { createClient } = require('@supabase/supabase-js');
  _client = createClient(url, key, {
    auth: { persistSession: false, autoRefreshToken: false },
    // StowHelp runs inside a dedicated 'stowhelp' Postgres schema so it
    // coexists cleanly with other apps sharing this Supabase project.
    db: { schema: 'stowhelp' }
  });
  return _client;
}

function isEnabled() {
  return !!(process.env.SUPABASE_URL && process.env.SUPABASE_SERVICE_ROLE_KEY);
}

// ------------------------------------------------------------
// KV compatibility layer (same shape as Netlify Blobs getStore)
// ------------------------------------------------------------
function kv(storeName) {
  const sb = getClient();
  return {
    async get(key, opts) {
      const { data, error } = await sb
        .from('kv_store')
        .select('value')
        .eq('store_name', storeName)
        .eq('key', key)
        .maybeSingle();
      if (error) throw error;
      if (!data) return null;
      // Blobs returns a string by default; `{ type: 'json' }` returns parsed.
      // We always store jsonb, so return the object; if caller wanted text,
      // they'll stringify. (Almost every caller already passes type: 'json'.)
      return data.value;
    },
    async setJSON(key, value) {
      const { error } = await sb
        .from('kv_store')
        .upsert({ store_name: storeName, key, value }, { onConflict: 'store_name,key' });
      if (error) throw error;
    },
    async set(key, value) {
      // Legacy string setter - wrap and store as jsonb
      return this.setJSON(key, value);
    },
    async delete(key) {
      const { error } = await sb
        .from('kv_store')
        .delete()
        .eq('store_name', storeName)
        .eq('key', key);
      if (error) throw error;
    },
    async list(opts) {
      // Returns { blobs: [{ key, etag }] } to match the Blobs API shape
      let q = sb.from('kv_store').select('key, updated_at').eq('store_name', storeName);
      if (opts && opts.prefix) {
        q = q.ilike('key', `${opts.prefix}%`);
      }
      const { data, error } = await q;
      if (error) throw error;
      return { blobs: (data || []).map((r) => ({ key: r.key, etag: r.updated_at })) };
    }
  };
}

// ------------------------------------------------------------
// Typed helpers (use these in new code)
// ------------------------------------------------------------

const listings = {
  async get(slug) {
    const { data, error } = await getClient()
      .from('listings').select('*').eq('slug', slug).maybeSingle();
    if (error) throw error;
    return data;
  },

  async upsert(row) {
    const { data, error } = await getClient()
      .from('listings').upsert(row, { onConflict: 'slug' }).select().single();
    if (error) throw error;
    return data;
  },

  async delete(slug) {
    const { error } = await getClient().from('listings').delete().eq('slug', slug);
    if (error) throw error;
  },

  // Paginated, filtered, sorted list for the admin dashboard.
  // Returns { items, total }.
  async list({
    page = 1, perPage = 50,
    state, city, plan, status, source, category, rep, search,
    claimedOnly, unclaimedOnly,
    sortBy = 'created_at', sortDir = 'desc'
  } = {}) {
    let q = getClient().from('listings').select('*', { count: 'exact' });

    if (state)        q = q.eq('state', state);
    if (city)         q = q.ilike('city', `%${city}%`);
    if (plan)         q = q.eq('plan', plan);
    if (status)       q = q.eq('status', status);
    if (source)       q = q.eq('source', source);
    if (rep)          q = q.eq('rep', rep);
    if (category)     q = q.contains('categories', [category]);
    if (search)       q = q.or(`name.ilike.%${search}%,email.ilike.%${search}%,city.ilike.%${search}%`);
    // "Claimed" = either explicit claimed_at timestamp OR the listing was
    // created by the owner themselves (source=owner-submission). This matches
    // the legacy admin definition so totals and the filter agree.
    if (claimedOnly)   q = q.or('claimed_at.not.is.null,source.eq.owner-submission');
    if (unclaimedOnly) q = q.is('claimed_at', null).neq('source', 'owner-submission');

    q = q.order(sortBy, { ascending: sortDir === 'asc' })
         .range((page - 1) * perPage, page * perPage - 1);

    const { data, error, count } = await q;
    if (error) throw error;
    return { items: data || [], total: count || 0 };
  }
};

const leads = {
  async insert(row) {
    const { data, error } = await getClient().from('leads').insert(row).select().single();
    if (error) throw error;
    return data;
  },
  async listForSlug(slug) {
    const { data, error } = await getClient()
      .from('leads').select('*').eq('slug', slug).order('created_at', { ascending: false });
    if (error) throw error;
    return data || [];
  },
  async countsBySlug() {
    // Replaces the Blobs lead-counts cache entirely - one indexed query.
    const { data, error } = await getClient().rpc('lead_counts_by_slug');
    if (error) {
      // Fallback: group in SQL via raw select if the RPC isn't defined yet
      const r = await getClient().from('leads').select('slug');
      if (r.error) throw r.error;
      const out = {};
      (r.data || []).forEach((l) => { if (l.slug) out[l.slug] = (out[l.slug] || 0) + 1; });
      return out;
    }
    const out = {};
    (data || []).forEach((row) => { out[row.slug] = row.count; });
    return out;
  },
  async update(id, patch) {
    const { data, error } = await getClient().from('leads').update(patch).eq('id', id).select().single();
    if (error) throw error;
    return data;
  }
};

const reviews = {
  async insert(row) {
    const { data, error } = await getClient().from('reviews').insert(row).select().single();
    if (error) throw error;
    return data; // trigger recalculates listings.review_count and rating
  },
  async listForSlug(slug) {
    const { data, error } = await getClient()
      .from('reviews').select('*').eq('slug', slug).eq('status', 'approved')
      .order('created_at', { ascending: false });
    if (error) throw error;
    return data || [];
  }
};

const outreach = {
  async insert(row) {
    const { data, error } = await getClient().from('outreach').insert(row).select().single();
    if (error) throw error;
    return data;
  },
  async bulkInsert(rows) {
    const { data, error } = await getClient().from('outreach').insert(rows).select();
    if (error) throw error;
    return data || [];
  },
  async update(id, patch) {
    const { data, error } = await getClient().from('outreach').update(patch).eq('id', id).select().single();
    if (error) throw error;
    return data;
  },
  async delete(id) {
    const { error } = await getClient().from('outreach').delete().eq('id', id);
    if (error) throw error;
  },
  async list({
    page = 1, perPage = 50,
    search, status, state, city, rep, category,
    sortBy = 'sent_at', sortDir = 'desc'
  } = {}) {
    let q = getClient().from('outreach').select('*', { count: 'exact' });
    if (status && status !== 'all') q = q.eq('status', status);
    if (state)    q = q.eq('state', state);
    if (city)     q = q.ilike('city', `%${city}%`);
    if (rep === '__unassigned__') q = q.is('rep', null);
    else if (rep) q = q.eq('rep', rep);
    if (category) q = q.contains('categories', [category]);
    if (search) {
      q = q.or(`facility.ilike.%${search}%,email.ilike.%${search}%,city.ilike.%${search}%`);
    }
    q = q.order(sortBy, { ascending: sortDir === 'asc' })
         .range((page - 1) * perPage, page * perPage - 1);
    const { data, error, count } = await q;
    if (error) throw error;
    return { items: data || [], total: count || 0 };
  }
};

const suppressions = {
  async isSuppressed(email) {
    if (!email) return false;
    const { data, error } = await getClient()
      .from('email_suppressions').select('email').eq('email', email.toLowerCase()).maybeSingle();
    if (error) throw error;
    return !!data;
  },
  async add(email, reason) {
    const { error } = await getClient()
      .from('email_suppressions')
      .upsert({ email: email.toLowerCase(), reason }, { onConflict: 'email' });
    if (error) throw error;
  }
};

const emailLog = {
  async insert(row) {
    const { error } = await getClient().from('email_log').insert(row);
    if (error) throw error;
  }
};

// ------------------------------------------------------------
// Health check - used by admin-supabase-health.js
// ------------------------------------------------------------
async function health() {
  if (!isEnabled()) {
    return { ok: false, reason: 'SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY not set' };
  }
  try {
    const sb = getClient();
    const { data, error } = await sb.from('listings').select('slug').limit(1);
    if (error) return { ok: false, reason: 'query failed: ' + error.message };
    return { ok: true, sampleRows: (data || []).length };
  } catch (e) {
    return { ok: false, reason: e.message };
  }
}

module.exports = {
  isEnabled,
  kv,
  listings,
  leads,
  reviews,
  outreach,
  suppressions,
  emailLog,
  health,
  // Escape hatch for one-off raw queries; avoid in routine code.
  raw: () => getClient()
};
