// catbox-proxy — Cloudflare Worker that relays Catbox.moe uploads.
//
// WHY: Catbox.moe actively blocks datacenter/VPS IP ranges (HTTP 412
// "Invalid uploader"), which is exactly the kind of network the DVR nodes
// run on. Routing uploads through this Worker means the request leaves from
// Cloudflare's edge IPs instead, which Catbox does not block.
//
// The DVR's uploader POSTs a standard multipart form (reqtype=fileupload,
// fileToUpload, optional userhash) to this Worker's URL. This Worker
// reconstructs the form, forwards it to https://catbox.moe/user/api.php, and
// returns Catbox's response VERBATIM (status + body). On Catbox errors the
// upstream status/body pass through unchanged so the Go client's existing
// retry/fallback logic (isRetryableCatboxError, ImgBB fallback) keeps
// working.
//
// Deploy:
//   cd workers/catbox-proxy
//   npx wrangler deploy
//   (or set CLOUDFLARE_API_TOKEN + CLOUDFLARE_ACCOUNT_ID and run the same)

export default {
	async fetch(request, env) {
		if (request.method !== 'POST') {
			return new Response('Method not allowed', { status: 405 });
		}

		try {
			// Parse the client's multipart form. Small files only (thumbs,
			// sprites, WEBP previews), so buffering via formData() is fine.
			const form = await request.formData();
			const file = form.get('fileToUpload');
			if (!file) {
				return new Response('Missing fileToUpload', { status: 400 });
			}

			// Rebuild the form for the upstream call. Forward the optional
			// userhash if the client sent one (ties files to the account,
			// makes them permanent + skips some abuse heuristics).
			const upstream = new FormData();
			upstream.append('reqtype', 'fileupload');
			const userhash = form.get('userhash') || env.CATBOX_USERHASH || '';
			if (userhash) {
				upstream.append('userhash', userhash);
			}
			upstream.append('fileToUpload', file, file.name || 'upload.webp');

			const resp = await fetch('https://catbox.moe/user/api.php', {
				method: 'POST',
				body: upstream,
				headers: {
					'User-Agent':
						'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
					Accept: '*/*',
					'Accept-Language': 'en-US,en;q=0.9',
					Origin: 'https://catbox.moe',
					Referer: 'https://catbox.moe/',
				},
			});

			const text = await resp.text();
			return new Response(text, {
				status: resp.status,
				headers: {
					'Content-Type': resp.headers.get('Content-Type') || 'text/plain',
				},
			});
		} catch (err) {
			// Never throw — an uncaught exception surfaces to the client as
			// Cloudflare's "error code: 1101" page, which the Go client does
			// not understand. Return a structured 502 instead so the client's
			// retry/fallback path can react.
			return new Response('catbox-proxy error: ' + (err && err.message ? err.message : String(err)), {
				status: 502,
			});
		}
	},
};
