# Highwater landing page

This directory is a dependency-free static site. It needs no application server or client-side build.

## Preview locally

```bash
cd landing
python3 -m http.server 8080
```

Open `http://localhost:8080`.

## Configure links

Review the destinations in `config.js` before publishing:

- documentation;
- early-access contact or form;
- GitHub;
- Discord;
- X / Twitter;
- LinkedIn;
- Bluesky;
- privacy;
- terms.

Links left as `#` render as disabled and display “link coming soon.”

Add an Open Graph image before launch and declare it with `og:image` and `twitter:image` in `index.html`.

## Publish to Amazon S3

Create a private bucket behind CloudFront for the eventual production deployment. For a direct S3 website preview:

```bash
aws s3 sync . s3://YOUR_BUCKET/ \
  --exclude "README.md" \
  --cache-control "public,max-age=300"

aws s3 cp index.html s3://YOUR_BUCKET/index.html \
  --content-type "text/html; charset=utf-8" \
  --cache-control "public,max-age=60,must-revalidate"
```

Configure `index.html` as the index document. The page uses only relative asset paths, so it also works from a preview prefix.

For `highwater.cloud`, put CloudFront in front of a private S3 origin, attach an ACM certificate issued in `us-east-1`, redirect HTTP to HTTPS, and point the Route 53 alias records at the distribution. Use immutable cache headers only after adding content hashes to asset filenames.

Configure CloudFront response headers for a content security policy, HTTP Strict Transport Security, `X-Content-Type-Options: nosniff`, a restrictive referrer policy, and framing protection. Give `config.js` a short cache lifetime so links can change independently of the page.
