#!/usr/bin/env python3
"""
email/get_token.py
KPI Platform — One-time OAuth token setup helper.

Run this ONCE on your local machine to generate a Gmail refresh token.
You will need:
  - GMAIL_CLIENT_ID    (from Google Cloud Console)
  - GMAIL_CLIENT_SECRET (from Google Cloud Console)

These can be set as environment variables or entered interactively.

What it does:
  1. Opens a browser for Gmail authorization
  2. You click "Allow" to grant access
  3. It exchanges the auth code for access + refresh tokens
  4. Prints the refresh token — copy it into GitHub Secrets

After running this, add three GitHub Secrets:
  GMAIL_CLIENT_ID       → your client ID
  GMAIL_CLIENT_SECRET   → your client secret
  GMAIL_REFRESH_TOKEN   → the refresh token printed below

Usage:
  GMAIL_CLIENT_ID=xxx GMAIL_CLIENT_SECRET=yyy python email/get_token.py
  — OR —
  python email/get_token.py  (will prompt for values)
"""

import os
import sys
import json
import urllib.parse
import urllib.request
import webbrowser
from http.server import HTTPServer, BaseHTTPRequestHandler

# Gmail scopes needed by the Email Assistant
SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",    # Read emails
    "https://www.googleapis.com/auth/gmail.compose",     # Create drafts
]

AUTH_URL   = "https://accounts.google.com/o/oauth2/v2/auth"
TOKEN_URL  = "https://oauth2.googleapis.com/token"
REDIRECT   = "http://localhost:8765/callback"

_auth_code = None


class _CallbackHandler(BaseHTTPRequestHandler):
    """Tiny local server to catch the OAuth callback."""

    def do_GET(self):
        global _auth_code
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)

        if "code" in params:
            _auth_code = params["code"][0]
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(b"""
                <html><body style="font-family:sans-serif;text-align:center;padding:60px">
                <h2>&#x2705; Authorization successful!</h2>
                <p>You can close this tab and return to your terminal.</p>
                </body></html>
            """)
        else:
            error = params.get("error", ["unknown"])[0]
            self.send_response(400)
            self.end_headers()
            self.wfile.write(f"<html><body>Error: {error}</body></html>".encode())

    def log_message(self, format, *args):
        pass  # Suppress request logs


def main():
    print()
    print("╔═══════════════════════════════════════════════╗")
    print("║  KPI — Gmail OAuth Token Setup                ║")
    print("╚═══════════════════════════════════════════════╝")
    print()

    # Get credentials
    client_id     = os.environ.get("GMAIL_CLIENT_ID", "").strip()
    client_secret = os.environ.get("GMAIL_CLIENT_SECRET", "").strip()

    if not client_id:
        client_id = input("Enter your GMAIL_CLIENT_ID: ").strip()
    if not client_secret:
        client_secret = input("Enter your GMAIL_CLIENT_SECRET: ").strip()

    if not client_id or not client_secret:
        print("ERROR: Both client_id and client_secret are required.")
        sys.exit(1)

    # Build authorization URL
    auth_params = {
        "client_id":     client_id,
        "redirect_uri":  REDIRECT,
        "response_type": "code",
        "scope":         " ".join(SCOPES),
        "access_type":   "offline",   # Required to get a refresh token
        "prompt":        "consent",   # Force consent screen (ensures refresh token)
    }
    auth_url = AUTH_URL + "?" + urllib.parse.urlencode(auth_params)

    print("Step 1: Opening your browser for Gmail authorization...")
    print(f"        If it doesn't open automatically, visit:\n        {auth_url}\n")
    webbrowser.open(auth_url)

    print("Step 2: Waiting for authorization (listening on localhost:8765)...")
    server = HTTPServer(("localhost", 8765), _CallbackHandler)
    server.handle_request()  # Handle exactly one request then stop

    if not _auth_code:
        print("ERROR: Did not receive authorization code. Please try again.")
        sys.exit(1)

    print("Step 3: Exchanging authorization code for tokens...")

    token_data = urllib.parse.urlencode({
        "grant_type":    "authorization_code",
        "code":          _auth_code,
        "client_id":     client_id,
        "client_secret": client_secret,
        "redirect_uri":  REDIRECT,
    }).encode()

    req = urllib.request.Request(TOKEN_URL, data=token_data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            tokens = json.loads(resp.read().decode())
    except Exception as e:
        print(f"ERROR: Token exchange failed: {e}")
        sys.exit(1)

    refresh_token = tokens.get("refresh_token", "")
    if not refresh_token:
        print("ERROR: No refresh_token received. Make sure you used access_type=offline and prompt=consent.")
        print("Full response:", tokens)
        sys.exit(1)

    print()
    print("╔═══════════════════════════════════════════════════════════╗")
    print("║  ✅  SUCCESS — Add these 3 secrets to GitHub              ║")
    print("╚═══════════════════════════════════════════════════════════╝")
    print()
    print("Go to: https://github.com/[your-repo]/settings/secrets/actions")
    print()
    print(f"  GMAIL_CLIENT_ID       →  {client_id}")
    print("  GMAIL_CLIENT_SECRET   →  (use your existing client secret; do NOT paste it here)")
    print(f"  GMAIL_REFRESH_TOKEN   →  {refresh_token}")
    print()
    print("Once those are saved, the Email Assistant workflow is ready to run.")
    print()


if __name__ == "__main__":
    main()
