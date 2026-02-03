#!/Users/lflrocha/Sistemas/v2.mpf.migracao/bin/python3
# -*- coding: utf-8 -*-

import csv
import json
import os
import re
import sys
from urllib.parse import urlencode

import requests
import urllib3

# some os warnings de SSL quando verify=False
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ========= CONFIG =========

ORIG_USER = os.getenv("PLONE_ORIG_USER", "")
ORIG_PASS = os.getenv("PLONE_ORIG_PASS", "")

DEST_USER = os.getenv("PLONE_DEST_USER", "admin")
DEST_PASS = os.getenv("PLONE_DEST_PASS", "zope")

ORIG_METHOD = os.getenv("PLONE_ORIG_METHOD_BODY", "v2_getDocumentosCorpo")
TIMEOUT = int(os.getenv("PLONE_TIMEOUT", "60"))

# SSL (default: ignora)
SSL_VERIFY_ENV = (os.getenv("PLONE_SSL_VERIFY", "0") or "").strip().lower()
CA_BUNDLE = (os.getenv("PLONE_CA_BUNDLE", "") or "").strip()
if CA_BUNDLE:
    SSL_VERIFY = CA_BUNDLE
else:
    SSL_VERIFY = not (SSL_VERIFY_ENV in ("0", "false", "no", "off", "nao", "não"))

HEADERS_JSON = {
    "Accept": "application/json",
    "Content-Type": "application/json",
}

# ========= HELPERS =========

def strip_wrapper(html: str) -> str:
    """Retorna só o miolo do <body> se existir; senão devolve tudo."""
    if not html:
        return ""
    h = html.strip()
    m = re.search(r"(?is)<body\b[^>]*>(.*)</body>", h)
    return (m.group(1).strip() if m else h)

def get_body_from_origin(sess, url, auth):
    r = sess.get(
        url.rstrip("/") + "/" + ORIG_METHOD,
        auth=auth,
        timeout=TIMEOUT,
        verify=SSL_VERIFY,
        allow_redirects=True,
    )
    r.raise_for_status()
    return strip_wrapper(r.text)

def patch_body_dest(sess, url, html, auth):
    payload = {"text": {"data": html or "", "content-type": "text/html"}}
    r = sess.patch(
        url.rstrip("/"),
        auth=auth,
        headers=HEADERS_JSON,
        data=json.dumps(payload),
        timeout=TIMEOUT,
        verify=SSL_VERIFY,
    )
    r.raise_for_status()

# ========= MAIN =========

def main():
    if len(sys.argv) < 2:
        print("Uso: atualiza_body.py arquivo.csv", file=sys.stderr)
        sys.exit(2)

    orig_auth = (ORIG_USER, ORIG_PASS) if (ORIG_USER and ORIG_PASS) else None
    dest_auth = (DEST_USER, DEST_PASS)

    s_orig = requests.Session()
    s_dest = requests.Session()

    ok = 0
    fail = 0

    with open(sys.argv[1], newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=";")

        for line, row in enumerate(reader, start=2):
            tipo = (row.get("tipo") or "").strip().lower()
            uo = (row.get("url_origem") or "").strip()
            ud = (row.get("url_destino") or "").strip()

            if tipo not in ("pagina", "página", "document", "page"):
                continue
            if not uo or not ud:
                continue

            try:
                body = get_body_from_origin(s_orig, uo, orig_auth)

                if not body.strip():
                    print(f"[linha {line}] WARN corpo vazio :: {uo}", file=sys.stderr)

                patch_body_dest(s_dest, ud, body, dest_auth)

                ok += 1
                print(f"[linha {line}] OK :: {ud}")

            except requests.exceptions.HTTPError as e:
                # Mostra status code e segue
                resp = getattr(e, "response", None)
                code = resp.status_code if resp is not None else "?"
                fail += 1
                print(f"[linha {line}] FAIL HTTP {code} :: {uo} -> {ud}", file=sys.stderr)

            except requests.exceptions.RequestException as e:
                # DNS, timeout, conexão, etc.
                fail += 1
                print(f"[linha {line}] FAIL CONEXAO :: {uo} -> {ud} :: {e}", file=sys.stderr)

            except Exception as e:
                fail += 1
                print(f"[linha {line}] FAIL ERRO :: {uo} -> {ud} :: {e}", file=sys.stderr)

    print("\nResumo:")
    print(f"  OK: {ok}")
    print(f"  FAIL: {fail}")

if __name__ == "__main__":
    main()
