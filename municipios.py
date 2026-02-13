#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import csv
import json
import time
import base64
import re
from urllib.parse import urlparse

import requests

# =========================
# CONFIG (env)
# =========================

ORIG_USER = os.getenv("PLONE_ORIG_USER", "")
ORIG_PASS = os.getenv("PLONE_ORIG_PASS", "")

DEST_USER = os.getenv("PLONE_DEST_USER", "admin")
DEST_PASS = os.getenv("PLONE_DEST_PASS", "Q7!mR2@x#9Lp")

TIMEOUT = int(os.getenv("PLONE_TIMEOUT", "60"))
SLEEP_BETWEEN = float(os.getenv("PLONE_SLEEP_BETWEEN", "0.05"))

# Destino (backend REST que responde JSON)
DEST_API_BASE = os.getenv("PLONE_DEST_API_BASE", "https://www-cdn.mpf.mp.br")
DEST_API_PREFIX = os.getenv("PLONE_DEST_API_PREFIX", "/o-mpf/unidades")  # ajuste

# SSL ignore (0 = ignora)
SSL_VERIFY_ENV = (os.getenv("PLONE_SSL_VERIFY", "0") or "").strip().lower()
SSL_VERIFY = not (SSL_VERIFY_ENV in ("0", "false", "no", "off", "nao", "não"))

if SSL_VERIFY is False:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

JSON_HEADERS = {"Accept": "application/json", "Content-Type": "application/json"}

# Métodos Zope na origem
M_META = "v2_getMunicipioMetadados"
M_BODY = "v2_getMunicipioCorpo"
M_CONT = "v2_getMunicipioContato"
M_END  = "v2_getMunicipioEndereco"
M_LOC  = "v2_getMunicipioLocalizacao"
M_IMG  = "v2_getMunicipioImagem"

# =========================
# HELPERS
# =========================

def auth(user, pwd):
    return (user, pwd) if user and pwd else None

def parent_and_id(url: str):
    url = url.rstrip("/")
    parent, _sep, _id = url.rpartition("/")
    return parent, _id

def safe_filename(name: str) -> str:
    name = (name or "").strip()
    if not name:
        return ""
    name = re.sub(r"[^\w\-. ()\[\]]+", "_", name, flags=re.UNICODE)
    return name[:120]

def call_method_text(sess, obj_url, method, a):
    r = sess.get(
        obj_url.rstrip("/") + "/" + method,
        auth=a,
        timeout=TIMEOUT,
        allow_redirects=True,
        verify=SSL_VERIFY,
    )
    r.raise_for_status()
    return r.text or ""

def parse_meta(txt: str) -> dict:
    out = {}
    for raw in (txt or "").splitlines():
        line = raw.strip()
        if " = " in line:
            k, v = line.split(" = ", 1)
            out[k.strip()] = v.strip()
    return out

def build_html(image_rel, corpo, contatos, endereco, localizacao):
    img_html = f'<p><img src="{image_rel}" alt=""></p>\n' if image_rel else ""
    return (
        f"{img_html}"
        f"{corpo}\n\n"
        f"<h2>Contatos:</h2>\n{contatos}\n\n"
        f"<h2>Endereço:</h2>\n{endereco}\n\n"
        f"<h2>Como chegar:</h2>\n{localizacao}\n"
    )

def dest_api_url(destino_url: str) -> str:
    """
    Converte URL pública (HTML) para URL do backend REST (JSON).
    Ex:
      https://novoportal.mpf.mp.br/mpf/municipios/x
    vira:
      https://www-cdn.mpf.mp.br/o-mpf/unidades/mpf/municipios/x
    """
    p = urlparse(destino_url.strip())
    path = (p.path or "/").rstrip("/")
    return DEST_API_BASE.rstrip("/") + DEST_API_PREFIX.rstrip("/") + path

# =========================
# DEST REST
# =========================

def dest_exists(sess, api_url, a) -> bool:
    r = sess.get(
        api_url.rstrip("/"),
        auth=a,
        headers={"Accept": "application/json"},
        timeout=TIMEOUT,
        verify=SSL_VERIFY,
    )
    if r.status_code == 404:
        return False
    r.raise_for_status()
    ctype = (r.headers.get("Content-Type") or "").lower()
    if "application/json" not in ctype:
        raise RuntimeError(f"DEST não é JSON: {api_url} ctype={ctype} body={r.text[:200]}")
    return True

def dest_post(sess, parent_api_url, payload, a):
    r = sess.post(
        parent_api_url.rstrip("/"),
        auth=a,
        headers=JSON_HEADERS,
        data=json.dumps(payload),
        timeout=TIMEOUT,
        verify=SSL_VERIFY,
    )
    r.raise_for_status()
    ctype = (r.headers.get("Content-Type") or "").lower()
    if "application/json" not in ctype:
        raise RuntimeError(f"POST não é JSON: {parent_api_url} ctype={ctype} body={r.text[:200]}")
    return r.json()

def dest_create_image(sess, parent_api_url, image_id, title, blob, ctype, filename, a):
    payload = {
        "@type": "Image",
        "id": image_id,
        "title": title or image_id,
        "image": {
            "data": base64.b64encode(blob).decode("ascii"),
            "encoding": "base64",
            "filename": filename or (image_id + ".jpg"),
            "content-type": ctype or "image/jpeg",
        },
    }
    dest_post(sess, parent_api_url, payload, a)

def dest_create_document(sess, parent_api_url, doc_id, title, html, a):
    payload = {
        "@type": "Document",
        "id": doc_id,
        "title": title or doc_id,
        "text": {"data": html or "", "content-type": "text/html"},
    }
    dest_post(sess, parent_api_url, payload, a)

# =========================
# MIGRATE
# =========================

def migrate_one(orig_sess, dest_sess, origem_url, destino_url, orig_auth, dest_auth):
    destino_api = dest_api_url(destino_url)

    if dest_exists(dest_sess, destino_api, dest_auth):
        return "SKIP_EXISTS"

    meta = parse_meta(call_method_text(orig_sess, origem_url, M_META, orig_auth))
    titulo = meta.get("titulo", "") or destino_url.rstrip("/").split("/")[-1]

    corpo = call_method_text(orig_sess, origem_url, M_BODY, orig_auth)
    contatos = call_method_text(orig_sess, origem_url, M_CONT, orig_auth)
    endereco = call_method_text(orig_sess, origem_url, M_END, orig_auth)
    localizacao = call_method_text(orig_sess, origem_url, M_LOC, orig_auth)

    parent_api, doc_id = parent_and_id(destino_api)

    # imagem (2 linhas): url e filename. Só sobe se filename não for vazio e se GET 200.
    img_lines = call_method_text(orig_sess, origem_url, M_IMG, orig_auth).splitlines()

    image_rel = ""
    if len(img_lines) >= 2:
        img_url = (img_lines[0] or "").strip()
        img_filename = safe_filename((img_lines[1] or "").strip())

        if img_filename:
            rimg = orig_sess.get(
                img_url,
                auth=orig_auth,
                timeout=TIMEOUT,
                allow_redirects=True,
                verify=SSL_VERIFY,
            )
            if rimg.status_code == 200 and rimg.content:
                blob = rimg.content
                ctype = rimg.headers.get("Content-Type", "image/jpeg")

                image_id = f"{doc_id}-imagem"
                dest_create_image(dest_sess, parent_api, image_id, f"{titulo} - Imagem", blob, ctype, img_filename, dest_auth)
                image_rel = f"{image_id}/@@images/image/large"

    html = build_html(image_rel, corpo, contatos, endereco, localizacao)
    dest_create_document(dest_sess, parent_api, doc_id, titulo, html, dest_auth)

    return "CREATED"

# =========================
# INPUT
# =========================

def read_pairs(path):
    # CSV ; com colunas url_origem;url_destino
    if path.lower().endswith(".csv"):
        out = []
        with open(path, newline="", encoding="utf-8-sig") as f:
            r = csv.DictReader(f, delimiter=";")
            for row in r:
                out.append((row["url_origem"].strip(), row["url_destino"].strip()))
        return out

    # TXT com "origem -> destino"
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            left, right = [x.strip() for x in s.split("->", 1)]
            out.append((left, right))
    return out

# =========================
# MAIN
# =========================

def main():
    inp = sys.argv[1]

    orig_auth = auth(ORIG_USER, ORIG_PASS)
    dest_auth = auth(DEST_USER, DEST_PASS)

    orig_sess = requests.Session()
    dest_sess = requests.Session()

    pairs = read_pairs(inp)

    for i, (uo, ud) in enumerate(pairs, start=1):
        st = migrate_one(orig_sess, dest_sess, uo, ud, orig_auth, dest_auth)
        print(f"[{i}/{len(pairs)}] {st} :: {ud}")
        time.sleep(SLEEP_BETWEEN)

if __name__ == "__main__":
    main()
