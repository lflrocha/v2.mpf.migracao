#!/Users/lflrocha/Sistemas/v2.mpf.migracao/bin/python3

# -*- coding: utf-8 -*-

import csv
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from urllib.parse import urlparse, urlencode

import requests
import base64


import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

orig_sess = requests.Session()
dest_sess = requests.Session()

orig_sess.verify = False
dest_sess.verify = False


# =========================
# CONFIG
# =========================

# Origem (sem restapi) - auth opcional
ORIG_USER = os.getenv("PLONE_ORIG_USER", "")
ORIG_PASS = os.getenv("PLONE_ORIG_PASS", "")

# Destino (com restapi)
DEST_USER = os.getenv("PLONE_DEST_USER", "admin")
DEST_PASS = os.getenv("PLONE_DEST_PASS", "zope")

TIMEOUT = 60
SLEEP_BETWEEN = 0.05  # pode subir se quiser aliviar

JSON_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
}
UPLOAD_HEADERS = {"Accept": "application/json"}

# Métodos Zope na ORIGEM para páginas
ORIG_METHOD_BODY = "v2_getDocumentosCorpo"
ORIG_METHOD_META = "v2_getDocumentosMetadados"

# Root do DESTINO (use exatamente como você acessa no browser)
# DEST_ROOT_URL = "http://svlh-plnptall01.pgr.mpf.mp.br:8401/mpf2026"
#DEST_ROOT_URL = "http://svlh-plnptall01.pgr.mpf.mp.br:8401/mpf2026/atuacao/sci/"
DEST_ROOT_URL = "https://www-cdn.mpf.mp.br/atuacao/sci/"


# =========================
# MODELOS
# =========================

@dataclass
class Row:
    tipo: str
    url_origem: str
    url_destino: str

@dataclass
class PageData:
    id: str = ""
    caminho: str = ""
    titulo: str = ""
    descricao: str = ""
    creationDate: str = ""
    effectiveDate: str = ""
    expirationDate: str = ""
    subject: list = None
    corpo_html: str = ""


# =========================
# HELPERS GERAIS
# =========================

def norm_tipo(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("página", "pagina")
    s = s.replace("pasta", "folder")
    return s

def split_base_and_path(url: str):
    u = urlparse(url.strip())
    base = f"{u.scheme}://{u.netloc}"
    path = (u.path or "/").rstrip("/")
    return base, path

def parent_and_id(url: str):
    url = url.rstrip("/")
    base, path = split_base_and_path(url)
    parent_path, _sep, item_id = path.rpartition("/")
    parent_url = base + parent_path
    return parent_url, item_id

def get_origin_auth():
    if ORIG_USER and ORIG_PASS:
        return (ORIG_USER, ORIG_PASS)
    return None

def guess_filename(resp: requests.Response, fallback_url: str) -> str:
    cd = resp.headers.get("Content-Disposition", "")
    m = re.search(r'filename="?([^"]+)"?', cd)
    if m:
        return m.group(1)
    # fallback: último segmento
    _p, fid = parent_and_id(fallback_url)
    return fid


# =========================
# DESTINO (REST API)
# =========================

def dest_create_file_json(dest_sess, parent_url: str, file_id: str, filename: str,
                          blob: bytes, content_type: str, dest_auth) -> bool:
    payload = {
        "@type": "File",
        "id": file_id,
        "title": filename or file_id,
        "file": {
            "data": base64.b64encode(blob).decode("ascii"),
            "encoding": "base64",
            "filename": filename or file_id,
            "content-type": content_type or "application/octet-stream",
        },
    }

    r = dest_sess.post(
        parent_url.rstrip("/"),   # POST no container (sem /@)
        auth=dest_auth,
        headers=JSON_HEADERS,
        data=json.dumps(payload),
        timeout=TIMEOUT,
    )

    if r.status_code in (200, 201):
        return True
    if r.status_code == 409:
        return False
    if r.status_code == 400 and "already in use" in (r.text or ""):
        return False

    raise RuntimeError(f"POST {parent_url} (File id={file_id}) -> {r.status_code} {r.text}")


def dest_get_type(dest_sess, url: str, dest_auth) -> str | None:
    r = dest_sess.get(url.rstrip("/"), auth=dest_auth, headers={"Accept": "application/json"}, timeout=TIMEOUT)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    data = r.json()
    return data.get("@type")


def dest_exists(dest_sess: requests.Session, url: str, dest_auth) -> bool:
    # plone.restapi: GET no objeto com Accept: application/json
    r = dest_sess.get(url.rstrip("/"), auth=dest_auth, headers={"Accept": "application/json"}, timeout=TIMEOUT)
    return r.status_code == 200

def dest_create_folder(dest_sess, parent_url, folder_id, title, dest_auth):
    payload = {"@type": "Folder", "id": folder_id, "title": title or folder_id}
    r = dest_sess.post(
        parent_url.rstrip("/"),
        auth=dest_auth,
        headers=JSON_HEADERS,
        data=json.dumps(payload),
        timeout=TIMEOUT,
    )
    if r.status_code in (200, 201):
        return True
    if r.status_code == 409:
        return False
    # >>> isso aqui ajuda MUITO
    raise RuntimeError(f"POST {parent_url} -> {r.status_code} {r.text}")

def dest_create_document(dest_sess, parent_url: str, doc_id: str,
                         title: str, html: str, description: str,
                         subject: list, effective: str, expires: str, dest_auth) -> bool:
    payload = {
        "@type": "Document",
        "id": doc_id,
        "title": title or doc_id,
        "description": description or "",
        "text": {"data": html or "", "content-type": "text/html"},
    }
    if subject:
        payload["subject"] = subject

    # MUITO comum dar 400 aqui por formato de data. Por enquanto, desabilite:
    # if effective: payload["effective"] = effective
    # if expires: payload["expires"] = expires

    r = dest_sess.post(
        parent_url.rstrip("/"),
        auth=dest_auth,
        headers=JSON_HEADERS,
        data=json.dumps(payload),
        timeout=TIMEOUT,
    )

    if r.status_code in (200, 201):
        return True
    if r.status_code == 409:
        return False

    # Tratamento do caso "já existe" vindo como 400 (já aconteceu com folder)
    if r.status_code == 400 and "already in use" in (r.text or ""):
        return False

    raise RuntimeError(f"POST {parent_url} (Document id={doc_id}) -> {r.status_code} {r.text}")


def dest_upload_file(dest_sess: requests.Session, container_url: str, filename: str, blob: bytes,
                     dest_auth, content_type: str = None):
    upload_url = container_url.rstrip("/") + "/@upload"
    files = {"file": (filename, blob, content_type or "application/octet-stream")}
    r = dest_sess.post(upload_url, auth=dest_auth, headers=UPLOAD_HEADERS, files=files, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def ensure_dest_folder_chain(dest_sess: requests.Session, dest_auth, dest_root_url: str, full_dest_url: str):
    """
    Garante que TODAS as pastas no destino existam até o PAI do item final.
    Ex:
      root = http://host:8401/mpf2026
      item = http://host:8401/mpf2026/a/b/c/item
    cria (se faltar):
      /mpf2026/a
      /mpf2026/a/b
      /mpf2026/a/b/c
    """
    root_base, root_path = split_base_and_path(dest_root_url)
    dest_base, dest_path = split_base_and_path(full_dest_url)

    if root_base != dest_base:
        raise ValueError(f"Destino fora do host esperado. root={root_base} dest={dest_base}")

    parent_path, _sep, _leaf = dest_path.rpartition("/")
    if not parent_path:
        parent_path = "/"

    rp = root_path.rstrip("/")
    if not parent_path.startswith(rp):
        raise ValueError(f"Destino fora do root. parent_path={parent_path} root_path={rp}")

    rel = parent_path[len(rp):].strip("/")
    if not rel:
        return  # pai é o próprio root

    current_url = dest_root_url.rstrip("/")
    for seg in rel.split("/"):
        next_url = current_url + "/" + seg
        if not dest_exists(dest_sess, next_url, dest_auth):
            dest_create_folder(dest_sess, current_url, seg, seg, dest_auth)
        current_url = next_url


# =========================
# ORIGEM (SEM REST API) - páginas via métodos Zope
# =========================

def call_zope_method_text(orig_sess: requests.Session, base_url: str, method_name: str, params: dict, orig_auth):
    url = base_url.rstrip("/") + "/" + method_name
    if params:
        url += "?" + urlencode(params, doseq=True)
    r = orig_sess.get(url, auth=orig_auth, timeout=TIMEOUT, allow_redirects=True)
    r.raise_for_status()
    return r.text

def parse_metadados_text(txt: str) -> dict:
    out = {}
    for raw in (txt or "").splitlines():
        line = raw.strip()
        if not line or " = " not in line:
            continue
        k, v = line.split(" = ", 1)
        out[k.strip()] = v.strip()

    subj = out.get("subject", "")
    if subj:
        out["subject"] = [s.strip() for s in subj.split("#;#") if s.strip()]
    else:
        out["subject"] = []
    return out

def fetch_page_data_from_origin(orig_sess: requests.Session, page_url: str, orig_auth) -> PageData:
    """
    Chama os 2 métodos na ORIGEM.
    Assumido: métodos publicados no PAI do documento e recebem ?id=<id-do-objeto>
    """
    parent_url, obj_id = parent_and_id(page_url)

    meta_txt = call_zope_method_text(orig_sess, parent_url, ORIG_METHOD_META, {"id": obj_id}, orig_auth)
    body_txt = call_zope_method_text(orig_sess, parent_url, ORIG_METHOD_BODY, {"id": obj_id}, orig_auth)

    meta = parse_metadados_text(meta_txt)

    return PageData(
        id=meta.get("id", obj_id),
        caminho=meta.get("caminho", ""),
        titulo=meta.get("titulo", ""),
        descricao=meta.get("descricao", ""),
        creationDate=meta.get("creationDate", ""),
        effectiveDate=meta.get("effectiveDate", ""),
        expirationDate=meta.get("expirationDate", ""),
        subject=meta.get("subject", []),
        corpo_html=(body_txt or ""),
    )


# =========================
# MIGRAÇÃO
# =========================

def migrate_folder(dest_sess, row: Row, dest_auth):
    ensure_dest_folder_chain(dest_sess, dest_auth, DEST_ROOT_URL, row.url_destino)

    if dest_exists(dest_sess, row.url_destino, dest_auth):
        return "exists"

    parent_url, folder_id = parent_and_id(row.url_destino)
    dest_create_folder(dest_sess, parent_url, folder_id, folder_id, dest_auth)
    return "created"

def migrate_pagina(orig_sess, dest_sess, row: Row, orig_auth, dest_auth):
    if dest_exists(dest_sess, row.url_destino, dest_auth):
        return "exists"

    ensure_dest_folder_chain(dest_sess, dest_auth, DEST_ROOT_URL, row.url_destino)

    pd = fetch_page_data_from_origin(orig_sess, row.url_origem, orig_auth)

    parent_url, doc_id = parent_and_id(row.url_destino)
    created = dest_create_document(
        dest_sess,
        parent_url=parent_url,
        doc_id=doc_id,
        title=(pd.titulo or doc_id),
        html=pd.corpo_html,
        description=pd.descricao,
        subject=pd.subject,
        effective=pd.effectiveDate,
        expires=pd.expirationDate,
        dest_auth=dest_auth,
    )
    return "created" if created else "exists"

def migrate_arquivo(orig_sess, dest_sess, row: Row, orig_auth, dest_auth):
    # se já existe no destino, pula
    if dest_exists(dest_sess, row.url_destino, dest_auth):
        return "exists"

    # baixa da origem
    r = orig_sess.get(row.url_origem, auth=orig_auth, timeout=TIMEOUT, allow_redirects=True)
    r.raise_for_status()
    blob = r.content
    ctype = r.headers.get("Content-Type", "application/octet-stream")
    filename = guess_filename(r, row.url_origem)

    # destino
    parent_url, file_id = parent_and_id(row.url_destino)

    # garante cadeia até o pai
    ensure_dest_folder_chain(dest_sess, dest_auth, DEST_ROOT_URL, row.url_destino)

    # se o parent existir mas não for Folder, cria fallback "-files" ao lado
    parent_type = dest_get_type(dest_sess, parent_url, dest_auth)
    if parent_type and parent_type != "Folder":
        pparent_url, pid = parent_and_id(parent_url)
        fallback_id = pid + "-files"
        fallback_url = pparent_url.rstrip("/") + "/" + fallback_id
        if not dest_exists(dest_sess, fallback_url, dest_auth):
            dest_create_folder(dest_sess, pparent_url, fallback_id, fallback_id, dest_auth)
        parent_url = fallback_url

    # cria File via JSON/base64 (sem @upload)
    created = dest_create_file_json(dest_sess, parent_url, file_id, filename, blob, ctype, dest_auth)
    return "created" if created else "exists"


# =========================
# CSV
# =========================

def read_rows(csv_path: str):
    out = []
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        r = csv.DictReader(f, delimiter=";")

        for i, row in enumerate(r, start=2):
            tipo = norm_tipo(row.get("tipo"))
            uo = (row.get("url_origem") or "").strip()
            ud = (row.get("url_destino") or "").strip()

            if not tipo or not uo or not ud:
                raise ValueError(f"Linha {i} inválida no CSV: {row}")

            out.append(Row(tipo=tipo, url_origem=uo, url_destino=ud))

    return out


# =========================
# MAIN
# =========================

def main():
    if len(sys.argv) < 2:
        print("Uso: bulk_migration.py arquivo.csv", file=sys.stderr)
        sys.exit(2)

    csv_path = sys.argv[1]
    rows = read_rows(csv_path)

    orig_auth = get_origin_auth()
    dest_auth = (DEST_USER, DEST_PASS)

    orig_sess = requests.Session()
    dest_sess = requests.Session()

    ok = 0
    skip = 0
    fail = 0

    total = len(rows)

    for idx, row in enumerate(rows, start=1):
        try:
            if row.tipo == "folder":
                st = migrate_folder(dest_sess, row, dest_auth)
            elif row.tipo in ("pagina", "document", "page"):
                st = migrate_pagina(orig_sess, dest_sess, row, orig_auth, dest_auth)
            elif row.tipo in ("arquivo", "file"):
                st = migrate_arquivo(orig_sess, dest_sess, row, orig_auth, dest_auth)
            else:
                skip += 1
                print(f"[{idx}/{total}] SKIP tipo={row.tipo} :: {row.url_origem}")
                continue

            ok += 1
            print(f"[{idx}/{total}] OK {row.tipo} -> {st} :: {row.url_destino}")
            time.sleep(SLEEP_BETWEEN)

        except Exception as e:
            fail += 1
            print(f"[{idx}/{total}] FAIL {row.tipo} :: {row.url_origem} -> {row.url_destino}", file=sys.stderr)
            print(f"  ERRO: {e}", file=sys.stderr)

    print("\nResumo:")
    print(f"  OK: {ok}")
    print(f"  SKIP: {skip}")
    print(f"  FAIL: {fail}")


if __name__ == "__main__":
    main()
