#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Migração de notícias V2 (Plone antigo -> Plone novo)

Fluxo:
1) Lê LISTA_URL (texto: 1 URL por linha)
2) Para cada URL:
   - GET <url>/v2_getNoticiasMetadados  (texto chave = valor)
   - GET <url>/v2_getNoticiasCorpo      (HTML)
   - (opcional) GET <url>/v2_getNoticiasImagem (texto em linhas: url, filename, ...)

3) Cria a notícia no destino conforme caminho:
   /portal/pgr/     -> /o-mpf/unidades/procuradoria-geral-da-republica-pgr/noticias
   /portal/regiao1/ -> /o-mpf/unidades/prr1/noticias  (vale regiao1..6)
   /portal/<uf>/    -> /o-mpf/unidades/pr-<uf>/noticias

4) Ajusta o HTML:
   - remove wrapper <!DOCTYPE..><html..><body> ... </body></html>
   - baixa imagens e arquivos embutidos no corpo, cria como Image/File dentro da notícia nova
   - atualiza links no HTML e faz PATCH no campo text

5) Publica:
   - local=True  -> workflow transition "show"
   - local=False -> workflow transition "publish"

Requisitos: requests, bs4, pillow (para detectar tamanho quando necessário).
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import sys
import time
from io import BytesIO
from typing import Optional
from urllib.parse import urljoin, urlparse

import requests
import urllib3
from bs4 import BeautifulSoup
from PIL import Image as PILImage

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =========================
# CONFIG (por env vars)
# =========================

LISTA_URL = os.getenv(
    "LISTA_URL",
    "http://svlp-plnptapp01.pgr.mpf.mp.br:8401/portal/portal_skins/custom/v2_getNoticiasLista",
)

PLONE_URL = os.getenv("PLONE_URL", "https://www-cdn.mpf.mp.br/").rstrip("/")
PLONE_USER = os.getenv("PLONE_USER", "admin")
PLONE_PASS = os.getenv("PLONE_PASS", "Q7!mR2@x#9Lp")
AUTH = (PLONE_USER, PLONE_PASS)

HEADERS_JSON = {
    "Accept": "application/json",
    "Content-Type": "application/json",
}
HEADERS_ACCEPT = {"Accept": "application/json"}

SLEEP_BETWEEN = float(os.getenv("SLEEP_BETWEEN", "0.05"))
TIMEOUT = int(os.getenv("TIMEOUT", "60"))
VERIFY_TLS = os.getenv("VERIFY_TLS", "0").strip() not in ("0", "false", "False", "no", "NO")

DRY_RUN = os.getenv("DRY_RUN", "0").strip() in ("1", "true", "True", "yes", "YES")

STATE_FILE = os.getenv("STATE_FILE", "import_state.json")

# Mapeamento de tema (classificacaoNoticia) -> id do vocabulário no destino
# ORIGEM -> DESTINO (path no destino, sem domínio)
ORIG_PREFIX_TO_DEST_PATH = {
    "/portal/pgr/": "/o-mpf/unidades/procuradoria-geral-da-republica-pgr/noticias",
    "/portal/regiao1/": "/o-mpf/unidades/prr1/noticias",
    "/portal/regiao2/": "/o-mpf/unidades/prr2/noticias",
    "/portal/regiao3/": "/o-mpf/unidades/prr3/noticias",
    "/portal/regiao4/": "/o-mpf/unidades/prr4/noticias",
    "/portal/regiao5/": "/o-mpf/unidades/prr5/noticias",
    "/portal/regiao6/": "/o-mpf/unidades/prr6/noticias",
    "/portal/ac/": "/o-mpf/unidades/pr-ac/noticias",
}

# -------------------------
# Utils
# -------------------------

def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_state(state: dict) -> None:
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, STATE_FILE)

def parse_kv_lines(text: str) -> dict:
    meta = {}
    for line in (text or "").splitlines():
        if " = " in line:
            k, v = line.split(" = ", 1)
            meta[k.strip()] = v.strip()
    return meta

def strip_body_wrappers(html: str) -> str:
    """Remove wrappers exatos citados: doctype/html/body e fecha html/body."""
    if not html:
        return ""
    s = html.strip()

    # Remove prefixo doctype/html/body (tolerando espaços)
    s = re.sub(r"(?is)^\s*<!DOCTYPE[^>]*>\s*<html[^>]*>\s*<body[^>]*>\s*", "", s)
    # Remove sufixo </body></html>
    s = re.sub(r"(?is)\s*</body>\s*</html>\s*$", "", s)
    return s.strip()

def guess_mime(filename: str) -> str:
    fn = (filename or "").lower()
    if fn.endswith((".jpg", ".jpeg")):
        return "image/jpeg"
    if fn.endswith(".png"):
        return "image/png"
    if fn.endswith(".gif"):
        return "image/gif"
    if fn.endswith(".webp"):
        return "image/webp"
    if fn.endswith(".pdf"):
        return "application/pdf"
    return "application/octet-stream"

def is_true(v: str) -> bool:
    return (v or "").strip().lower() in ("1", "true", "sim", "yes")

def subjects_from_meta(meta: dict) -> list[str]:
    raw = meta.get("subject", "")
    return [x.strip() for x in raw.split("#;#") if x.strip()]


def unidade_origem_from_caminho(caminho: str) -> str:
    """Gera token de unidadeOrigem a partir do caminho da origem.

    Padrões esperados no destino (conforme widget):
      - pgr
      - prr1..prr6
      - pr<uf> (sem hífen), ex.: pral, prac, prsp, prdf
      - pfdc (se aplicável)
    """
    caminho = (caminho or "").strip()
    if not caminho.startswith("/"):
        caminho = "/" + caminho

    m = re.match(r"^/portal/([^/]+)/", caminho)
    if not m:
        return ""

    codigo = m.group(1).lower()

    if codigo == "pgr":
        return "pgr"

    if codigo == "pfdc":
        return "pfdc"

    mreg = re.match(r"^regiao([1-6])$", codigo)
    if mreg:
        return f"prr{mreg.group(1)}"

    # UFs e demais códigos: padrão pr<codigo> (sem hífen)
    return f"pr{codigo}"


def tema_from_meta(meta: dict) -> str:
    """No destino, `tema` é string. Na origem pode vir como lista/linha com separador.

    Regra: pegar o primeiro tema disponível nesta ordem:
    1) meta['tema']
    2) meta['temas']
    3) meta['classificacaoNoticia']
    """
    for key in ("tema", "temas", "classificacaoNoticia"):
        raw = (meta.get(key) or "").strip()
        if not raw or raw in ("None",):
            continue
        # pode vir separado por #;# (ou vírgula, em alguns casos)
        parts = [x.strip() for x in raw.split("#;#") if x.strip()]
        if not parts and "," in raw:
            parts = [x.strip() for x in raw.split(",") if x.strip()]
        if parts:
            return parts[0]
        return raw
    return ""
    # Pode vir com #;# também
    for t in [x.strip() for x in classificacao.split("#;#") if x.strip()]:
        if t in TEMA_PARA_ID:
            return TEMA_PARA_ID[t]
    return ""

def destino_path_from_caminho(caminho: str) -> str:
    caminho = (caminho or "").strip()
    if not caminho.startswith("/"):
        caminho = "/" + caminho

    for orig_prefix, dest_path in ORIG_PREFIX_TO_DEST_PATH.items():
        if caminho.startswith(orig_prefix):
            return dest_path

    # fallback automático: /portal/<codigo>/...
    m = re.match(r"^/portal/([^/]+)/", caminho)
    if not m:
        raise ValueError(f"Não foi possível identificar unidade a partir do caminho: {caminho}")
    codigo = m.group(1).lower()

    if codigo == "pgr":
        return ORIG_PREFIX_TO_DEST_PATH["/portal/pgr/"]

    mreg = re.match(r"^regiao([1-6])$", codigo)
    if mreg:
        n = mreg.group(1)
        return f"/o-mpf/unidades/prr{n}/noticias"

    # UF: qualquer código vira pr-<uf>
    return f"/o-mpf/unidades/pr-{codigo}/noticias"

def join_v2_endpoint(news_url: str, endpoint: str) -> str:
    base = news_url.rstrip("/")
    endpoint = endpoint.lstrip("/")
    return f"{base}/{endpoint}"

# -------------------------
# Plone destination helpers
# -------------------------

def ensure_path_folders(session: requests.Session, dest_path: str) -> str:
    """Garante que a hierarquia do dest_path exista (criando Folder). Retorna URL completa."""
    dest_path = (dest_path or "").strip()
    if not dest_path.startswith("/"):
        dest_path = "/" + dest_path
    parts = [p for p in dest_path.split("/") if p]
    current_url = PLONE_URL
    for part in parts:
        next_url = f"{current_url}/{part}"
        r = session.get(next_url, headers=HEADERS_ACCEPT, auth=AUTH, timeout=TIMEOUT, verify=VERIFY_TLS)
        if r.status_code == 200:
            current_url = next_url
            continue

        if DRY_RUN:
            current_url = next_url
            continue

        payload = {"@type": "Folder", "id": part, "title": part}
        pr = session.post(current_url, headers=HEADERS_JSON, auth=AUTH, json=payload, timeout=TIMEOUT, verify=VERIFY_TLS)
        if pr.status_code not in (200, 201):
            raise RuntimeError(f"Não foi possível criar pasta '{part}' em {current_url}: {pr.status_code} {pr.text}")
        current_url = next_url
    return f"{PLONE_URL}/{dest_path.lstrip('/')}"

def create_news_item(session: requests.Session, container_url: str, meta: dict, text_html: str, image_info: dict) -> dict:
    payload = {
        "@type": "Noticia",
        "id": meta.get("id") or None,
        "title": meta.get("titulo", ""),
        "description": meta.get("descricao", ""),
        "tituloAlternativo": meta.get("tituloAlternativo", ""),
        "descricaoAlternativa": meta.get("descricaoAlternativo", meta.get("descricaoAlternativa", "")),
        "dicaAcessibilidade": meta.get("dicaAcessibilidade", ""),
        "tema": tema_from_meta(meta),
        "unidadeOrigem": unidade_origem_from_caminho(meta.get("caminho", "")),
        "descricaoImagem": meta.get("descricaoImagem", ""),
        "subjects": subjects_from_meta(meta),
        "effective": meta.get("effectiveDate") if meta.get("effectiveDate") not in ("None", "", None) else None,
        "expires": meta.get("expirationDate") if meta.get("expirationDate") not in ("None", "", None) else None,
        "text": {
            "data": text_html or "",
            "content-type": "text/html",
            "encoding": "utf-8",
        },
    }

    # Remove chaves None para não causar validação ruim
    payload = {k: v for k, v in payload.items() if v is not None}

    if image_info.get("data_b64") and image_info.get("filename"):
        payload["image"] = {
            "filename": image_info["filename"],
            "content-type": guess_mime(image_info["filename"]),
            "data": image_info["data_b64"],
            "encoding": "base64",
        }

    if DRY_RUN:
        return {"@id": f"{container_url.rstrip('/')}/{meta.get('id','fake')}"}

    r = session.post(container_url, headers=HEADERS_JSON, auth=AUTH, json=payload, timeout=TIMEOUT, verify=VERIFY_TLS)
    if r.status_code not in (200, 201):
        raise Exception(f"Erro criando noticia: {r.status_code} {r.reason}\n{r.text}")
    return r.json()

def patch_news_text(session: requests.Session, news_url: str, new_html: str) -> None:
    if DRY_RUN:
        return
    payload = {
        "text": {
            "data": new_html,
            "content-type": "text/html",
            "encoding": "utf-8",
        }
    }
    r = session.patch(news_url, headers=HEADERS_JSON, auth=AUTH, json=payload, timeout=TIMEOUT, verify=VERIFY_TLS)
    if r.status_code not in (200, 204):
        raise Exception(f"Erro dando PATCH no text: {r.status_code} {r.reason}\n{r.text}")

def publish_item(session: requests.Session, news_url: str, local: bool) -> None:
    transition = "show" if local else "publish"
    if DRY_RUN:
        return
    r = session.post(
        f"{news_url.rstrip('/')}/@workflow/{transition}",
        headers=HEADERS_ACCEPT,
        auth=AUTH,
        timeout=TIMEOUT,
        verify=VERIFY_TLS,
    )
    if r.status_code not in (200, 204):
        raise Exception(f"Erro executando transição '{transition}': {r.status_code} {r.reason}\n{r.text}")

# -------------------------
# Embedded assets (images/files) in body HTML
# -------------------------

FILE_EXTS = {
    ".pdf",".doc",".docx",".xls",".xlsx",".ppt",".pptx",".zip",".rar",".7z",".csv",".txt",
    ".odt",".ods",".odp",".rtf",
}
def looks_like_file_link(href: str) -> bool:
    try:
        path = urlparse(href).path.lower()
    except Exception:
        path = href.lower()
    return any(path.endswith(ext) for ext in FILE_EXTS)

def is_internal_or_local(url: str) -> bool:
    """Retorna True para:
    - URLs relativas (ex: ../docs/... ou docs/...)
    - URLs absolutas do domínio interno/MPF
    - caminhos absolutos iniciando com /
    """
    if not url:
        return False
    u = url.strip()

    # Caminho absoluto dentro do site
    if u.startswith("/"):
        return True

    pu = urlparse(u)

    # URL relativa (sem scheme e sem netloc)
    if not pu.scheme and not pu.netloc:
        return True

    host = (pu.netloc or "").lower()
    return any(h in host for h in ("mpf.mp.br", "pgr.mpf.mp.br", "svlp-plnptapp01"))


def filename_from_any_url(url: str, fallback_ext: str) -> str:
    p = urlparse(url).path
    name = (p.rsplit("/", 1)[-1] or "").strip()
    if not name:
        return "arquivo" + fallback_ext
    if "." not in name:
        return name + fallback_ext
    return name

def unique_id_from_source(filename: str, source_url: str) -> str:
    base = re.sub(r"[^a-zA-Z0-9\-]+", "-", (filename or "asset").lower()).strip("-")
    h = hashlib.sha1((source_url or filename).encode("utf-8")).hexdigest()[:8]
    return f"{base}-{h}"[:60]

def original_image_url(abs_src: str) -> str:
    # Se for algo como .../image_thumb ou @@images/... volta pra antes
    if "/@@images/" in abs_src:
        return abs_src.split("/@@images/", 1)[0]
    return abs_src

def image_size_from_bytes(data: bytes):
    try:
        im = PILImage.open(BytesIO(data))
        return im.size
    except Exception:
        return (None, None)

SCALES = [
    ("large", 768),
    ("preview", 400),
    ("mini", 200),
    ("thumb", 128),
    ("tile", 64),
    ("icon", 32),
    ("listing", 16),
]
def pick_scale_for_max_side(max_side: Optional[int]) -> str:
    if not max_side:
        return "preview"
    for name, limit in SCALES:
        if max_side >= limit:
            return name
    return "listing"

def max_side_from_img_tag(img_tag) -> Optional[int]:
    w = img_tag.get("width")
    h = img_tag.get("height")
    try:
        if w and h:
            return max(int(w), int(h))
    except Exception:
        pass
    # tenta style width/height
    style = (img_tag.get("style") or "")
    m1 = re.search(r"width\s*:\s*(\d+)px", style)
    m2 = re.search(r"height\s*:\s*(\d+)px", style)
    vals = []
    if m1: vals.append(int(m1.group(1)))
    if m2: vals.append(int(m2.group(1)))
    return max(vals) if vals else None

def create_dx_image(session: requests.Session, parent_url: str, filename: str, data_bytes: bytes, source_url: str) -> str:
    b64 = base64.b64encode(data_bytes).decode("utf-8")
    image_id = unique_id_from_source(filename, source_url)

    payload = {
        "@type": "Image",
        "id": image_id,
        "title": filename,
        "image": {
            "filename": filename,
            "content-type": guess_mime(filename),
            "data": b64,
            "encoding": "base64",
        },
    }

    if DRY_RUN:
        return f"{parent_url.rstrip('/')}/{image_id}"

    r = session.post(parent_url, headers=HEADERS_JSON, auth=AUTH, json=payload, timeout=TIMEOUT, verify=VERIFY_TLS)
    if r.status_code in (200, 201):
        return r.json().get("@id")

    if r.status_code == 400 and "already in use" in (r.text or ""):
        for i in range(2, 10):
            payload["id"] = f"{image_id}-v{i}"
            rr = session.post(parent_url, headers=HEADERS_JSON, auth=AUTH, json=payload, timeout=TIMEOUT, verify=VERIFY_TLS)
            if rr.status_code in (200, 201):
                return rr.json().get("@id")

    raise Exception(f"Erro criando Image {filename}: {r.status_code} {r.reason}\n{r.text}")

def create_dx_file(session: requests.Session, parent_url: str, filename: str, data_bytes: bytes, source_url: str) -> str:
    b64 = base64.b64encode(data_bytes).decode("utf-8")
    file_id = unique_id_from_source(filename, source_url)

    payload = {
        "@type": "File",
        "id": file_id,
        "title": filename,
        "file": {
            "filename": filename,
            "content-type": guess_mime(filename),
            "data": b64,
            "encoding": "base64",
        },
    }

    if DRY_RUN:
        return f"{parent_url.rstrip('/')}/{file_id}"

    r = session.post(parent_url, headers=HEADERS_JSON, auth=AUTH, json=payload, timeout=TIMEOUT, verify=VERIFY_TLS)
    if r.status_code in (200, 201):
        return r.json().get("@id")

    if r.status_code == 400 and "already in use" in (r.text or ""):
        for i in range(2, 10):
            payload["id"] = f"{file_id}-v{i}"
            rr = session.post(parent_url, headers=HEADERS_JSON, auth=AUTH, json=payload, timeout=TIMEOUT, verify=VERIFY_TLS)
            if rr.status_code in (200, 201):
                return rr.json().get("@id")

    raise Exception(f"Erro criando File {filename}: {r.status_code} {r.reason}\n{r.text}")

def migrate_embedded_assets(session: requests.Session, old_base_url: str, new_news_url: str, html: str) -> str:
    soup = BeautifulSoup(html or "", "html.parser")

    created_cache: dict[str, str] = {}

    # Imagens
    for img in soup.find_all("img"):
        src = (img.get("src") or "").strip()
        if not src or not is_internal_or_local(src):
            continue

        abs_src = urljoin(old_base_url.rstrip("/") + "/", src)
        abs_original = original_image_url(abs_src)

        # tenta baixar a imagem do jeito mais confiável:
        # 1) se o src já é @@images/... (scale), isso normalmente já retorna bytes da imagem
        # 2) tenta @@download/image no objeto base
        # 3) por último tenta o src "base" (pode retornar HTML, então validamos content-type)
        max_side = max_side_from_img_tag(img)
        if not max_side:
            resp_scaled = session.get(abs_src, timeout=TIMEOUT, verify=VERIFY_TLS)
            if resp_scaled.status_code == 200:
                w_s, h_s = image_size_from_bytes(resp_scaled.content)
                if w_s and h_s:
                    max_side = max(w_s, h_s)

        scale = pick_scale_for_max_side(max_side)

        candidates = []
        candidates.append(abs_src)

        base_obj = abs_src.split("/@@images/", 1)[0] if "/@@images/" in abs_src else abs_original
        candidates.append(base_obj.rstrip("/") + "/@@download/image")
        candidates.append(abs_original)

        img_bytes = None
        chosen_url = None
        for cand in candidates:
            try:
                rr = session.get(cand, timeout=TIMEOUT, verify=VERIFY_TLS)
            except Exception:
                continue
            if rr.status_code != 200:
                continue
            ctype = (rr.headers.get("Content-Type") or "").lower()
            if ctype.startswith("image/") or rr.content[:4] in (b"\xff\xd8\xff\xe0", b"\x89PNG", b"GIF8"):
                img_bytes = rr.content
                chosen_url = cand
                break

        if not img_bytes:
            print("Falha baixando img:", abs_src)
            continue

        filename = filename_from_any_url(base_obj, fallback_ext=".jpg")
        img_obj_url = create_dx_image(session, new_news_url, filename, img_bytes, chosen_url or abs_src)

        img["src"] = f"{img_obj_url.rstrip('/')}/@@images/image/{scale}"

    # Arquivos
    for a in soup.find_all("a"):
        href = (a.get("href") or "").strip()
        if not href or not looks_like_file_link(href) or not is_internal_or_local(href):
            continue

        abs_url = urljoin(old_base_url.rstrip("/") + "/", href)
        if abs_url in created_cache:
            a["href"] = created_cache[abs_url]
            continue

        resp = session.get(abs_url, timeout=TIMEOUT, verify=VERIFY_TLS)
        if resp.status_code != 200:
            print("Falha baixando arquivo:", abs_url, resp.status_code)
            continue

        filename = filename_from_any_url(abs_url, fallback_ext=".bin")
        file_obj_url = create_dx_file(session, new_news_url, filename, resp.content, abs_url)

        created_cache[abs_url] = file_obj_url
        a["href"] = file_obj_url

    return str(soup)

# -------------------------
# Origem: fetchers
# -------------------------

def fetch_lista(session: requests.Session) -> list[str]:
    r = session.get(LISTA_URL, timeout=TIMEOUT, verify=False)
    r.raise_for_status()
    urls = []
    for line in r.text.splitlines():
        u = line.strip()
        if not u:
            continue
        urls.append(u)
    return urls

def fetch_metadados(session: requests.Session, old_url: str) -> dict:
    r = session.get(join_v2_endpoint(old_url, "v2_getNoticiasMetadados"), timeout=TIMEOUT, verify=False)
    r.raise_for_status()
    return parse_kv_lines(r.text)

def fetch_corpo(session: requests.Session, old_url: str) -> str:
    r = session.get(join_v2_endpoint(old_url, "v2_getNoticiasCorpo"), timeout=TIMEOUT, verify=False)
    r.raise_for_status()
    return strip_body_wrappers(r.text)

def fetch_imagem_principal(session: requests.Session, old_url: str) -> dict:
    """Tenta pegar imagem principal se o endpoint existir. Não falha se não existir."""
    try:
        r = session.get(join_v2_endpoint(old_url, "v2_getNoticiasImagem"), timeout=TIMEOUT, verify=False)
        if r.status_code != 200:
            return {}
        lines = [l.strip() for l in r.text.splitlines() if l.strip()]
        if not lines:
            return {}
        img_url = lines[0] if len(lines) > 0 else ""
        filename = lines[1] if len(lines) > 1 else ""
        caption = ""
        if len(lines) > 2 and " = " in lines[2]:
            caption = lines[2].split(" = ", 1)[1].strip()

        if not img_url:
            return {}

        img_resp = session.get(img_url, timeout=TIMEOUT, verify=False)
        if img_resp.status_code != 200:
            return {}

        return {
            "filename": filename or filename_from_any_url(img_url, ".jpg"),
            "caption": caption,
            "data_b64": base64.b64encode(img_resp.content).decode("utf-8"),
        }
    except Exception:
        return {}

# -------------------------
# Main
# -------------------------

def migrate_one(old_session: requests.Session, new_session: requests.Session, old_url: str, state: dict, idx: int, total: int) -> None:
    key = old_url.strip()
    if state.get(key) == "ok":
        return

    meta = fetch_metadados(old_session, old_url)
    corpo_html = fetch_corpo(old_session, old_url)
    img_info = fetch_imagem_principal(old_session, old_url)

    caminho = meta.get("caminho", "")
    dest_path = destino_path_from_caminho(caminho)
    container_url = ensure_path_folders(new_session, dest_path)

    local_flag = is_true(meta.get("local", "False"))

    # Cria notícia com HTML "cru" primeiro
    created = create_news_item(new_session, container_url, meta, corpo_html, img_info)
    new_url = created.get("@id") or ""
    if not new_url:
        raise RuntimeError("Resposta sem @id ao criar notícia")

    # Migra assets embutidos e aplica PATCH no corpo
    patched_html = migrate_embedded_assets(old_session, old_url, new_url, corpo_html)
    patch_news_text(new_session, new_url, patched_html)

    # Publica conforme local
    publish_item(new_session, new_url, local_flag)

    state[key] = "ok"
    save_state(state)

    print(f"[OK] ({idx}/{total}) {meta.get('id','')} -> {new_url}")

def main():
    old_session = requests.Session()
    new_session = requests.Session()

    state = load_state()
    urls = fetch_lista(old_session)

    total = len(urls)
    for i, old_url in enumerate(urls, start=1):
        try:
            if SLEEP_BETWEEN:
                time.sleep(SLEEP_BETWEEN)
            migrate_one(old_session, new_session, old_url, state, i, total)
        except Exception as e:
            print(f"[ERRO] ({i}/{total}) {old_url}\n  {e}")
            state[old_url.strip()] = f"erro: {e}"
            save_state(state)

if __name__ == "__main__":
    main()
