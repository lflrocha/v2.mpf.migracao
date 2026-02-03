from __future__ import annotations

import base64
import hashlib
from io import BytesIO
from PIL import Image as PILImage
from urllib.parse import urljoin, urlparse
import re
from bs4 import BeautifulSoup
from typing import Optional

from config import AUTH, HEADERS
from utils import (
    filename_from_any_url,
    guess_mime,
    id_from_filename,
    is_internal_or_local,
    looks_like_file_link,
)


SCALES = [
    ("large", 768),
    ("preview", 400),
    ("mini", 200),
    ("thumb", 128),
    ("tile", 64),
    ("icon", 32),
    ("listing", 16),
]

def image_size_from_bytes(data: bytes):
    try:
        im = PILImage.open(BytesIO(data))
        return im.size  # (w, h)
    except Exception:
        return (None, None)

def pick_scale_for_max_side(max_side: Optional[int]) -> str:
    if not max_side:
        return "preview"
    # maior scale cujo limite <= max_side
    for name, limit in SCALES:
        if max_side >= limit:
            return name
    return "listing"

def max_side_from_img_tag(img_tag) -> int | None:
    # 1) width/height attributes
    w = img_tag.get("width")
    h = img_tag.get("height")
    try:
        if w and h:
            return max(int(w), int(h))
    except Exception:
        pass

    # 2) style="width: 200px; height: 150px"
    style = img_tag.get("style") or ""
    m1 = re.search(r"width\s*:\s*(\d+)px", style)
    m2 = re.search(r"height\s*:\s*(\d+)px", style)
    if m1 or m2:
        ww = int(m1.group(1)) if m1 else 0
        hh = int(m2.group(1)) if m2 else 0
        return max(ww, hh) or None

    return None

def original_image_url(src: str) -> str:
    """
    Se vier com /@@images/... retorna a URL base (original) antes do @@images.
    Caso contrário, retorna o src como está.
    """
    src = (src or "").strip()
    if "/@@images/" in src:
        return src.split("/@@images/", 1)[0]
    return src

def image_size_from_bytes(data: bytes) -> tuple[int, int] | tuple[None, None]:
    try:
        im = PILImage.open(BytesIO(data))
        return im.size  # (width, height)
    except Exception:
        return (None, None)

def pick_scale(width: int | None, height: int | None) -> str:
    """
    Escolhe o scale mais “adequado” pelo maior lado (max(width, height)).
    Regra: pega o MAIOR scale cujo limite seja <= maior lado.
    Se não conseguir medir, cai em preview.
    """
    if not width or not height:
        return "preview"
    m = max(width, height)
    for name, limit in SCALES:
        if m >= limit:
            return name
    return "listing"


def _split_name_ext(filename: str):
    name = (filename or "").strip()
    if "." in name:
        base, ext = name.rsplit(".", 1)
        return base, "." + ext
    return name, ""

def unique_id_from_source(filename: str, source_url: str) -> str:
    """
    Gera um id estável e único a partir do nome do arquivo + hash da URL origem.
    """
    base, ext = _split_name_ext(filename)
    base = id_from_filename(base)  # reaproveita sua normalização
    h = hashlib.md5((source_url or "").encode("utf-8")).hexdigest()[:6]
    return f"{base}-{h}{ext}" if ext else f"{base}-{h}"

def create_dx_image(session, parent_url: str, filename: str, data_bytes: bytes, source_url: str) -> str:
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

    r = session.post(parent_url, headers=HEADERS, auth=AUTH, json=payload, verify=False)
    if r.status_code in (200, 201):
        return r.json().get("@id")

    # Se ainda assim colidir (muito improvável), tenta sems-2, -3...
    if r.status_code == 400 and "already in use" in (r.text or ""):
        for i in range(2, 10):
            payload["id"] = f"{image_id}-v{i}"
            rr = session.post(parent_url, headers=HEADERS, auth=AUTH, json=payload, verify=False)
            if rr.status_code in (200, 201):
                return rr.json().get("@id")

    raise Exception(f"Erro criando Image {filename}: {r.status_code} {r.reason}\n{r.text}")


def create_dx_file(session, parent_url: str, filename: str, data_bytes: bytes, source_url: str) -> str:
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

    r = session.post(parent_url, headers=HEADERS, auth=AUTH, json=payload, verify=False)
    if r.status_code in (200, 201):
        return r.json().get("@id")

    if r.status_code == 400 and "already in use" in (r.text or ""):
        for i in range(2, 10):
            payload["id"] = f"{file_id}-v{i}"
            rr = session.post(parent_url, headers=HEADERS, auth=AUTH, json=payload, verify=False)
            if rr.status_code in (200, 201):
                return rr.json().get("@id")

    raise Exception(f"Erro criando File {filename}: {r.status_code} {r.reason}\n{r.text}")


def patch_news_text(session, news_url: str, new_html: str):
    payload = {
        "text": {
            "data": new_html,
            "content-type": "text/html",
            "encoding": "utf-8",
        }
    }
    r = session.patch(news_url, headers=HEADERS, auth=AUTH, json=payload, verify=False)
    if r.status_code not in (200, 204):
        raise Exception(f"Erro dando PATCH no text: {r.status_code} {r.reason}\n{r.text}")


def migrate_embedded_assets(session, old_base_url: str, new_news_url: str, html: str) -> str:
    """
    - old_base_url: URL antiga base (use a própria URL da notícia antiga)
    - new_news_url: URL da notícia criada no Plone novo
    - html: corpo da notícia
    Retorna HTML com links ajustados.
    """
    soup = BeautifulSoup(html or "", "html.parser")

    # cache pra não subir o mesmo asset repetido
    created_cache = {}  # abs_url -> new_url

    # 1) IMAGENS
    for img in soup.find_all("img"):
        src = (img.get("src") or "").strip()
        if not src or not is_internal_or_local(src):
            continue

        abs_src = urljoin(old_base_url.rstrip("/") + "/", src)

        # NOVO: se for @@images, pega a original antes do @@images
        abs_original = original_image_url(abs_src)


        max_side = max_side_from_img_tag(img)
        if not max_side:
            resp_scaled = session.get(abs_src)
            if resp_scaled.status_code == 200:
                w_s, h_s = image_size_from_bytes(resp_scaled.content)
                if w_s and h_s:
                    max_side = max(w_s, h_s)

        scale = pick_scale_for_max_side(max_side)

        resp_original = session.get(abs_original)
        if resp_original.status_code != 200:
            print("Falha baixando img original:", abs_original, resp_original.status_code)
            continue

        filename = filename_from_any_url(abs_original, fallback_ext=".jpg")
        img_obj_url = create_dx_image(session, new_news_url, filename, resp_original.content, abs_original)

        # linka com o scale escolhido
        img["src"] = f"{img_obj_url.rstrip('/')}/@@images/image/{scale}"


    # 2) ARQUIVOS (links)
    for a in soup.find_all("a"):
        href = (a.get("href") or "").strip()
        if not href or not looks_like_file_link(href) or not is_internal_or_local(href):
            continue

        abs_url = urljoin(old_base_url.rstrip("/") + "/", href)
        if abs_url in created_cache:
            a["href"] = created_cache[abs_url]
            continue

        resp = session.get(abs_url)
        if resp.status_code != 200:
            print("Falha baixando arquivo:", abs_url, resp.status_code)
            continue

        filename = filename_from_any_url(abs_url, fallback_ext=".bin")
        file_obj_url = create_dx_file(session, new_news_url, filename, resp.content, abs_url)

        created_cache[abs_url] = file_obj_url
        a["href"] = file_obj_url

    body = soup.body
    return body.decode_contents().strip() if body else str(soup)
