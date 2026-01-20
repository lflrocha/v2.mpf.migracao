from __future__ import annotations

import re
from datetime import datetime
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from config import FILE_EXTS, INTERNAL_HOSTS



def _stamp_hms():
    return datetime.now().strftime("%H:%M:%S")

def _fmt(i, total):
    return f"({i}/{total})"

def year_from_effective(effective_iso: str) -> str:
    # ex: 2026-01-19T09:10:00-03:00
    return str(datetime.fromisoformat(effective_iso).year)


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
    return "application/octet-stream"


def parse_kv_lines(text: str) -> dict:
    meta = {}
    for line in (text or "").splitlines():
        if " = " in line:
            k, v = line.split(" = ", 1)
            meta[k.strip()] = v.strip()
    return meta


def clean_html(corpo_html: str) -> str:
    soup = BeautifulSoup(corpo_html or "", "html.parser")

    for span in soup.find_all("span"):
        span.unwrap()

    for tag in soup.find_all(True):
        tag.attrs.pop("align", None)
        tag.attrs.pop("dir", None)

        if "class" in tag.attrs:
            classes = [c for c in tag["class"] if c != "pf0"]
            if classes:
                tag["class"] = classes
            else:
                del tag["class"]

    for p in soup.find_all("p"):
        p.attrs.pop("style", None)

    return soup.body.decode_contents().strip() if soup.body else soup.decode().strip()


def is_internal_or_local(url: str) -> bool:
    """
    Considera interno/local:
      - relativo (/ ./ ../)
      - absoluto com host igual ao host do LISTA_URL/portal antigo (ajuste em INTERNAL_HOSTS)
    """
    if not url:
        return False
    u = url.strip()
    if u.startswith(("#", "mailto:", "tel:", "data:")):
        return False
    if u.startswith(("/", "./", "../")):
        return True
    if u.startswith("//"):
        u = "http:" + u

    p = urlparse(u)
    if p.scheme not in ("http", "https"):
        return False

    host = (p.netloc or "").lower()
    return host in INTERNAL_HOSTS


def clean_filename(name: str) -> str:
    """
    Normaliza nome de arquivo para usar como id e filename.
    Mantém extensão.
    """
    name = (name or "").strip()
    name = name.replace("\\", "/").split("/")[-1]  # remove path
    name = re.sub(r"\s+", "-", name)
    name = re.sub(r"[^A-Za-z0-9\-_.]+", "-", name)
    name = re.sub(r"-{2,}", "-", name).strip("-")
    return name or "arquivo"


def id_from_filename(filename: str) -> str:
    """
    ID seguro pro Plone (sem pontos demais, lowercase).
    """
    fn = (filename or "").lower()
    fn = clean_filename(fn)
    # Plone aceita ponto, mas pra evitar id estranho, troca pontos por hífen exceto na extensão
    if "." in fn:
        base, ext = fn.rsplit(".", 1)
        base = base.replace(".", "-")
        return f"{base}.{ext}"
    return fn


def filename_from_any_url(u: str, fallback_ext: str = ".bin") -> str:
    path = urlparse(u).path
    name = path.rstrip("/").split("/")[-1] or "arquivo"
    if "." not in name:
        name += fallback_ext
    return clean_filename(name)


def looks_like_file_link(href: str) -> bool:
    if not href:
        return False
    u = href.strip()
    if u.startswith("//"):
        u = "http:" + u
    path = urlparse(u).path.lower()
    return any(path.endswith(ext) for ext in FILE_EXTS)
