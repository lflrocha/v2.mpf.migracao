from __future__ import annotations

import requests

from config import API_BASE, AUTH, HEADERS
from utils import guess_mime

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def unidade_from_caminho(caminho: str) -> str:
    unidade = (caminho or "").split("/")[2]

    if len(unidade) == 2:
        return f"pr{unidade}"
    if len(unidade) == 7:
        return f"prr{unidade[6]}"
    return unidade

def publish_item(session, item_url: str):
    r = session.post(f"{item_url.rstrip('/')}/@workflow/publish", headers=HEADERS, auth=AUTH, verify=False)
    if r.status_code not in (200, 204):
        raise Exception(f"Erro publicando: {r.status_code} {r.reason}\n{r.text}")



def ensure_year_folder(session: requests.Session, year: str) -> str:
    container = f"{API_BASE.rstrip('/')}/{year}"

    r = session.get(container, headers=HEADERS, auth=AUTH, verify=False)
    if r.status_code == 200:
        return container

    payload = {"@type": "Folder", "id": year, "title": year}
    rc = session.post(API_BASE, headers=HEADERS, auth=AUTH, json=payload, verify=False)
    if rc.status_code not in (200, 201):
        raise Exception(f"Erro criando pasta {year}: {rc.status_code} {rc.reason}\n{rc.text}")

    return container


def create_news_item(session: requests.Session, container_url: str, item: dict) -> dict:
    payload = {
        "@type": "Noticia",
        "title": item.get("title", ""),
        "description": item.get("description", ""),
        "tituloAlternativo": item.get("tituloAlternativo", ""),
        "descricaoAlternativa": item.get("descricaoAlternativa", ""),
        "tema": item.get("temas", []),
        "unidadeOrigem": item.get("unidadeOrigem", ""),
        "subjects": item.get("Subjects", []),
        "effective": item.get("effective"),  # opcional (se o seu tipo aceita)
        "text": {
            "data": item.get("text_html", ""),
            "content-type": "text/html",
            "encoding": "utf-8",
        },
        "image_caption": item.get("image_caption", ""),
    }

    if item.get("image_b64") and item.get("image_filename"):
        payload["image"] = {
            "filename": item["image_filename"],
            "content-type": guess_mime(item["image_filename"]),
            "data": item["image_b64"],
            "encoding": "base64",
        }

    r = session.post(container_url, headers=HEADERS, auth=AUTH, json=payload, verify=False)
    if r.status_code not in (200, 201):
        raise Exception(f"Erro criando noticia: {r.status_code} {r.reason}\n{r.text}")
    return r.json()
