# -*- coding: utf-8 -*-
import base64
import json
import os
import requests

PLONE_URL = "http://170.187.151.174:8080/mpf2026"      # site
CONTAINER_PATH = "/noticias"                           # pasta onde criar
API_BASE = PLONE_URL.rstrip("/") + "/" + CONTAINER_PATH

USERNAME = "admin"
PASSWORD = "zope"

HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
}

AUTH = (USERNAME, PASSWORD)


def create_news_item(item):
    """
    item = dict com chaves:
      title, description, tituloAlternativo, descricaoAlternativa,
      temas (list), unidadeOrigem (str), text_html (str),
      image_path (str ou None)
    """

    payload = {
        "@type": "Noticia",  # ajuste se o ID do tipo for diferente
        "title": item.get("title", ""),
        "description": item.get("description", ""),

        "tituloAlternativo": item.get("tituloAlternativo", ""),
        "descricaoAlternativa": item.get("descricaoAlternativa", ""),

        "temas": item.get("temas", []),
        "unidadeOrigem": item.get("unidadeOrigem", ""),

        # campo RichText do behavior: normalmente espera {"data": "...", "content-type": "..."}
        "text": {
            "data": item.get("text_html", ""),
            "content-type": "text/html",
            "encoding": "utf-8",
        },

        "effective": "2025-12-15T15:01:39-03:00",
        "subjects": ["tag1", "tag2", "tag3"],

    }

    # imagem (behavior image): mandar como base64
    image_path = item.get("image_path")
    if image_path:
        with open(image_path, "rb") as f:
            data = f.read()
        b64 = base64.b64encode(data).decode("ascii")
        filename = os.path.basename(image_path)

        payload["image"] = {
            "filename": filename,
            "content-type": _guess_mime(filename),
            "data": b64,
            "encoding": "base64",
        }

    r = requests.post(API_BASE, headers=HEADERS, auth=AUTH, data=json.dumps(payload))

    if r.status_code not in (200, 201):
        raise Exception("Erro criando noticia: {} {}\n{}".format(r.status_code, r.reason, r.text))

    return r.json()


def _guess_mime(filename):
    fn = filename.lower()
    if fn.endswith(".jpg") or fn.endswith(".jpeg"):
        return "image/jpeg"
    if fn.endswith(".png"):
        return "image/png"
    if fn.endswith(".gif"):
        return "image/gif"
    if fn.endswith(".webp"):
        return "image/webp"
    return "application/octet-stream"


if __name__ == "__main__":
    noticia = {
        "title": "Meu título",
        "description": "Minha descrição",
        "tituloAlternativo": "Título de capa",
        "descricaoAlternativa": "Descrição de apoio na capa",
        "temas": ["meio-ambiente", "transparencia"],  # precisa bater com os values do seu vocabulário
        "unidadeOrigem": "prrs",                      # value do vocabulário de unidades
        "text_html": "<p>Conteúdo da notícia...</p>",
        #"image_path": "/Users/lflrocha/Pictures/Pattern.jpg",     # ou None
        "image_path": None,     # ou None
    }

    created = create_news_item(noticia)
    print("Criado:", created.get("@id"))

    obj_url = created["@id"]

    patch = {
      "created": "2025-12-15T15:01:39-03:00",
      "modified": "2025-12-15T15:02:39-03:00",
    }

    r = requests.patch(obj_url, headers=HEADERS, auth=AUTH, data=json.dumps(patch))
    data = r.content

    print(data)
