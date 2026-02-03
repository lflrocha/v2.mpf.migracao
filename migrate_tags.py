#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import csv
import requests
from requests.auth import HTTPBasicAuth

# =========================
# CONFIG
# =========================

ORIG_BASE = os.getenv("PLONE_ORIG_BASE", "http://svlp-plnptapp01.pgr.mpf.mp.br:8401/portal/")
DEST_BASE = os.getenv("PLONE_DEST_BASE", "https://www-cdn.mpf.mp.br/comunicacao/noticias")

DEST_USER = os.getenv("PLONE_DEST_USER", "admin")
DEST_PASS = os.getenv("PLONE_DEST_PASS", "zope")

TIMEOUT = 60
VERIFY_SSL = False   # se precisar ignorar certificado

HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
}

# CSV no formato:
# url_origem;url_destino
CSV_FILE = sys.argv[1] if len(sys.argv) > 1 else "conteudos.csv"


# =========================
# FUNÇÕES
# =========================

def get_tags_from_origem(url):
    """
    Busca as tags (Subject) via chamada Zope simples.
    Espera que a view retorne texto ou JSON simples.
    """
    r = requests.get(
        f"{url.rstrip('/')}/getSubject",
        timeout=TIMEOUT,
        verify=VERIFY_SSL,
    )

    if r.status_code != 200:
        print(f"  ERRO lendo tags da origem: {r.status_code}")
        return []

    text = r.text.strip()

    # Tentativa simples de parse
    if not text:
        return []

    # Exemplo comum: "('tag1', 'tag2')"
    text = text.strip("()")
    tags = [t.strip(" '\"") for t in text.split(",") if t.strip()]
    return tags


def update_tags_destino(url, tags):
    payload = {
        "subject": tags
    }

    r = requests.patch(
        url,
        json=payload,
        headers=HEADERS,
        auth=HTTPBasicAuth(DEST_USER, DEST_PASS),
        timeout=TIMEOUT,
        verify=VERIFY_SSL,
    )

    if r.status_code not in (200, 204):
        raise Exception(
            f"Erro atualizando tags: {r.status_code}\n{r.text}"
        )


# =========================
# MAIN
# =========================

def main():
    print("🚚 Migração de TAGS (Subject)")
    print("-" * 50)

    with open(CSV_FILE, newline="", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter=";")

        for row in reader:
            if not row or len(row) < 2:
                continue

            url_origem, url_destino = ro
