import base64
import csv

import requests

from datetime import datetime
import time

from config import LISTA_URL, TEMA_PARA_ID
from utils import clean_html, parse_kv_lines, year_from_effective, _stamp_hms, _fmt
from importer import create_news_item, ensure_year_folder, unidade_from_caminho, publish_item
from assets import migrate_embedded_assets, patch_news_text


import os
import json

STATE_FILE = "import_state.json"

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_state(state):
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, STATE_FILE)


def main():
    state = load_state()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"logs/mapeamento_noticias_{timestamp}.csv"

    with requests.Session() as s:
        lista = s.get(LISTA_URL)
        lista.raise_for_status()
        urls = [u.strip() for u in lista.text.splitlines() if u.strip()]
        total = len(urls)

        mapeamentos = []

        for i, old_url in enumerate(urls, start=1):
            prefix = f"({i}/{total})"
            print(f"{prefix} {datetime.now().strftime('%H:%M:%S')} - {old_url}")

            if old_url in state:
                new_url = state[old_url]
                print(f"{prefix} JÁ IMPORTADO: {new_url}")
                print(f"{prefix} Concluído: 0.00s - {new_url}")
                print("==================================")
                continue

            t0 = time.perf_counter()
            errors = []
            new_url = ""
            effective = ""
            year = ""

            try:
                meta = parse_kv_lines(s.get(f"{old_url}/v2_getNoticiasMetadados").text)

                caminho = meta.get("caminho", "")
                unidade = unidade_from_caminho(caminho)

                effective = meta.get("effectiveDate")  # ex: 2026-01-19T09:10:00-03:00
                year = year_from_effective(effective)
                container_year = ensure_year_folder(s, year)

                classificacao = meta.get("classificacaoNoticia", "")
                temas_txt = [t.strip() for t in classificacao.split("#;#") if t.strip()]
                temas = [TEMA_PARA_ID[t] for t in temas_txt if t in TEMA_PARA_ID]
                temas = temas[0]

                subjects_raw = meta.get("subjects", "")
                subjects = [x.strip() for x in subjects_raw.split("#;#") if x.strip()]

                img_lines = [
                    l.strip()
                    for l in s.get(f"{old_url}/v2_getNoticiasImagem").text.splitlines()
                    if l.strip()
                ]
                img_url = img_lines[0] if len(img_lines) > 0 else ""
                filename = img_lines[1] if len(img_lines) > 1 else ""
                caption = (
                    img_lines[2].split(" = ", 1)[1].strip()
                    if len(img_lines) > 2 and " = " in img_lines[2]
                    else ""
                )

                image_b64 = ""
                if img_url:
                    try:
                        img_resp = s.get(img_url)
                        img_resp.raise_for_status()
                        image_b64 = base64.b64encode(img_resp.content).decode("utf-8")
                    except Exception as e:
                        errors.append(f"Falha imagem principal: {e}")

                corpo_resp = s.get(f"{old_url}/v2_getNoticiasCorpo")
                corpo_resp.raise_for_status()
                text_html = clean_html(corpo_resp.text)

                noticia = {
                    "title": meta.get("titulo", ""),
                    "description": meta.get("descricao", ""),
                    "tituloAlternativo": meta.get("tituloAlternativo", ""),
                    "descricaoAlternativa": meta.get("descricaoAlternativa", ""),
                    "temas": temas,
                    "unidadeOrigem": unidade,
                    "subjects": subjects,
                    "effective": effective,
                    "text_html": text_html,
                    "image_b64": image_b64,
                    "image_filename": filename,
                    "image_caption": caption,
                }

                created = create_news_item(s, container_year, noticia)
                new_url = created.get("@id", "")
                publish_item(s, new_url)

                try:
                    html2 = migrate_embedded_assets(
                        s,
                        old_base_url=old_url,
                        new_news_url=new_url,
                        html=noticia["text_html"],
                    )
                    if html2 != noticia["text_html"]:
                        patch_news_text(s, new_url, html2)
                except Exception as e:
                    errors.append(f"Falha assets internos: {e}")

                mapeamentos.append({
                    "old_url": old_url,
                    "new_url": new_url,
                    "effective": effective,
                    "year": year,
                })

            except Exception as e:
                errors.append(str(e))
                mapeamentos.append({
                    "old_url": old_url,
                    "new_url": new_url,
                    "effective": effective,
                    "year": year,
                })

            dur = time.perf_counter() - t0

            if errors:
                print(f"{prefix} ERROS: " + " | ".join(errors))

            state[old_url] = new_url
            save_state(state)
            print(f"{prefix} Concluído: {dur:.2f}s - {new_url}")
            print("==================================")

    with open(output_filename, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["old_url", "new_url", "effective", "year"])
        w.writeheader()
        w.writerows(mapeamentos)

    print(f"Arquivo gerado: {output_filename}")

if __name__ == "__main__":
    main()
