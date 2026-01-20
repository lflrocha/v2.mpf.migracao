import re
import unicodedata
import requests

# ====== CONFIG (AJUSTADO) ======
CONTAINER_URL = "http://svlh-plnptall01.pgr.mpf.mp.br:8401/mpf2026/comunicacao/grandes-casos"

USERNAME = "lflrocha"   # ajuste
PASSWORD = "bl@ckb!rd"   # ajuste

TITLES = [
"Caso Fundef",
"Caso Samarco",
"Atos Antidemocráticos",
"Caso Pinheiro/Braskem",
"Caso Potássio",
"ACP do Carvão",
"Preservação da Baía Babitonga",

]
# ===============================


def slugify(text: str) -> str:
    text = text.strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    return text or "pasta"


def create_folder(session: requests.Session, container_url: str, title: str) -> None:
    folder_id = slugify(title)

    payload = {
        "@type": "Folder",
        "id": folder_id,
        "title": title,
    }

    r = session.post(container_url.rstrip("/"), json=payload)

    if r.status_code in (200, 201):
        print(f"OK  : {title} -> {folder_id}")
        return

    if r.status_code == 409:
        print(f"SKIP: já existe {title} -> {folder_id}")
        return

    print(f"ERRO: {title} -> {folder_id} | HTTP {r.status_code}")
    print(r.text)
    r.raise_for_status()


def main():
    s = requests.Session()
    s.headers.update({
        "Accept": "application/json",
        "Content-Type": "application/json",
    })

    # Basic Auth (o mais comum em ambiente interno)
    s.auth = (USERNAME, PASSWORD)

    for t in TITLES:
        create_folder(s, CONTAINER_URL, t)


if __name__ == "__main__":
    main()
