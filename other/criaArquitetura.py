import re
import unicodedata
import requests

# ====== CONFIG (AJUSTADO) ======
CONTAINER_URL = "http://svlh-plnptall01.pgr.mpf.mp.br:8401/mpf2026/o-mpf/unidades"

USERNAME = "lflrocha"   # ajuste
PASSWORD = "bl@ckb!rd"   # ajuste

TITLES = [
"Procuradoria-Geral da República (PGR)",
"Procuradoria Regional da República da 1ª Região (PRR1)",
"Procuradoria Regional da República da 2ª Região (PRR2)",
"Procuradoria Regional da República da 3ª Região (PRR3)",
"Procuradoria Regional da República da 4ª Região (PRR4)",
"Procuradoria Regional da República da 5ª Região (PRR5)",
"Procuradoria Regional da República da 6ª Região (PRR6)",
"Procuradoria da República em Alagoas",
"Procuradoria da República em Goiás",
"Procuradoria da República em Mato Grosso",
"Procuradoria da República em Mato Grosso do Sul",
"Procuradoria da República em Minas Gerais",
"Procuradoria da República em Pernambuco",
"Procuradoria da República em Rondônia",
"Procuradoria da República em Roraima",
"Procuradoria da República em Santa Catarina",
"Procuradoria da República em São Paulo",
"Procuradoria da República em Sergipe",
"Procuradoria da República na Bahia",
"Procuradoria da República na Paraíba",
"Procuradoria da República no Acre",
"Procuradoria da República no Amapá",
"Procuradoria da República no Amazonas",
"Procuradoria da República no Ceará",
"Procuradoria da República no Distrito Federal",
"Procuradoria da República no Espírito Santo",
"Procuradoria da República no Maranhão",
"Procuradoria da República no Pará",
"Procuradoria da República no Paraná",
"Procuradoria da República no Piauí",
"Procuradoria da República no Rio de Janeiro",
"Procuradoria da República no Rio Grande do Norte",
"Procuradoria da República no Rio Grande do Sul",
"Procuradoria da República no Tocantins",
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
        "@type": "unidade",
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
