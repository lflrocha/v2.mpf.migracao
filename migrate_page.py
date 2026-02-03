#!/Users/lflrocha/Sistemas/v2.mpf.migracao/bin/python3
# migrate_page.py
from __future__ import annotations

import sys
import requests

from config import AUTH, HEADERS
from utils import clean_html
from assets import migrate_embedded_assets, patch_news_text

METADADOS_ENDPOINT = "v2_getDocumentosMetadados"
CORPO_ENDPOINT = "v2_getDocumentosCorpo"


def join_url(base: str, suffix: str) -> str:
    return base.rstrip("/") + "/" + suffix.lstrip("/")


def split_parent_and_id(url: str):
    u = url.rstrip("/")
    parent = u.rsplit("/", 1)[0]
    _id = u.rsplit("/", 1)[1]
    return parent, _id


def is_real_value(v: str) -> bool:
    if v is None:
        return False
    s = str(v).strip()
    return bool(s) and s.lower() not in ("none", "null", "undefined")


def parse_kv_lines(text: str) -> dict:
    data = {}
    for raw in (text or "").splitlines():
        line = raw.strip()
        if not line or "=" not in line:
            continue
        k, v = line.split("=", 1)
        data[k.strip()] = v.strip()
    return data


def fetch_metadados(session: requests.Session, old_page_url: str) -> dict:
    url = join_url(old_page_url, METADADOS_ENDPOINT)
    r = session.get(url)
    r.raise_for_status()

    meta = parse_kv_lines(r.text)
    subjects_raw = meta.get("subjects", "") or ""
    meta["subjects_list"] = [s.strip() for s in subjects_raw.split("#;#") if s.strip()]
    return meta


def fetch_corpo(session: requests.Session, old_page_url: str) -> str:
    url = join_url(old_page_url, CORPO_ENDPOINT)
    r = session.get(url)
    r.raise_for_status()
    return r.text or ""


def exists_real_object(session: requests.Session, url: str) -> bool:
    """
    Evita falso-positivo por aquisição:
    só considera existente se /@json retornar 200.
    """
    r = session.get(url.rstrip("/") + "/@json", headers=HEADERS, auth=AUTH)
    if r.status_code == 200:
        return True
    if r.status_code == 404:
        return False
    raise Exception(f"Erro checando existência via @json: {r.status_code} {r.reason}\n{r.text}")



def ensure_container_exists(session: requests.Session, parent_url: str):
    """
    Valida que o parent responde (evita depender de /@json, que pode ser bloqueado por proxy/VHM).
    NÃO garante que é folderish; isso o POST vai dizer.
    """
    r = session.get(parent_url.rstrip("/"), headers=HEADERS, auth=AUTH)
    if r.status_code == 200:
        return
    if r.status_code == 404:
        raise Exception(
            "Parent do destino não existe (GET 404).\n"
            f"Parent: {parent_url}"
        )
    raise Exception(f"Erro checando parent (GET): {r.status_code} {r.reason}\n{r.text}")



def ensure_container_exists_old(session: requests.Session, parent_url: str):
    """
    Garante que o parent do POST existe e é acessível.
    NÃO cria nada automaticamente.
    """
    r = session.get(parent_url.rstrip("/") + "/@json", headers=HEADERS, auth=AUTH)
    if r.status_code == 200:
        return
    if r.status_code == 404:
        raise Exception(
            "Parent do destino não existe (ou não está acessível pelo REST).\n"
            f"Parent: {parent_url}"
        )
    raise Exception(f"Erro checando parent: {r.status_code} {r.reason}\n{r.text}")


def ensure_folder(session: requests.Session, folder_url: str, title: str = "", description: str = "") -> str:
    """
    Garante a existência de uma pasta exatamente em folder_url.
    Cria como Folder com id = último segmento.
    NÃO cria intermediárias.
    Retorna SEMPRE folder_url (string).
    """
    folder_url = folder_url.rstrip("/")
    parent_url, folder_id = split_parent_and_id(folder_url)

    # Existe de verdade? (preferimos @json, mas pode estar bloqueado; fallback no GET normal)
    try:
        if exists_real_object(session, folder_url):
            return folder_url
    except Exception:
        # fallback: se @json estiver bloqueado, tenta GET normal
        gr = session.get(folder_url, headers=HEADERS, auth=AUTH)
        if gr.status_code == 200:
            return folder_url
        if gr.status_code not in (404,):
            raise Exception(f"Erro checando folder (GET): {gr.status_code} {gr.reason}\n{gr.text}")

    # Parent precisa existir (GET normal, não @json)
    ensure_container_exists(session, parent_url)

    payload = {
        "@type": "Folder",
        "id": folder_id,
        "title": title or folder_id,
        "description": description or "",
    }

    rc = session.post(parent_url, headers=HEADERS, auth=AUTH, json=payload)

    if rc.status_code in (401, 403):
        raise Exception(
            "Sem permissão para criar a pasta no parent.\n"
            f"Parent: {parent_url}\n"
            f"Status: {rc.status_code}\n{rc.text}"
        )

    # Se já existe, o Plone pode retornar 409 Conflict
    if rc.status_code == 409:
        return folder_url

    if rc.status_code == 404:
        raise Exception(
            "Falha criando a pasta: parent não aceito como container (404 no POST).\n"
            f"Parent: {parent_url}\n{rc.text}"
        )

    if rc.status_code not in (200, 201):
        raise Exception(f"Erro criando Folder: {rc.status_code} {rc.reason}\n{rc.text}")

    # Não confie em @id no JSON; valide pelo GET
    vr = session.get(folder_url, headers=HEADERS, auth=AUTH)
    if vr.status_code != 200:
        raise Exception(
            "POST retornou sucesso, mas a pasta não aparece no GET de validação.\n"
            f"Folder esperada: {folder_url}\n"
            f"GET validate: {vr.status_code} {vr.reason}\n{vr.text}"
        )

    return folder_url


def ensure_or_update_document(
    session: requests.Session,
    page_url: str,
    title: str,
    description: str,
    text_html: str,
    subjects: list,
    created: str,
    effective: str,
    expires: str,
) -> str:
    """
    Cria/atualiza Document em page_url.
    O parent deve existir.
    """
    page_url = page_url.rstrip("/")
    parent_url, doc_id = split_parent_and_id(page_url)

    payload = {
        "title": title,
        "description": description,
        "subjects": subjects,
        "text": {
            "data": text_html,
            "content-type": "text/html",
            "encoding": "utf-8",
        },
    }

    if is_real_value(created):
        payload["created"] = created
    if is_real_value(effective):
        payload["effective"] = effective
    if is_real_value(expires):
        payload["expires"] = expires

    # existe de verdade?
    if exists_real_object(session, page_url):
        rp = session.patch(page_url, headers=HEADERS, auth=AUTH, json=payload)
        if rp.status_code not in (200, 204):
            raise Exception(f"Erro PATCH destino: {rp.status_code} {rp.reason}\n{rp.text}")
        return page_url

    # parent precisa existir
    ensure_container_exists(session, parent_url)

    create_payload = {"@type": "Document", "id": doc_id, **payload}
    rc = session.post(parent_url, headers=HEADERS, auth=AUTH, json=create_payload)
    if rc.status_code not in (200, 201):
        raise Exception(f"Erro criando Document: {rc.status_code} {rc.reason}\n{rc.text}")

    # valida pelo GET na URL esperada
    vr = session.get(page_url, headers=HEADERS, auth=AUTH)
    if vr.status_code != 200:
        raise Exception(
            "POST retornou sucesso, mas a página não aparece no GET de validação.\n"
            f"Página esperada: {page_url}\n"
            f"GET validate: {vr.status_code} {vr.reason}\n{vr.text}"
        )
    return page_url



def publish_item(session: requests.Session, item_url: str):
    r = session.post(f"{item_url.rstrip('/')}/@workflow/publish", headers=HEADERS, auth=AUTH)
    if r.status_code not in (200, 204):
        raise Exception(f"Erro publicando: {r.status_code} {r.reason}\n{r.text}")


def main():
    if len(sys.argv) < 3:
        print("Uso:")
        print("  python migrate_page.py <old_page_url> <dest_section_url> [--publish]")
        print("")
        print("Ex.:")
        print("  python migrate_page.py http://origem/... http://destino/.../atuacao --publish")
        sys.exit(2)

    old_page_url = sys.argv[1].strip().rstrip("/")
    dest_section_url = sys.argv[2].strip().rstrip("/")  # <- agora é a URL da SEÇÃO
    do_publish = ("--publish" in sys.argv)

    with requests.Session() as s:
        meta = fetch_metadados(s, old_page_url)
        raw_html = fetch_corpo(s, old_page_url)

        title = (meta.get("titulo") or meta.get("title") or "").strip()
        description = (meta.get("descricao") or meta.get("description") or "").strip()
        created = (meta.get("creationDate") or "").strip()
        effective = (meta.get("effectiveDate") or "").strip()
        expires = (meta.get("expirationDate") or "").strip()
        subjects = meta.get("subjects_list", [])

        text_html = clean_html(raw_html)

        # 1) cria/garante a pasta da seção (id = último segmento da dest_section_url)
        folder_url = ensure_folder(
            s,
            folder_url=dest_section_url,
            title=title,          # você pode trocar se quiser outro título
            description=description,
        )

        # 2) cria/atualiza a página dentro, com o MESMO ID do folder
        _, last_id = split_parent_and_id(folder_url)
        page_url = folder_url.rstrip("/") + "/" + last_id

        new_page_url = ensure_or_update_document(
            s,
            page_url=page_url,
            title=title,
            description=description,
            text_html=text_html,
            subjects=subjects,
            created=created,
            effective=effective,
            expires=expires,
        )

        # 3) assets ficam no MESMO NÍVEL da página -> dentro da pasta (folder_url)
        try:
            html2 = migrate_embedded_assets(
                s,
                old_base_url=old_page_url,
                new_news_url=folder_url,  # <- irmãos da página dentro da pasta
                html=text_html,
            )
            if html2 != text_html:
                patch_news_text(s, new_page_url, html2)
        except Exception as e:
            print("AVISO: falha migrando assets embutidos:", e)

        if do_publish:
            # publica página (e opcionalmente a pasta, se fizer sentido no seu workflow)
            publish_item(s, new_page_url)

        print("OK")
        print("Origem :", old_page_url)
        print("Destino (pasta):", folder_url)
        print("Destino (página):", new_page_url)


if __name__ == "__main__":
    main()
