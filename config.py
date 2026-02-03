# ====== DESTINO ======
PLONE_URL = "https://www-cdn.mpf.mp.br/"
CONTAINER_PATH = "/comunicacao/noticias"
API_BASE = f"{PLONE_URL.rstrip('/')}/{CONTAINER_PATH.lstrip('/')}"

USERNAME = "admin"
PASSWORD = "zope"
AUTH = (USERNAME, PASSWORD)

HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
}

# ====== ORIGEM ======
LISTA_URL = "http://svlp-plnptapp01.pgr.mpf.mp.br:8401/portal/portal_skins/custom/v2_getNoticiasLista"


# ====== CONSTANTES ======
TEMA_PARA_ID = {
    "Combate à Corrupção": "combate-a-corrupcao",
    "Comunidades Tradicionais": "comunidades-tradicionais",
    "Concursos": "concursos",
    "Constitucional": "constitucional",
    "Consumidor e Ordem Econômica": "consumidor-e-ordem-economica",
    "Controle Externo da Atividade Policial": "controle-externo-da-atividade-policial",
    "Cooperação Internacional": "cooperacao-internacional",
    "Criminal": "criminal",
    "Direitos do Cidadão": "direitos-do-cidadao",
    "Eleitoral": "eleitoral",
    "Fiscalização de Atos Administrativos": "fiscalizacao-de-atos-administrativos",
    "Geral": "geral",
    "Improbidade Administrativa": "improbidade-administrativa",
    "Indígenas": "indigenas",
    "Meio Ambiente": "meio-ambiente",
    "Patrimônio Público": "patrimonio-publico",
    "Patrimônio Cultural": "patrimonio-cultural",
    "Sistema Prisional": "sistema-prisional",
    "Transparência": "transparencia",
}

FILE_EXTS = {
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
    ".zip", ".rar", ".7z",
    ".odt", ".ods", ".odp",
    ".csv", ".txt",
}

INTERNAL_HOSTS = {
    "svlp-plnptapp01.pgr.mpf.mp.br:8401",
    "svlp-plnptapp01.pgr.mpf.mp.br",
    # se tiver outros, adicione aqui
}
