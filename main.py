"""
=============================================================
  Bot Auditor ATC - NOVONET + VENSA
  Bitrix24 + Wazzup + Groq + PostgreSQL
=============================================================
"""

import os
import re
import json
import asyncio
import httpx
import psycopg2
from datetime import datetime
from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

app = FastAPI(title="Bot Auditor ATC")

# ── Configuración ─────────────────────────────────────────
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL   = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")

DB_HOST     = os.getenv("DB_HOST", "")
DB_PORT     = int(os.getenv("DB_PORT", 5432))
DB_NAME     = os.getenv("DB_NAME", "")
DB_USER     = os.getenv("DB_USER", "")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

# Empresas configuradas
EMPRESAS = {
    "novonet": {
        "nombre":       "NOVONET",
        "bitrix":       os.getenv("NOVONET_BITRIX", "https://novonet.bitrix24.es/rest/87387/vcca209sfcjflxp8"),
        "atc_stage":    os.getenv("NOVONET_ATC_STAGE", "C19:UC_U0JYD8"),
        "wazzup_key":   os.getenv("NOVONET_WAZZUP_KEY", "7b535e6f961d4cfd8282fffbbd36fa8c"),
    },
    "velsa": {
        "nombre":       "VELSA",
        "bitrix":       os.getenv("VELSA_BITRIX", "https://aclopecuador.bitrix24.es/rest/1/49hra49433psie0t"),
        "atc_stage":    os.getenv("VELSA_ATC_STAGE", ""),
        "wazzup_key":   os.getenv("VELSA_WAZZUP_KEY", "3340c8993cf940639f06cf894e2b8143"),
    },
}

# Polling automático de leads que pasan a ATC (sin webhook de Bitrix).
# Lee directamente las tablas ya sincronizadas en esta misma base (bddgeneral):
#   - VELSA: bitrix_deals + bitrix_etapas (stage_id -> nombre de etapa)
#   - NOVONET: mestra_bitrix (b_etapa_de_la_negociacion ya viene resuelta como texto)
ATC_POLL_INTERVAL_SECONDS = int(os.getenv("ATC_POLL_INTERVAL_SECONDS", 600))
ATC_POLL_ENABLED = os.getenv("ATC_POLL_ENABLED", "true").lower() in ("1", "true", "yes")
# ─────────────────────────────────────────────────────────


# ════════════════════════════════════════════════════════
#  BASE DE DATOS
# ════════════════════════════════════════════════════════
def guardar_auditoria(data: dict):
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO auditorias (
                id_bitrix, asesor, fecha_creacion_lead, fecha_hora_auditada,
                conversacion_anonimizada, puntuacion_venta, puntuacion_atc,
                calificacion, observacion, empresa, tipo_canal
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            data["id_bitrix"],
            data["asesor"],
            data["fecha_creacion_lead"],
            datetime.now(),
            data["conversacion_anonimizada"],
            data["puntuacion_venta"],
            data["puntuacion_atc"],
            data["calificacion"],
            data["observacion"],
            data["empresa"],
            data["tipo_canal"],
        ))
        conn.commit()
        log.info(f"Auditoria guardada: deal {data['id_bitrix']} [{data['empresa']}] [{data['tipo_canal']}]")
    finally:
        conn.close()


def _get_conn():
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)


def init_watermark_table():
    """Crea (si no existe) la tabla de control para detectar transiciones a ATC sin webhook."""
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS atc_watermark (
                empresa        VARCHAR(20)  NOT NULL,
                deal_id        VARCHAR(50)  NOT NULL,
                last_stage     TEXT,
                last_modified  TEXT,
                audited        BOOLEAN DEFAULT FALSE,
                detectado_en   TIMESTAMP DEFAULT now(),
                PRIMARY KEY (empresa, deal_id)
            )
        """)
        conn.commit()
        log.info("Tabla atc_watermark verificada/creada.")
    finally:
        conn.close()


def _fetch_velsa_en_atc():
    """Deals VELSA cuya etapa actual (resuelta vía bitrix_etapas) es 'ATC'."""
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT bd.id::text, be.nombre, bd.fecha_modificacion::text
            FROM bitrix_deals bd
            JOIN bitrix_etapas be
              ON be.status_id = bd.stage_id AND be.category_id = bd.category_id
            WHERE be.nombre = 'ATC'
        """)
        return cur.fetchall()
    finally:
        conn.close()


def _fetch_novonet_en_atc():
    """Deals NOVONET cuya etapa actual (ya resuelta como texto) es 'ATC'."""
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT b_id, 'ATC',
                   COALESCE(b_modificado_el_fecha, '') || ' ' || COALESCE(b_modificado_el_hora, '')
            FROM mestra_bitrix
            WHERE b_etapa_de_la_negociacion = 'ATC'
              AND b_id IS NOT NULL
        """)
        return cur.fetchall()
    finally:
        conn.close()


def _watermark_lookup(empresa: str, deal_id: str):
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT last_stage, last_modified FROM atc_watermark WHERE empresa=%s AND deal_id=%s",
            (empresa, deal_id)
        )
        return cur.fetchone()
    finally:
        conn.close()


def _watermark_upsert(empresa: str, deal_id: str, stage: str, modified: str):
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO atc_watermark (empresa, deal_id, last_stage, last_modified, audited, detectado_en)
            VALUES (%s, %s, %s, %s, TRUE, now())
            ON CONFLICT (empresa, deal_id) DO UPDATE SET
                last_stage = EXCLUDED.last_stage,
                last_modified = EXCLUDED.last_modified,
                audited = TRUE,
                detectado_en = now()
        """, (empresa, deal_id, stage, modified))
        conn.commit()
    finally:
        conn.close()


async def revisar_y_disparar_auditorias():
    """
    Poll periódico (sin webhook): revisa las tablas ya sincronizadas en bddgeneral
    cada ATC_POLL_INTERVAL_SECONDS y dispara una auditoria para cada deal que:
      - no tenía registro previo en atc_watermark (nuevo en ATC), o
      - su 'last_modified' cambió respecto al watermark (re-entró a ATC tras moverse).
    """
    try:
        velsa_rows    = await asyncio.to_thread(_fetch_velsa_en_atc)
        novonet_rows  = await asyncio.to_thread(_fetch_novonet_en_atc)
    except Exception as e:
        log.error(f"Error consultando tablas de ATC para polling: {e}")
        return

    candidatos = [("velsa", *r) for r in velsa_rows] + [("novonet", *r) for r in novonet_rows]
    nuevos = 0

    for empresa_key, deal_id, etapa, modificado in candidatos:
        deal_id = str(deal_id)
        try:
            watermark = await asyncio.to_thread(_watermark_lookup, empresa_key, deal_id)
        except Exception as e:
            log.error(f"Error leyendo watermark {empresa_key}/{deal_id}: {e}")
            continue

        es_nuevo = (
            watermark is None
            or watermark[0] != "ATC"
            or (watermark[1] or "") != (modificado or "")
        )
        if not es_nuevo:
            continue

        nuevos += 1
        log.info(f"ATC detectado por polling: {empresa_key}/{deal_id} (modificado={modificado})")
        try:
            await procesar_deal(deal_id, EMPRESAS[empresa_key])
        except Exception as e:
            log.error(f"Error auditando {empresa_key}/{deal_id} desde el poller: {e}")
            continue

        try:
            await asyncio.to_thread(_watermark_upsert, empresa_key, deal_id, "ATC", modificado)
        except Exception as e:
            log.error(f"Error actualizando watermark {empresa_key}/{deal_id}: {e}")

    log.info(f"Polling ATC: {len(candidatos)} en ATC actualmente, {nuevos} auditados en este ciclo.")


async def _poller_loop():
    await asyncio.to_thread(init_watermark_table)
    while True:
        try:
            await revisar_y_disparar_auditorias()
        except Exception as e:
            log.error(f"Error en ciclo de polling ATC: {e}")
        await asyncio.sleep(ATC_POLL_INTERVAL_SECONDS)


@app.on_event("startup")
async def _on_startup():
    if ATC_POLL_ENABLED:
        asyncio.create_task(_poller_loop())
        log.info(f"Polling ATC activado cada {ATC_POLL_INTERVAL_SECONDS}s.")
    else:
        log.info("Polling ATC deshabilitado (ATC_POLL_ENABLED=false).")


# ════════════════════════════════════════════════════════
#  WEBHOOKS
# ════════════════════════════════════════════════════════
@app.post("/webhook/novonet")
async def webhook_novonet(request: Request, background_tasks: BackgroundTasks):
    return await handle_webhook(request, background_tasks, "novonet")

@app.post("/webhook/vensa")
async def webhook_vensa(request: Request, background_tasks: BackgroundTasks):
    return await handle_webhook(request, background_tasks, "velsa")


async def handle_webhook(request: Request, background_tasks: BackgroundTasks, empresa_key: str):
    try:
        data = dict(await request.form())
    except Exception:
        data = await request.json()

    deal_id  = data.get("data[FIELDS][ID]") or data.get("document_id[2]")
    stage_id = data.get("data[FIELDS][STAGE_ID]")
    empresa  = EMPRESAS[empresa_key]

    if not deal_id:
        return JSONResponse({"status": "ignored", "reason": "no deal_id"})

    if stage_id and empresa["atc_stage"] and stage_id != empresa["atc_stage"]:
        return JSONResponse({"status": "ignored", "reason": f"stage {stage_id} != ATC"})

    background_tasks.add_task(procesar_deal, deal_id, empresa)
    return JSONResponse({"status": "ok", "deal_id": deal_id, "empresa": empresa["nombre"]})


# ════════════════════════════════════════════════════════
#  EXTRACCIÓN DE CONVERSACIÓN
# ════════════════════════════════════════════════════════
async def obtener_deal(bitrix: str, deal_id: str) -> dict:
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(f"{bitrix}/crm.deal.get?id={deal_id}")
        return resp.json().get("result", {})


async def obtener_chat(bitrix: str, deal_id: str) -> tuple[str | None, str]:
    """Retorna (chat_id, tipo_canal). tipo_canal = 'WHATSAPP' o 'WABA'"""
    url = f"{bitrix}/crm.activity.list"
    params = {"filter[OWNER_TYPE_ID]": "2", "filter[OWNER_ID]": deal_id, "select[]": "*"}
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(url, params=params)
        result = resp.json().get("result", [])

    for activity in result:
        if activity.get("PROVIDER_ID") == "IMOPENLINES_SESSION":
            chat_id    = activity.get("ASSOCIATED_ENTITY_ID")
            user_code  = activity.get("PROVIDER_PARAMS", {}).get("USER_CODE", "")
            # WABA tiene transport "wapi" en el user_code o viene sin canal whatsapp
            tipo_canal = "WABA" if "wapi" in user_code.lower() else "WHATSAPP"
            return chat_id, tipo_canal

    return None, "WHATSAPP"


async def obtener_mensajes(bitrix: str, chat_id: str) -> tuple[list, list]:
    url = f"{bitrix}/im.dialog.messages.get"
    params = {"DIALOG_ID": f"chat{chat_id}", "LIMIT": 200}
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(url, params=params)
        data = resp.json().get("result", {})

    usuarios_map = {u["id"]: u for u in data.get("users", [])}
    mensajes = []

    for m in reversed(data.get("messages", [])):
        author_id = m.get("author_id", 0)
        texto     = m.get("text", "").strip()
        if author_id == 0 or not texto or texto.startswith("=== SYSTEM WZ ==="):
            continue

        user_info  = usuarios_map.get(author_id, {})
        es_cliente = user_info.get("connector", False) or user_info.get("extranet", False)
        mensajes.append({
            "fecha":  m.get("date", "")[:10],
            "rol":    "CLIENTE" if es_cliente else "ASESOR",
            "nombre": user_info.get("name", str(author_id)),
            "texto":  texto
        })

    return mensajes, list(usuarios_map.values())


def formatear_conversacion(mensajes: list) -> str:
    return "\n".join(
        f"[{m['fecha']}] {m['rol']} ({m['nombre']}): {m['texto']}"
        for m in mensajes
    )


# ════════════════════════════════════════════════════════
#  ANONIMIZACIÓN
# ════════════════════════════════════════════════════════
def anonimizar(texto: str) -> str:
    texto = re.sub(r'\+?593\d{9}', '[TELEFONO]', texto)
    texto = re.sub(r'0\d{9}',       '[TELEFONO]', texto)
    texto = re.sub(r'\b\d{7,15}\b', '[TELEFONO]', texto)
    texto = re.sub(r'[\w\.-]+@[\w\.-]+\.\w+', '[EMAIL]', texto)
    texto = re.sub(r'\b\d{10}\b',   '[CEDULA]',  texto)
    texto = re.sub(r'https?://\S+',  '[URL]',     texto)
    texto = re.sub(r'-?\d+\.\d{4,}','[COORDENADA]', texto)
    texto = re.sub(r'\(([^)]+)\)', '(PARTICIPANTE)', texto)
    return texto

def anonimizar_conversacion(conv: str) -> str:
    return "\n".join(anonimizar(l) for l in conv.splitlines())


# ════════════════════════════════════════════════════════
#  ANÁLISIS CON GROQ
# ════════════════════════════════════════════════════════
PROMPT_SISTEMA = """
Eres un auditor de calidad para una empresa de internet.
Analiza conversaciones de WhatsApp del canal ATC (Atención al Cliente).

DEFINICIONES:
- ATC: cliente contactó por soporte, reclamo o consulta (NO quiere comprar)
- VENTA: cliente mostró intención de contratar, mejorar plan, o hay oportunidad de venta

SEÑALES DE VENTA:
- Preguntar por precios o planes
- Familiar/vecino quiere contratar
- Comparar con otra empresa
- Querer agregar servicios
- Mudanza y necesita internet
- Plan actual no alcanza

PUNTUACIÓN VENTA (0-100): qué tan clara fue la intención de compra que el asesor no vio
PUNTUACIÓN ATC (0-100): qué tan correcto fue clasificar esto como servicio al cliente (100=bien clasificado)
CALIFICACIÓN: "ATC" si puntuacion_atc >= 60, "VENTA" si puntuacion_venta > puntuacion_atc

RESPONDE SOLO en este JSON:
{
  "puntuacion_venta": <0-100>,
  "puntuacion_atc": <0-100>,
  "calificacion": "<ATC|VENTA>",
  "observacion": "<evaluación en 3-4 oraciones>"
}
"""

async def analizar_con_groq(conversacion: str) -> dict:
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "model": GROQ_MODEL,
        "messages": [
            {"role": "system", "content": PROMPT_SISTEMA},
            {"role": "user",   "content": f"--- CONVERSACIÓN ---\n{conversacion}\n--- FIN ---"}
        ],
        "temperature": 0.2
    }
    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post("https://api.groq.com/openai/v1/chat/completions",
                                 headers=headers, json=payload)
        if resp.status_code != 200:
            log.error(f"Groq error {resp.status_code}: {resp.text}")
            resp.raise_for_status()
        raw = resp.json()["choices"][0]["message"]["content"]

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        # Intentar extraer JSON del texto
        match = re.search(r'\{.*\}', raw, re.DOTALL)
        if match:
            return json.loads(match.group())
        return {"puntuacion_venta": 0, "puntuacion_atc": 50, "calificacion": "ATC",
                "observacion": "No se pudo parsear la respuesta de Groq."}


# ════════════════════════════════════════════════════════
#  ORQUESTADOR
# ════════════════════════════════════════════════════════
async def procesar_deal(deal_id: str, empresa: dict):
    log.info(f"Procesando deal {deal_id} [{empresa['nombre']}]...")

    deal   = await obtener_deal(empresa["bitrix"], deal_id)
    asesor = deal.get("ASSIGNED_BY_ID", "")
    try:
        fecha_creacion = datetime.fromisoformat(deal.get("DATE_CREATE", ""))
    except Exception:
        fecha_creacion = None

    chat_id, tipo_canal = await obtener_chat(empresa["bitrix"], deal_id)

    if not chat_id:
        log.warning(f"Deal {deal_id} sin chat. Guardando sin conversación.")
        guardar_auditoria({
            "id_bitrix": deal_id, "asesor": asesor,
            "fecha_creacion_lead": fecha_creacion,
            "conversacion_anonimizada": "Sin conversación disponible",
            "puntuacion_venta": 0, "puntuacion_atc": 0,
            "calificacion": "ATC",
            "observacion": "No se encontró conversación de WhatsApp para este deal.",
            "empresa": empresa["nombre"], "tipo_canal": tipo_canal or "DESCONOCIDO"
        })
        return

    mensajes, usuarios = await obtener_mensajes(empresa["bitrix"], chat_id)

    if not mensajes:
        log.warning(f"Chat {chat_id} sin mensajes accesibles.")
        obs = "Canal WABA (Meta API): historial no accesible." if tipo_canal == "WABA" else "No se encontraron mensajes en el chat. Posible problema de permisos del webhook."
        guardar_auditoria({
            "id_bitrix": deal_id, "asesor": asesor,
            "fecha_creacion_lead": fecha_creacion,
            "conversacion_anonimizada": "Sin mensajes disponibles",
            "puntuacion_venta": 0, "puntuacion_atc": 0,
            "calificacion": "ATC",
            "observacion": obs,
            "empresa": empresa["nombre"], "tipo_canal": tipo_canal
        })
        return

    nombre_asesor = next(
        (u.get("name", asesor) for u in usuarios
         if str(u.get("id")) == str(asesor) and not u.get("connector")),
        asesor
    )

    conversacion         = formatear_conversacion(mensajes)
    conversacion_anonima = anonimizar_conversacion(conversacion)
    analisis             = await analizar_con_groq(conversacion_anonima)

    guardar_auditoria({
        "id_bitrix":               deal_id,
        "asesor":                  nombre_asesor,
        "fecha_creacion_lead":     fecha_creacion,
        "conversacion_anonimizada": conversacion_anonima,
        "puntuacion_venta":        analisis.get("puntuacion_venta", 0),
        "puntuacion_atc":          analisis.get("puntuacion_atc", 0),
        "calificacion":            analisis.get("calificacion", "ATC"),
        "observacion":             analisis.get("observacion", ""),
        "empresa":                 empresa["nombre"],
        "tipo_canal":              tipo_canal,
    })
    log.info(f"Deal {deal_id} completado. Score venta: {analisis.get('puntuacion_venta')} ATC: {analisis.get('puntuacion_atc')}")


# ════════════════════════════════════════════════════════
#  ENDPOINTS
# ════════════════════════════════════════════════════════
@app.get("/test/{empresa_key}/{deal_id}")
async def test_deal(empresa_key: str, deal_id: str, background_tasks: BackgroundTasks):
    if empresa_key not in EMPRESAS:
        return JSONResponse({"error": f"Empresa '{empresa_key}' no encontrada. Usa: novonet, velsa"}, status_code=404)
    background_tasks.add_task(procesar_deal, deal_id, EMPRESAS[empresa_key])
    return {"status": "procesando", "deal_id": deal_id, "empresa": EMPRESAS[empresa_key]["nombre"]}

@app.get("/health")
async def health():
    return {"status": "ok", "modelo": GROQ_MODEL, "empresas": list(EMPRESAS.keys())}

@app.post("/poll/run")
async def poll_run_now(background_tasks: BackgroundTasks):
    """Dispara manualmente un ciclo de polling ATC (sin esperar el intervalo)."""
    background_tasks.add_task(revisar_y_disparar_auditorias)
    return {"status": "ciclo de polling lanzado"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
