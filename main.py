"""
=============================================================
  NETLIFE - Bot Auditor ATC
  Bitrix24 + Wazzup + Groq + PostgreSQL
=============================================================
"""

import os
import re
import json
import httpx
import asyncpg
from datetime import datetime
from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

app = FastAPI(title="Netlife Bot Auditor ATC")

# ── Configuración ─────────────────────────────────────────
BITRIX_WEBHOOK = os.getenv("BITRIX_WEBHOOK", "")
ATC_STAGE_ID   = os.getenv("ATC_STAGE_ID", "C19:UC_U0JYD8")
GROQ_API_KEY   = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL     = os.getenv("GROQ_MODEL", "llama3-8b-8192")

DB_HOST        = os.getenv("DB_HOST", "")
DB_PORT        = int(os.getenv("DB_PORT", 5432))
DB_NAME        = os.getenv("DB_NAME", "")
DB_USER        = os.getenv("DB_USER", "")
DB_PASSWORD    = os.getenv("DB_PASSWORD", "")
# ─────────────────────────────────────────────────────────


# ════════════════════════════════════════════════════════
#  BASE DE DATOS
# ════════════════════════════════════════════════════════
async def get_db():
    return await asyncpg.connect(
        host=DB_HOST, port=DB_PORT,
        database=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )

async def guardar_auditoria(data: dict):
    conn = await get_db()
    try:
        await conn.execute("""
            INSERT INTO auditorias (
                id_bitrix, asesor, fecha_creacion_lead, fecha_hora_auditada,
                conversacion_anonimizada, puntuacion_venta, puntuacion_atc,
                calificacion, observacion
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        """,
            data["id_bitrix"],
            data["asesor"],
            data["fecha_creacion_lead"],
            datetime.now(),
            data["conversacion_anonimizada"],
            data["puntuacion_venta"],
            data["puntuacion_atc"],
            data["calificacion"],
            data["observacion"]
        )
        log.info(f"Auditoria guardada para deal {data['id_bitrix']}")
    finally:
        await conn.close()


# ════════════════════════════════════════════════════════
#  WEBHOOK BITRIX24
# ════════════════════════════════════════════════════════
@app.post("/webhook/bitrix")
async def receive_webhook(request: Request, background_tasks: BackgroundTasks):
    try:
        data = dict(await request.form())
    except Exception:
        data = await request.json()

    deal_id  = data.get("data[FIELDS][ID]") or data.get("document_id[2]")
    stage_id = data.get("data[FIELDS][STAGE_ID]")

    if not deal_id:
        return JSONResponse({"status": "ignored", "reason": "no deal_id"})

    if stage_id and stage_id != ATC_STAGE_ID:
        return JSONResponse({"status": "ignored", "reason": f"stage {stage_id} != ATC"})

    background_tasks.add_task(procesar_deal, deal_id)
    return JSONResponse({"status": "ok", "deal_id": deal_id})


# ════════════════════════════════════════════════════════
#  EXTRACCIÓN DE CONVERSACIÓN
# ════════════════════════════════════════════════════════
async def obtener_deal(deal_id: str) -> dict:
    url = f"{BITRIX_WEBHOOK}/crm.deal.get?id={deal_id}"
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(url)
        return resp.json().get("result", {})


async def obtener_chat_id(deal_id: str) -> str | None:
    url = f"{BITRIX_WEBHOOK}/crm.activity.list"
    params = {"filter[OWNER_TYPE_ID]": "2", "filter[OWNER_ID]": deal_id, "select[]": "*"}
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(url, params=params)
        result = resp.json().get("result", [])

    for activity in result:
        if activity.get("PROVIDER_ID") == "IMOPENLINES_SESSION":
            return activity.get("ASSOCIATED_ENTITY_ID")
    return None


async def obtener_mensajes(chat_id: str) -> tuple[list, list]:
    url = f"{BITRIX_WEBHOOK}/im.dialog.messages.get"
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
    texto = re.sub(r'0\d{9}',      '[TELEFONO]', texto)
    texto = re.sub(r'\b\d{7,15}\b','[TELEFONO]', texto)
    texto = re.sub(r'[\w\.-]+@[\w\.-]+\.\w+', '[EMAIL]', texto)
    texto = re.sub(r'\b\d{10}\b',  '[CEDULA]',  texto)
    texto = re.sub(r'https?://\S+', '[URL]',     texto)
    texto = re.sub(r'-?\d+\.\d{4,}','[COORDENADA]', texto)
    texto = re.sub(r'\(([^)]+)\)', '(PARTICIPANTE)', texto)
    return texto

def anonimizar_conversacion(conversacion: str) -> str:
    return "\n".join(anonimizar(l) for l in conversacion.splitlines())


# ════════════════════════════════════════════════════════
#  ANÁLISIS CON GROQ
# ════════════════════════════════════════════════════════
PROMPT_SISTEMA = """
Eres un auditor de calidad para NETLIFE Ecuador, empresa de internet.
Analiza conversaciones de WhatsApp del canal ATC (Atención al Cliente).

DEFINICIONES:
- ATC: el cliente contactó por soporte, reclamo o consulta de servicio (NO quiere comprar)
- VENTA: el cliente mostró intención de contratar, mejorar plan, o hay oportunidad de venta

SEÑALES DE VENTA:
- Preguntar por precios o planes
- Familiar/vecino quiere contratar
- Comparar con otra empresa
- Querer agregar servicios
- Mudanza y necesita internet
- Plan actual no alcanza

PUNTUACIÓN VENTA (0-100):
- 0  = No hay ninguna oportunidad de venta
- 100 = El cliente quería comprar claramente y el asesor no lo atendió

PUNTUACIÓN ATC (0-100):
- 0  = No era ATC, era claramente una venta mal clasificada
- 100 = Correctamente clasificado, era 100% servicio al cliente

CALIFICACIÓN:
- "ATC"   si puntuacion_atc >= 60
- "VENTA" si puntuacion_venta > puntuacion_atc

RESPONDE SOLO en este JSON:
{
  "puntuacion_venta": <0-100>,
  "puntuacion_atc": <0-100>,
  "calificacion": "<ATC|VENTA>",
  "observacion": "<evaluación en 3-4 oraciones: qué pasó, si estaba bien clasificado, qué debería hacer el equipo>"
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
        "temperature": 0.2,
    }
    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post("https://api.groq.com/openai/v1/chat/completions",
                                 headers=headers, json=payload)
        resp.raise_for_status()
        raw = resp.json()["choices"][0]["message"]["content"]

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"error": "Respuesta no válida de Groq", "raw": raw}


# ════════════════════════════════════════════════════════
#  ORQUESTADOR
# ════════════════════════════════════════════════════════
async def procesar_deal(deal_id: str):
    log.info(f"Procesando deal {deal_id}...")

    # Datos del deal
    deal     = await obtener_deal(deal_id)
    asesor   = deal.get("ASSIGNED_BY_ID", "")
    fecha_creacion = deal.get("DATE_CREATE", "")
    try:
        fecha_creacion = datetime.fromisoformat(fecha_creacion)
    except Exception:
        fecha_creacion = None

    # Conversación
    chat_id = await obtener_chat_id(deal_id)
    if not chat_id:
        log.warning(f"Deal {deal_id} sin conversación Wazzup.")
        return

    mensajes, usuarios = await obtener_mensajes(chat_id)
    if not mensajes:
        log.warning(f"Chat {chat_id} sin mensajes.")
        return

    # Nombre del asesor
    nombre_asesor = next(
        (u.get("name", asesor) for u in usuarios if str(u.get("id")) == str(asesor) and not u.get("connector")),
        asesor
    )

    conversacion         = formatear_conversacion(mensajes)
    conversacion_anonima = anonimizar_conversacion(conversacion)

    # Análisis IA
    analisis = await analizar_con_groq(conversacion_anonima)
    log.info(f"Análisis: {json.dumps(analisis, ensure_ascii=False)}")

    # Guardar en PostgreSQL
    await guardar_auditoria({
        "id_bitrix":               deal_id,
        "asesor":                  nombre_asesor,
        "fecha_creacion_lead":     fecha_creacion,
        "conversacion_anonimizada": conversacion_anonima,
        "puntuacion_venta":        analisis.get("puntuacion_venta", 0),
        "puntuacion_atc":          analisis.get("puntuacion_atc", 0),
        "calificacion":            analisis.get("calificacion", "ATC"),
        "observacion":             analisis.get("observacion", "")
    })

    log.info(f"Deal {deal_id} auditado y guardado.")


# ════════════════════════════════════════════════════════
#  ENDPOINTS
# ════════════════════════════════════════════════════════
@app.get("/test/deal/{deal_id}")
async def test_deal(deal_id: str, background_tasks: BackgroundTasks):
    background_tasks.add_task(procesar_deal, deal_id)
    return {"status": "procesando", "deal_id": deal_id}

@app.get("/health")
async def health():
    return {"status": "ok", "modelo": GROQ_MODEL}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
