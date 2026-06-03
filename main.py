"""
=============================================================
  NETLIFE - Detector de Oportunidades de Venta en ATC
  Webhook Bitrix24 + Análisis con Groq (llama3)
=============================================================
"""

import os
import re
import json
import httpx
from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

app = FastAPI(title="Netlife ATC - Detector de Oportunidades")

# ── Configuración ────────────────────────────────────────────
BITRIX_WEBHOOK = os.getenv("BITRIX_WEBHOOK", "")
ATC_STAGE_ID   = os.getenv("ATC_STAGE_ID", "C19:UC_U0JYD8")
GROQ_API_KEY   = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL     = os.getenv("GROQ_MODEL", "llama3-8b-8192")
# ────────────────────────────────────────────────────────────


# ════════════════════════════════════════════════════════════
#  PASO 1 — Recibir webhook de Bitrix24
# ════════════════════════════════════════════════════════════
@app.post("/webhook/bitrix")
async def receive_webhook(request: Request, background_tasks: BackgroundTasks):
    try:
        data = dict(await request.form())
    except Exception:
        data = await request.json()

    log.info(f"Webhook recibido: {data}")

    deal_id  = data.get("data[FIELDS][ID]") or data.get("document_id[2]")
    stage_id = data.get("data[FIELDS][STAGE_ID]")

    if not deal_id:
        return JSONResponse({"status": "ignored", "reason": "no deal_id"})

    if stage_id and stage_id != ATC_STAGE_ID:
        log.info(f"Deal {deal_id} en etapa {stage_id} — no es ATC, ignorando.")
        return JSONResponse({"status": "ignored", "reason": f"stage {stage_id} != ATC"})

    background_tasks.add_task(procesar_deal, deal_id)
    return JSONResponse({"status": "ok", "deal_id": deal_id})


# ════════════════════════════════════════════════════════════
#  PASO 2 — Extraer conversación del deal
# ════════════════════════════════════════════════════════════
async def obtener_chat_id(deal_id: str) -> str | None:
    url = f"{BITRIX_WEBHOOK}/crm.activity.list"
    params = {
        "filter[OWNER_TYPE_ID]": "2",
        "filter[OWNER_ID]": deal_id,
        "select[]": "*"
    }
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(url, params=params)
        result = resp.json().get("result", [])

    for activity in result:
        if activity.get("PROVIDER_ID") == "IMOPENLINES_SESSION":
            chat_id = activity.get("ASSOCIATED_ENTITY_ID")
            log.info(f"Chat ID encontrado: {chat_id}")
            return chat_id

    log.warning(f"No se encontró chat de Wazzup para deal {deal_id}")
    return None


async def obtener_mensajes(chat_id: str) -> tuple[list, list]:
    url = f"{BITRIX_WEBHOOK}/im.dialog.messages.get"
    params = {"DIALOG_ID": f"chat{chat_id}", "LIMIT": 200}

    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(url, params=params)
        data = resp.json().get("result", {})

    mensajes_raw = data.get("messages", [])
    usuarios     = data.get("users", [])
    usuarios_map = {u["id"]: u for u in usuarios}

    mensajes = []
    for m in mensajes_raw:
        author_id = m.get("author_id", 0)
        texto     = m.get("text", "").strip()

        if author_id == 0 or not texto:
            continue
        if texto.startswith("=== SYSTEM WZ ==="):
            continue

        user_info  = usuarios_map.get(author_id, {})
        nombre     = user_info.get("name", str(author_id))
        es_cliente = user_info.get("connector", False) or user_info.get("extranet", False)
        rol        = "CLIENTE" if es_cliente else "ASESOR"

        mensajes.append({
            "fecha":  m.get("date", "")[:10],
            "rol":    rol,
            "nombre": nombre,
            "texto":  texto
        })

    mensajes.reverse()
    return mensajes, usuarios


def formatear_conversacion(mensajes: list) -> str:
    return "\n".join(
        f"[{m['fecha']}] {m['rol']} ({m['nombre']}): {m['texto']}"
        for m in mensajes
    )


# ════════════════════════════════════════════════════════════
#  ANONIMIZACIÓN — elimina datos personales antes de enviar a Groq
# ════════════════════════════════════════════════════════════
def anonimizar(texto: str) -> str:
    texto = re.sub(r'\+?593\d{9}', '[TELEFONO]', texto)
    texto = re.sub(r'0\d{9}', '[TELEFONO]', texto)
    texto = re.sub(r'\b\d{7,15}\b', '[TELEFONO]', texto)
    texto = re.sub(r'[\w\.-]+@[\w\.-]+\.\w+', '[EMAIL]', texto)
    texto = re.sub(r'\b\d{10}\b', '[CEDULA]', texto)
    texto = re.sub(r'https?://\S+', '[URL]', texto)
    texto = re.sub(r'-?\d+\.\d{4,}', '[COORDENADA]', texto)
    texto = re.sub(r'\(([^)]+)\)', '(PARTICIPANTE)', texto)
    return texto


def anonimizar_conversacion(conversacion: str) -> str:
    return "\n".join(anonimizar(linea) for linea in conversacion.splitlines())


# ════════════════════════════════════════════════════════════
#  PASO 3 — Analizar con Groq
# ════════════════════════════════════════════════════════════
PROMPT_SISTEMA = """
Eres un analista de calidad para un equipo de ventas de servicios de internet (NETLIFE Ecuador).
Tu tarea es evaluar conversaciones de WhatsApp que llegan al canal de Atención al Cliente (ATC).

CONTEXTO:
- Los asesores son de VENTAS pero atienden consultas de servicio al cliente.
- Debes detectar si el cliente, dentro de la conversación, mostró una INTENCIÓN DE COMPRA que el asesor NO aprovechó.

SEÑALES DE INTENCIÓN DE COMPRA:
- Preguntar por precios, planes o velocidades
- Mencionar que un familiar o vecino quiere contratar
- Comparar con otra empresa de internet
- Preguntar si pueden agregar un servicio adicional
- Mencionar que se muda y necesita internet nuevo
- Decir que el plan actual ya no les alcanza

SISTEMA DE PUNTUACIÓN (0 a 100):
- 0   = Conversación 100% de servicio, sin ninguna oportunidad de venta
- 25  = Señales muy débiles, casi imperceptibles
- 50  = Oportunidad moderada que el asesor pudo explorar
- 75  = Clara intención de compra que el asesor ignoró
- 100 = El cliente quería comprar explícitamente y el asesor no actuó

RESPONDE ÚNICAMENTE en este formato JSON (sin texto extra):
{
  "score": <número 0-100>,
  "nivel": "<SIN OPORTUNIDAD | OPORTUNIDAD DÉBIL | OPORTUNIDAD MODERADA | OPORTUNIDAD ALTA | VENTA PERDIDA>",
  "señales_detectadas": ["señal 1", "señal 2"],
  "resumen": "<2-3 oraciones explicando qué pasó>",
  "recomendacion": "<qué debería hacer el equipo ahora con este cliente>"
}
"""

async def analizar_con_groq(conversacion: str) -> dict:
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": GROQ_MODEL,
        "messages": [
            {"role": "system", "content": PROMPT_SISTEMA},
            {"role": "user",   "content": f"--- CONVERSACIÓN ---\n{conversacion}\n--- FIN ---"}
        ],
        "temperature": 0.2,
        "response_format": {"type": "json_object"}
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


# ════════════════════════════════════════════════════════════
#  PASO 4 — Guardar resultado en Bitrix24
# ════════════════════════════════════════════════════════════
async def guardar_resultado_en_deal(deal_id: str, analisis: dict):
    score   = analisis.get("score", "N/A")
    nivel   = analisis.get("nivel", "")
    resumen = analisis.get("resumen", "")
    recom   = analisis.get("recomendacion", "")
    señales = "\n".join([f"• {s}" for s in analisis.get("señales_detectadas", [])])

    comentario = (
        f"ANALISIS IA - OPORTUNIDAD DE VENTA\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"SCORE: {score}/100 — {nivel}\n\n"
        f"Senales detectadas:\n{señales}\n\n"
        f"Resumen: {resumen}\n\n"
        f"Recomendacion: {recom}"
    )

    url = f"{BITRIX_WEBHOOK}/crm.timeline.comment.add"
    payload = {
        "fields[ENTITY_ID]":   deal_id,
        "fields[ENTITY_TYPE]": "deal",
        "fields[COMMENT]":     comentario
    }

    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(url, data=payload)
        log.info(f"Comentario guardado en deal {deal_id}: {resp.json()}")


# ════════════════════════════════════════════════════════════
#  ORQUESTADOR
# ════════════════════════════════════════════════════════════
async def procesar_deal(deal_id: str):
    log.info(f"Procesando deal {deal_id}...")

    chat_id = await obtener_chat_id(deal_id)
    if not chat_id:
        return

    mensajes, _ = await obtener_mensajes(chat_id)
    if not mensajes:
        log.warning(f"No hay mensajes en el chat {chat_id}.")
        return

    log.info(f"{len(mensajes)} mensajes extraídos")

    conversacion         = formatear_conversacion(mensajes)
    conversacion_anonima = anonimizar_conversacion(conversacion)
    log.info("Conversacion anonimizada antes de enviar a Groq.")
    analisis             = await analizar_con_groq(conversacion_anonima)
    log.info(f"Resultado: {json.dumps(analisis, ensure_ascii=False)}")

    await guardar_resultado_en_deal(deal_id, analisis)
    log.info(f"Proceso completado para deal {deal_id}")


# ════════════════════════════════════════════════════════════
#  ENDPOINTS
# ════════════════════════════════════════════════════════════
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
