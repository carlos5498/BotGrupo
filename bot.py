"""
Bot de "grupo anónimo" vía Telegram.
--------------------------------------------------------------
Los usuarios aceptados escriben al bot en privado y el bot reenvía
ese mensaje (texto o multimedia) a todos los demás usuarios aceptados,
agregando el nombre de quien lo envió.

REQUISITOS DE ENTORNO (variables de entorno en Render):
  BOT_TOKEN   -> token de tu bot (de @BotFather)
  MONGO_URI   -> connection string de MongoDB
  OWNER_ID    -> tu ID numérico de Telegram (solo tú puedes abrir el panel admin)
  PORT        -> opcional, puerto para el keep-alive HTTP (default 8080)

REQUIREMENTS.TXT necesarios:
  python-telegram-bot[job-queue]==21.*
  pymongo

Comandos especiales (SOLO funcionan si los envía OWNER_ID):
  /Carlos13mar    -> activa el modo administrador (panel de ajustes)
  /Carlos13mar02  -> vuelve al modo usuario normal (tus mensajes se
                      reenvían al grupo como cualquier otro usuario)

Comando para cualquier usuario aceptado:
  /aportes -> ve cuánta multimedia lleva enviada en la ventana actual
              (editable desde el panel admin, por defecto 1 día)

Notas de esta versión:
- El nombre del remitente siempre va en su propia línea, arriba del texto.
- Los álbumes se agrupan por usuario (no por media_group_id de Telegram),
  así juntan fotos/videos aunque se manden uno por uno seguidos.
- Si respondes a un mensaje (texto u otro tipo) de otro usuario, el reenvío
  también se manda como respuesta al mensaje correspondiente en el chat de
  cada destinatario (se guarda un mapeo en memoria de las últimas
  transmisiones para poder ubicar el mensaje equivalente en cada chat).
- Si se configura una "meta de multimedia" (panel admin), cada usuario debe
  mandar esa cantidad de fotos/videos cada N días (configurable, por
  defecto 1 día) o deja de RECIBIR mensajes de los demás (no se banea) y
  libera su lugar para otro usuario. Se pueden marcar IDs como "excepción"
  (panel admin, debajo de Banear) para que nunca se les exija esa meta.

ENVÍO (rediseñado en esta versión para ser más rápido y mantener el orden):
- Hay una cola de envío POR CADA chat destino, cada una consumida por su
  propia tarea. Esto garantiza que los mensajes le lleguen a cada usuario
  en el mismo orden en que se generaron, y que un envío lento a un usuario
  no bloquee el envío a los demás (con una sola cola compartida y varios
  workers, un usuario con problemas de red podía atrasar a todos).
- La velocidad TOTAL (sumando todos los chats) sigue limitada por un
  "token bucket" global (TASA_MAXIMA msj/seg) para respetar el límite de
  Telegram (~30 msj/seg a chats distintos), con un pool de conexiones HTTP
  (POOL_CONEXIONES) para que esos envíos realmente viajen en paralelo por
  la red.
- Ya NO se descartan mensajes viejos si la cola crece: todo lo que entra
  se termina enviando, en orden.
- Cada QUEUE_LOG_INTERVAL segundos se loguea cuántos mensajes hay en total
  esperando a ser enviados (sumando todas las colas). Si ese número no baja
  nunca a 0 durante varios minutos seguidos, TASA_MAXIMA se quedó corta
  para la cantidad de usuarios/actividad actual y hay que subirla.
- La revisión periódica de metas de multimedia corre en un hilo aparte
  (no bloquea el envío de mensajes mientras revisa a todos los usuarios) y
  su frecuencia es configurable con REVISION_METAS_INTERVALO_SEG.
"""

import os
import time
import logging
import asyncio
import threading
from collections import OrderedDict
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer

from pymongo import MongoClient, UpdateOne
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    InputMediaPhoto, InputMediaVideo, InputMediaDocument, InputMediaAudio,
)
from telegram.request import HTTPXRequest
from telegram.constants import ChatMemberStatus
from telegram.error import Forbidden
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ChatMemberHandler, ContextTypes, filters,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------------------------------------------------------------
# CONFIGURACIÓN BÁSICA
# ---------------------------------------------------------------
BOT_TOKEN = os.environ["BOT_TOKEN"]
MONGO_URI = os.environ["MONGO_URI"]
OWNER_ID = int(os.environ["OWNER_ID"])
PORT = int(os.environ.get("PORT", 8080))

CMD_ADMIN_ON = "Carlos13mar"
CMD_ADMIN_OFF = "Carlos13mar02"

client = MongoClient(MONGO_URI)
db = client["grupo_anonimo_bot"]
col_usuarios = db["usuarios"]
col_config = db["config"]
col_grupos = db["grupos"]

VENTANA_MULTIMEDIA = timedelta(days=2)

REGEX_LINK = re.compile(r"(https?://\S+|www\.\S+|t\.me/\S+|telegram\.me/\S+)", re.IGNORECASE)

# Sesión del owner (en memoria, no necesita persistir en Mongo)
admin_sesion = {"activo": False, "paso": None}
# paso puede ser: None | "limite" | "password" | "bienvenida" | "baneo"

# Buffer temporal para armar álbumes: se agrupa por user_id (no por
# media_group_id de Telegram), así junta fotos/videos aunque el usuario
# los mande uno por uno seguido, no solo cuando los selecciona juntos.
media_buffer = {}

# Cache en memoria de la lista de usuarios aceptados. Sin esto, CADA mensaje
# de CADA usuario dispara una consulta a Mongo que bloquea todo el bot mientras
# responde (pymongo es síncrono) -- con pocos usuarios no se nota, pero con
# cientos empieza a sentirse todo más lento. Se refresca sola cada CACHE_TTL
# segundos, y se puede forzar su refresco con invalidar_cache_aceptados().
CACHE_TTL_ACEPTADOS = 3
_cache_aceptados = {"ids": [], "actualizado": 0.0}


def _consultar_aceptados_mongo():
    return [u["_id"] for u in col_usuarios.find(
        {"estado": "aceptado", "baneado": {"$ne": True}, "multimedia_activo": {"$ne": False}},
        {"_id": 1},
    )]


def invalidar_cache_aceptados():
    _cache_aceptados["actualizado"] = 0.0

# ---------------------------------------------------------------
# HELPERS DE CONFIG / USUARIOS (Mongo)
# ---------------------------------------------------------------
def get_config():
    conf = col_config.find_one({"_id": "config"}) or {"_id": "config"}
    conf.setdefault("limite_usuarios", 200)
    conf.setdefault("contraseña", None)
    conf.setdefault("ignorar_enlaces", False)
    conf.setdefault(
        "mensaje_bienvenida",
        "👋 ¡Bienvenido! Este es un chat grupal anónimo.\nEnvía la contraseña para poder participar.",
    )
    conf.setdefault("grupo_requerido", None)
    conf.setdefault("grupo_requerido_nombre", None)
    conf.setdefault("meta_multimedia", 0)  # 0 = desactivada
    return conf


def set_config_campo(campo, valor):
    col_config.update_one({"_id": "config"}, {"$set": {campo: valor}}, upsert=True)


def get_usuario(user_id):
    u = col_usuarios.find_one({"_id": user_id}) or {"_id": user_id}
    u.setdefault("nombre", "")
    u.setdefault("estado", "nuevo")  # nuevo | pendiente_grupo | pendiente_password | aceptado | baneado
    u.setdefault("baneado", False)
    u.setdefault("multimedia_activo", True)
    u.setdefault("multimedia_contador", 0)
    u.setdefault("multimedia_ventana_inicio", None)
    return u


def set_usuario_campo(user_id, campo, valor):
    col_usuarios.update_one({"_id": user_id}, {"$set": {campo: valor}}, upsert=True)


def aceptar_usuario(user_id):
    """Marca al usuario como aceptado e inicializa su ventana de metas de multimedia."""
    col_usuarios.update_one(
        {"_id": user_id},
        {"$set": {
            "estado": "aceptado",
            "multimedia_activo": True,
            "multimedia_contador": 0,
            "multimedia_ventana_inicio": datetime.utcnow(),
        }},
        upsert=True,
    )
    invalidar_cache_aceptados()


async def contar_aceptados():
    return len(await obtener_aceptados())


async def obtener_aceptados(excluir=None):
    # Destinatarios del reenvío: mismos criterios de siempre (los inactivos no reciben nada),
    # pero leídos de un cache en memoria en vez de golpear Mongo en cada mensaje.
    ahora = time.monotonic()
    if ahora - _cache_aceptados["actualizado"] > CACHE_TTL_ACEPTADOS:
        ids = await asyncio.to_thread(_consultar_aceptados_mongo)
        _cache_aceptados["ids"] = ids
        _cache_aceptados["actualizado"] = ahora
    ids = list(_cache_aceptados["ids"])
    if excluir is not None and excluir in ids:
        ids.remove(excluir)
    return ids


def registrar_multimedia(user_id, cantidad=1):
    """Suma multimedia enviada a la ventana actual del usuario; si con esto cumple
    la meta y estaba inactivo, lo reactiva de inmediato."""
    conf = get_config()
    meta = conf.get("meta_multimedia", 0)
    if not meta:
        return
    u = get_usuario(user_id)
    nuevo_contador = u.get("multimedia_contador", 0) + cantidad
    set_usuario_campo(user_id, "multimedia_contador", nuevo_contador)
    if not u.get("multimedia_activo", True) and nuevo_contador >= meta:
        set_usuario_campo(user_id, "multimedia_activo", True)
        set_usuario_campo(user_id, "multimedia_contador", 0)
        set_usuario_campo(user_id, "multimedia_ventana_inicio", datetime.utcnow())
        invalidar_cache_aceptados()


async def revisar_metas_multimedia(context):
    """Job periódico: al terminar cada ventana de 2 días, si no se cumplió la
    meta, el usuario pasa a inactivo (deja de recibir y libera su lugar)."""
    conf = get_config()
    meta = conf.get("meta_multimedia", 0)
    if not meta:
        return
    ahora = datetime.utcnow()
    hubo_cambios = False
    for u in col_usuarios.find({"estado": "aceptado", "baneado": {"$ne": True}}):
        inicio = u.get("multimedia_ventana_inicio") or ahora
        if ahora - inicio >= VENTANA_MULTIMEDIA:
            cumplio = u.get("multimedia_contador", 0) >= meta
            set_usuario_campo(u["_id"], "multimedia_activo", cumplio)
            set_usuario_campo(u["_id"], "multimedia_contador", 0)
            hubo_cambios = True
            set_usuario_campo(u["_id"], "multimedia_ventana_inicio", ahora)

    if hubo_cambios:
        invalidar_cache_aceptados()


def obtener_nombre(user):
    return user.first_name or user.username or f"Usuario{user.id}"


def contiene_enlace(texto):
    return bool(texto and REGEX_LINK.search(texto))


def formatear_caption(nombre, caption):
    # Nombre siempre en su propia línea, separado del texto -> "Fernanda:\nmensaje"
    if caption:
        return f"{nombre}:\n{caption}"
    return f"{nombre}:"


# ---------------------------------------------------------------
# COLA DE ENVÍO CON RITMO ADAPTATIVO (respeta límites de Telegram)
# ---------------------------------------------------------------

# Tamaño máximo de la cola de envío. Si se llena (porque entran más mensajes
# de los que TASA_MAXIMA permite repartir), se descartan los envíos más
# VIEJOS para priorizar los recientes, en vez de acumular un atraso creciente.
COLA_MAX_SIZE = 4000

# Cada cuántos segundos se loguea el tamaño actual de la cola, para poder
# diagnosticar si se está acumulando un atraso (ver nota al inicio del archivo).
QUEUE_LOG_INTERVAL = 15

cola_envio = asyncio.Queue(maxsize=COLA_MAX_SIZE)

# Contador de cuántos envíos se han descartado por cola llena, para verlo en logs.
_descartados_por_cola_llena = 0


class LimitadorTasa:
    """Limitador de tasa global tipo 'token bucket': deja pasar como máximo
    `tasa` envíos por segundo en TOTAL, sin importar cuántos workers estén
    enviando en paralelo ni cuántos usuarios haya. Esto es lo que de verdad
    respeta el límite de Telegram (~30 msj/seg a chats distintos)."""

    def __init__(self, tasa_por_seg):
        self.tasa = tasa_por_seg
        self.tokens = tasa_por_seg
        self.actualizado = time.monotonic()
        self.lock = asyncio.Lock()

    async def acquire(self):
        while True:
            async with self.lock:
                ahora = time.monotonic()
                self.tokens = min(self.tasa, self.tokens + (ahora - self.actualizado) * self.tasa)
                self.actualizado = ahora
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
            await asyncio.sleep(0.02)


TASA_MAXIMA = 25   # mensajes/seg en total (Telegram permite ~30/seg; dejamos margen)
NUM_WORKERS = 20   # envíos en paralelo (el límite real de velocidad lo pone el LimitadorTasa, no esto)

limitador = LimitadorTasa(TASA_MAXIMA)


async def _enviar_por_tipo(app, chat_id, tipo, payload, kwargs):
    if tipo == "text":
        return await app.bot.send_message(chat_id=chat_id, text=payload, **kwargs)
    elif tipo == "photo":
        return await app.bot.send_photo(chat_id=chat_id, photo=payload["file_id"], caption=payload.get("caption"), **kwargs)
    elif tipo == "video":
        return await app.bot.send_video(chat_id=chat_id, video=payload["file_id"], caption=payload.get("caption"), **kwargs)
    elif tipo == "document":
        return await app.bot.send_document(chat_id=chat_id, document=payload["file_id"], caption=payload.get("caption"), **kwargs)
    elif tipo == "audio":
        return await app.bot.send_audio(chat_id=chat_id, audio=payload["file_id"], caption=payload.get("caption"), **kwargs)
    elif tipo == "animation":
        return await app.bot.send_animation(chat_id=chat_id, animation=payload["file_id"], caption=payload.get("caption"), **kwargs)
    elif tipo == "voice":
        return await app.bot.send_voice(chat_id=chat_id, voice=payload["file_id"], caption=payload.get("caption"), **kwargs)
    elif tipo == "video_note":
        kwargs.pop("caption", None)
        return await app.bot.send_video_note(chat_id=chat_id, video_note=payload["file_id"], **kwargs)
    elif tipo == "sticker":
        return await app.bot.send_sticker(chat_id=chat_id, sticker=payload["file_id"], **kwargs)
    elif tipo == "album":
        return await app.bot.send_media_group(chat_id=chat_id, media=payload, **kwargs)
    return None


async def worker_envio(app):
    """Procesa la cola de reenvíos. Varias instancias de esta función corren en
    paralelo (ver NUM_WORKERS); el LimitadorTasa es quien realmente controla
    la velocidad total para no pasar el límite de Telegram."""
    while True:
        chat_id, tipo, payload = await cola_envio.get()
        await limitador.acquire()
        try:
            await _enviar_por_tipo(app, chat_id, tipo, payload, {})
        except Forbidden:
            # El usuario bloqueó al bot: dejamos de intentar mandarle (no lo baneamos, solo lo marcamos)
            set_usuario_campo(chat_id, "bloqueo_bot", True)
        except Exception as e:
            logging.warning(f"Error reenviando a {chat_id}: {e}")
        finally:
            cola_envio.task_done()


async def encolar_para_todos(tipo, payload, excluir_id):
    global _descartados_por_cola_llena
    for chat_id in await obtener_aceptados(excluir=excluir_id):
        try:
            cola_envio.put_nowait((chat_id, tipo, payload))
        except asyncio.QueueFull:
            # La cola está llena: se descarta el envío MÁS VIEJO (el que lleva
            # más tiempo esperando) para hacer lugar y priorizar lo reciente,
            # en vez de rechazar el mensaje nuevo o seguir acumulando atraso.
            try:
                cola_envio.get_nowait()
                cola_envio.task_done()
                _descartados_por_cola_llena += 1
            except asyncio.QueueEmpty:
                pass
            try:
                cola_envio.put_nowait((chat_id, tipo, payload))
            except asyncio.QueueFull:
                pass  # caso extremo: no insistimos más para no bloquear el loop


async def loguear_tamano_cola(context):
    """Job periódico de diagnóstico: muestra en los logs de Render el tamaño
    actual de la cola de envío. Si este número no baja de un valor alto
    durante varios minutos seguidos, confirma que TASA_MAXIMA se quedó corta
    para la cantidad de usuarios/actividad actual."""
    tamano = cola_envio.qsize()
    total_usuarios = await contar_aceptados()
    logging.info(
        f"[DIAGNOSTICO COLA] tamaño actual={tamano} | "
        f"usuarios aceptados={total_usuarios} | "
        f"descartados acumulados por cola llena={_descartados_por_cola_llena}"
    )


# ---------------------------------------------------------------
# VERIFICACIÓN DE GRUPO OBLIGATORIO
# ---------------------------------------------------------------
async def verificar_en_grupo(context, user_id, chat_id):
    try:
        member = await context.bot.get_chat_member(chat_id, user_id)
        return member.status in (
            ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER
        )
    except Exception:
        return False


# ---------------------------------------------------------------
# ONBOARDING (nuevo usuario -> grupo -> contraseña -> aceptado)
# ---------------------------------------------------------------
async def avanzar_onboarding(update, context, conf):
    user_id = update.effective_user.id
    msg = update.effective_message

    if await contar_aceptados() >= conf["limite_usuarios"]:
        await msg.reply_text("🚫 Se alcanzó el límite máximo de usuarios. Intenta más tarde.")
        return

    if conf.get("grupo_requerido"):
        en_grupo = await verificar_en_grupo(context, user_id, conf["grupo_requerido"])
        if not en_grupo:
            set_usuario_campo(user_id, "estado", "pendiente_grupo")
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("✅ Ya me uní", callback_data="check_grupo")]])
            texto = f"👥 Primero únete al grupo *{conf.get('grupo_requerido_nombre') or ''}* y luego pulsa el botón."
            await msg.reply_text(texto, parse_mode="Markdown", reply_markup=kb)
            return

    if conf.get("contraseña"):
        set_usuario_campo(user_id, "estado", "pendiente_password")
        await msg.reply_text(conf["mensaje_bienvenida"])
        return

    aceptar_usuario(user_id)
    await msg.reply_text(conf["mensaje_bienvenida"] + "\n\n✅ ¡Ya puedes escribir en el chat grupal!")


async def cmd_start(update, context):
    user_id = update.effective_user.id
    if user_id == OWNER_ID and admin_sesion["activo"]:
        return
    usuario = get_usuario(user_id)
    if usuario.get("baneado"):
        return
    set_usuario_campo(user_id, "nombre", obtener_nombre(update.effective_user))
    await avanzar_onboarding(update, context, get_config())


async def cb_check_grupo(update, context):
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    conf = get_config()

    if not conf.get("grupo_requerido"):
        await query.edit_message_text("✅ Ya puedes continuar, escribe /start.")
        return

    en_grupo = await verificar_en_grupo(context, user_id, conf["grupo_requerido"])
    if not en_grupo:
        await query.answer("❌ Todavía no detecto que te hayas unido.", show_alert=True)
        return

    if conf.get("contraseña"):
        set_usuario_campo(user_id, "estado", "pendiente_password")
        await query.edit_message_text(conf["mensaje_bienvenida"])
    else:
        aceptar_usuario(user_id)
        await query.edit_message_text(conf["mensaje_bienvenida"] + "\n\n✅ ¡Ya puedes escribir en el chat grupal!")


async def procesar_onboarding(update, context, usuario):
    msg = update.effective_message
    user_id = update.effective_user.id
    conf = get_config()

    if msg.text and msg.text.startswith("/"):
        return  # comandos no aplican aquí

    estado = usuario["estado"]

    if estado == "pendiente_grupo":
        await msg.reply_text("Primero únete al grupo requerido y pulsa «✅ Ya me uní» en el mensaje anterior.")
        return

    if estado == "pendiente_password":
        texto = (msg.text or "").strip()
        if conf.get("contraseña") and texto == conf["contraseña"]:
            aceptar_usuario(user_id)
            await msg.reply_text("✅ ¡Contraseña correcta! Ya puedes escribir en el chat grupal.")
        else:
            await msg.reply_text("❌ Contraseña incorrecta. Intenta de nuevo.")
        return

    # estado "nuevo" escribiendo directo sin /start
    await avanzar_onboarding(update, context, conf)


# ---------------------------------------------------------------
# REENVÍO DE MENSAJES ENTRE USUARIOS ACEPTADOS
# ---------------------------------------------------------------
def _armar_input_media(m, caption=None):
    if m.photo:
        return InputMediaPhoto(m.photo[-1].file_id, caption=caption)
    elif m.video:
        return InputMediaVideo(m.video.file_id, caption=caption)
    return None


async def _flush_media_buffer(context: ContextTypes.DEFAULT_TYPE):
    user_id = context.job.data["user_id"]
    info = media_buffer.pop(user_id, None)
    if not info:
        return

    mensajes = sorted(info["mensajes"], key=lambda m: m.message_id)
    nombre = info["nombre"]

    caption_original = next((m.caption for m in mensajes if m.caption), None)
    if contiene_enlace(caption_original) and get_config().get("ignorar_enlaces"):
        return

    registrar_multimedia(user_id, len(mensajes))
    caption = formatear_caption(nombre, caption_original)

    # Un solo elemento -> se manda suelto (no tiene sentido un "álbum" de 1)
    if len(mensajes) == 1:
        m = mensajes[0]
        tipo = "photo" if m.photo else "video"
        file_id = m.photo[-1].file_id if m.photo else m.video.file_id
        await encolar_para_todos(tipo, {"file_id": file_id, "caption": caption}, user_id)
        return

    # Telegram permite máximo 10 elementos por álbum -> se parte en bloques si hace falta
    for inicio in range(0, len(mensajes), 10):
        bloque = mensajes[inicio:inicio + 10]
        cap_bloque = caption if inicio == 0 else None
        media_list = []
        for i, m in enumerate(bloque):
            item = _armar_input_media(m, caption=cap_bloque if i == 0 else None)
            if item:
                media_list.append(item)

        if len(media_list) < 2:
            if media_list:
                m = bloque[0]
                tipo = "photo" if m.photo else "video"
                file_id = m.photo[-1].file_id if m.photo else m.video.file_id
                await encolar_para_todos(tipo, {"file_id": file_id, "caption": cap_bloque or formatear_caption(nombre, None)}, user_id)
            continue

        await encolar_para_todos("album", media_list, user_id)


async def manejar_mensaje(update, context):
    user = update.effective_user
    msg = update.effective_message
    user_id = user.id
    chat_id = msg.chat_id

    # --- Modo administrador (solo el owner) ---
    if user_id == OWNER_ID and admin_sesion["activo"]:
        await procesar_panel_admin(update, context)
        return

    usuario = get_usuario(user_id)

    if usuario.get("baneado"):
        return  # ignorado por completo

    if usuario["estado"] != "aceptado":
        await procesar_onboarding(update, context, usuario)
        return

    # --- Usuario aceptado: reenviar a los demás ---
    nombre = obtener_nombre(user)

    # --- Fotos/Videos: se agrupan en álbum durante una breve espera ---
    # (agrupamos por usuario, no por el media_group_id de Telegram, porque
    # ese campo solo existe si el usuario los seleccionó y mandó juntos;
    # si los manda uno por uno seguido, igual queremos juntarlos)
    if msg.photo or msg.video:
        conf = get_config()
        if contiene_enlace(msg.caption) and conf.get("ignorar_enlaces"):
            return

        if user_id not in media_buffer:
            media_buffer[user_id] = {"mensajes": [], "nombre": nombre}
        media_buffer[user_id]["mensajes"].append(msg)

        # Si ya se juntaron 10 (máximo de Telegram por álbum), se manda de inmediato
        for job in context.job_queue.get_jobs_by_name(f"media_{user_id}"):
            job.schedule_removal()
        if len(media_buffer[user_id]["mensajes"]) >= 10:
            context.job_queue.run_once(_flush_media_buffer, when=0, data={"user_id": user_id}, name=f"media_{user_id}")
        else:
            context.job_queue.run_once(_flush_media_buffer, when=1.5, data={"user_id": user_id}, name=f"media_{user_id}")
        return

    # --- Mensaje suelto (texto u otro tipo de multimedia) ---
    texto = msg.text or msg.caption
    conf = get_config()
    if contiene_enlace(texto) and conf.get("ignorar_enlaces"):
        return

    if msg.document:
        await encolar_para_todos("document", {"file_id": msg.document.file_id, "caption": formatear_caption(nombre, texto)}, user_id)
    elif msg.audio:
        await encolar_para_todos("audio", {"file_id": msg.audio.file_id, "caption": formatear_caption(nombre, texto)}, user_id)
    elif msg.animation:
        await encolar_para_todos("animation", {"file_id": msg.animation.file_id, "caption": formatear_caption(nombre, texto)}, user_id)
    elif msg.voice:
        await encolar_para_todos("voice", {"file_id": msg.voice.file_id, "caption": formatear_caption(nombre, texto)}, user_id)
    elif msg.video_note:
        await encolar_para_todos("text", formatear_caption(nombre, None), user_id)
        await encolar_para_todos("video_note", {"file_id": msg.video_note.file_id}, user_id)
    elif msg.sticker:
        await encolar_para_todos("text", formatear_caption(nombre, None), user_id)
        await encolar_para_todos("sticker", {"file_id": msg.sticker.file_id}, user_id)
    elif msg.text:
        await encolar_para_todos("text", formatear_caption(nombre, msg.text), user_id)


async def cmd_aportes(update, context):
    user_id = update.effective_user.id
    usuario = get_usuario(user_id)

    if usuario["estado"] != "aceptado":
        await update.message.reply_text("Aún no formas parte del chat grupal.")
        return

    conf = get_config()
    meta = conf.get("meta_multimedia", 0)
    if not meta:
        await update.message.reply_text("📊 No hay una meta de multimedia configurada actualmente.")
        return

    contador = usuario.get("multimedia_contador", 0)
    activo = usuario.get("multimedia_activo", True)
    inicio = usuario.get("multimedia_ventana_inicio") or datetime.utcnow()
    restante = (inicio + VENTANA_MULTIMEDIA) - datetime.utcnow()

    if restante.total_seconds() > 0:
        horas = int(restante.total_seconds() // 3600)
        tiempo_txt = f"~{horas} h"
    else:
        tiempo_txt = "menos de 1 h"

    estado_txt = "🟢 Activo (recibiendo mensajes)" if activo else "🔴 Inactivo (no recibirás mensajes hasta cumplir tu meta)"

    await update.message.reply_text(
        "📊 *Tus aportes*\n\n"
        f"Multimedia enviada: {contador}/{meta}\n"
        f"Tiempo restante de la ventana: {tiempo_txt}\n"
        f"Estado: {estado_txt}",
        parse_mode="Markdown",
    )


# ---------------------------------------------------------------
# PANEL DE ADMINISTRADOR
# ---------------------------------------------------------------
def teclado_admin(conf):
    estado_enlaces = "🟢 Activado" if conf.get("ignorar_enlaces") else "🔴 Desactivado"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"👥 Límite de usuarios: {conf['limite_usuarios']}", callback_data="adm_limite")],
        [InlineKeyboardButton("🔑 Establecer contraseña", callback_data="adm_password")],
        [InlineKeyboardButton(f"🔗 Ignorar enlaces: {estado_enlaces}", callback_data="adm_enlaces")],
        [InlineKeyboardButton("💬 Mensaje de bienvenida", callback_data="adm_bienvenida")],
        [InlineKeyboardButton("👥 Unir grupo", callback_data="adm_grupo")],
        [InlineKeyboardButton("⛔ Banear", callback_data="adm_banear")],
        [InlineKeyboardButton(
            f"🎯 Meta multimedia: {conf.get('meta_multimedia', 0) or 'desactivada'}",
            callback_data="adm_meta",
        )],
    ])


async def texto_panel(conf):
    total = await contar_aceptados()
    meta = conf.get("meta_multimedia", 0)
    return (
        "🛠️ *Panel de Administrador*\n\n"
        f"👥 Usuarios aceptados: {total}/{conf['limite_usuarios']}\n"
        f"🔑 Contraseña: {'configurada' if conf.get('contraseña') else 'sin configurar'}\n"
        f"👥 Grupo requerido: {conf.get('grupo_requerido_nombre') or 'ninguno'}\n"
        f"🎯 Meta multimedia (cada 2 días): {meta if meta else 'desactivada'}"
    )


async def cmd_admin_on(update, context):
    if update.effective_user.id != OWNER_ID:
        return
    admin_sesion["activo"] = True
    admin_sesion["paso"] = None
    conf = get_config()
    await update.message.reply_text(await texto_panel(conf), parse_mode="Markdown", reply_markup=teclado_admin(conf))


async def cmd_admin_off(update, context):
    if update.effective_user.id != OWNER_ID:
        return
    admin_sesion["activo"] = False
    admin_sesion["paso"] = None
    await update.message.reply_text("👤 Modo usuario normal activado. Tus mensajes ahora se reenviarán al grupo.")


async def cb_admin(update, context):
    query = update.callback_query
    if update.effective_user.id != OWNER_ID:
        await query.answer()
        return
    await query.answer()
    data = query.data
    conf = get_config()

    if data == "adm_limite":
        admin_sesion["paso"] = "limite"
        await query.edit_message_text("✏️ Envía el nuevo límite máximo de usuarios (solo el número).")
    elif data == "adm_password":
        admin_sesion["paso"] = "password"
        await query.edit_message_text("✏️ Envía el texto que será la nueva contraseña.")
    elif data == "adm_enlaces":
        nuevo = not conf.get("ignorar_enlaces")
        set_config_campo("ignorar_enlaces", nuevo)
        conf["ignorar_enlaces"] = nuevo
        await query.edit_message_text(await texto_panel(conf), parse_mode="Markdown", reply_markup=teclado_admin(conf))
    elif data == "adm_bienvenida":
        admin_sesion["paso"] = "bienvenida"
        await query.edit_message_text("✏️ Envía el nuevo mensaje de bienvenida.")
    elif data == "adm_grupo":
        grupos = list(col_grupos.find({}))
        if not grupos:
            await query.edit_message_text(
                "⚠️ Aún no agrego el bot como *administrador* de ningún grupo. "
                "Agrégalo a tu grupo con permisos de admin y vuelve a intentar.",
                parse_mode="Markdown", reply_markup=teclado_admin(conf),
            )
            return
        botones = [[InlineKeyboardButton(g["titulo"], callback_data=f"adm_setgrupo_{g['_id']}")] for g in grupos]
        botones.append([InlineKeyboardButton("🚫 Quitar requisito de grupo", callback_data="adm_setgrupo_none")])
        await query.edit_message_text("Selecciona el grupo obligatorio:", reply_markup=InlineKeyboardMarkup(botones))
    elif data.startswith("adm_setgrupo_"):
        valor = data.split("adm_setgrupo_", 1)[1]
        if valor == "none":
            set_config_campo("grupo_requerido", None)
            set_config_campo("grupo_requerido_nombre", None)
        else:
            chat_id = int(valor)
            g = col_grupos.find_one({"_id": chat_id})
            set_config_campo("grupo_requerido", chat_id)
            set_config_campo("grupo_requerido_nombre", g["titulo"] if g else str(chat_id))
        conf = get_config()
        await query.edit_message_text(await texto_panel(conf), parse_mode="Markdown", reply_markup=teclado_admin(conf))
    elif data == "adm_banear":
        admin_sesion["paso"] = "baneo"
        await query.edit_message_text("✏️ Envía las IDs a banear, una por línea.")
    elif data == "adm_meta":
        admin_sesion["paso"] = "meta"
        await query.edit_message_text(
            "✏️ Envía cuántas fotos/videos debe mandar cada usuario cada 2 días.\n"
            "Envía 0 para desactivar esta función."
        )


async def procesar_panel_admin(update, context):
    msg = update.effective_message
    if msg.text and msg.text.startswith("/"):
        return  # los comandos los maneja su propio handler, aquí se ignoran

    paso = admin_sesion.get("paso")
    if not paso:
        return  # nada pendiente -> en modo admin no se reenvía nada

    texto = (msg.text or "").strip()

    if paso == "limite":
        if texto.isdigit():
            set_config_campo("limite_usuarios", int(texto))
            await msg.reply_text(f"✅ Límite actualizado a {texto} usuarios.")
        else:
            await msg.reply_text("⚠️ Envía solo un número.")
            return
    elif paso == "password":
        set_config_campo("contraseña", texto)
        await msg.reply_text("✅ Contraseña actualizada.")
    elif paso == "bienvenida":
        set_config_campo("mensaje_bienvenida", texto)
        await msg.reply_text("✅ Mensaje de bienvenida actualizado.")
    elif paso == "baneo":
        ids = [l.strip() for l in texto.splitlines() if l.strip().lstrip("-").isdigit()]
        for uid in ids:
            set_usuario_campo(int(uid), "baneado", True)
            set_usuario_campo(int(uid), "estado", "baneado")
        if ids:
            invalidar_cache_aceptados()
        await msg.reply_text(f"✅ {len(ids)} usuario(s) baneado(s).")
    elif paso == "meta":
        if texto.isdigit():
            set_config_campo("meta_multimedia", int(texto))
            if int(texto) == 0:
                await msg.reply_text("✅ Meta de multimedia desactivada.")
            else:
                await msg.reply_text(f"✅ Meta actualizada: {texto} fotos/videos cada 2 días.")
        else:
            await msg.reply_text("⚠️ Envía solo un número.")
            return

    admin_sesion["paso"] = None
    conf = get_config()
    await msg.reply_text(await texto_panel(conf), parse_mode="Markdown", reply_markup=teclado_admin(conf))


# ---------------------------------------------------------------
# SEGUIMIENTO DE GRUPOS DONDE EL BOT ES ADMIN (para "Unir grupo")
# ---------------------------------------------------------------
async def on_bot_chat_member_update(update, context):
    result = update.my_chat_member
    if result.chat.type not in ("group", "supergroup"):
        return
    nuevo_status = result.new_chat_member.status
    if nuevo_status == ChatMemberStatus.ADMINISTRATOR:
        col_grupos.update_one({"_id": result.chat.id}, {"$set": {"titulo": result.chat.title}}, upsert=True)
    elif nuevo_status in (ChatMemberStatus.LEFT, ChatMemberStatus.KICKED, ChatMemberStatus.MEMBER):
        col_grupos.delete_one({"_id": result.chat.id})


# ---------------------------------------------------------------
# KEEP-ALIVE HTTP (para Render + cron-job.org)
# ---------------------------------------------------------------
def keep_alive():
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")

        def log_message(self, *args):
            pass

    HTTPServer(("0.0.0.0", PORT), Handler).serve_forever()


# ---------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------
def main():
    threading.Thread(target=keep_alive, daemon=True).start()

    request = HTTPXRequest(
        connection_pool_size=NUM_WORKERS + 10,  # antes: 1 (default) -> todos los workers hacían fila igual
        pool_timeout=20.0,
    )
    app = Application.builder().token(BOT_TOKEN).request(request).build()

    app.add_handler(CommandHandler(CMD_ADMIN_ON, cmd_admin_on))
    app.add_handler(CommandHandler(CMD_ADMIN_OFF, cmd_admin_off))
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("aportes", cmd_aportes))
    app.add_handler(CallbackQueryHandler(cb_check_grupo, pattern="^check_grupo$"))
    app.add_handler(CallbackQueryHandler(cb_admin, pattern="^adm_"))
    app.add_handler(ChatMemberHandler(on_bot_chat_member_update, ChatMemberHandler.MY_CHAT_MEMBER))
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND, manejar_mensaje))

    for _ in range(NUM_WORKERS):
        app.job_queue.run_once(lambda ctx: asyncio.create_task(worker_envio(app)), when=1)
    app.job_queue.run_repeating(revisar_metas_multimedia, interval=1800, first=10)  # cada 30 min
    app.job_queue.run_repeating(loguear_tamano_cola, interval=QUEUE_LOG_INTERVAL, first=QUEUE_LOG_INTERVAL)

    logging.info("🤖 Bot de grupo anónimo iniciado...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
