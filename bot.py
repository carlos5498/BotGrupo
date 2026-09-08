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
"""

import os
import time
import logging
import asyncio
import threading
from collections import OrderedDict
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer

from pymongo import MongoClient
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    InputMediaPhoto, InputMediaVideo,
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

# Sesión del owner (en memoria)
admin_sesion = {"activo": False, "paso": None}

# Buffer temporal para armar álbumes por usuario
media_buffer = {}

# Mapeo de respuestas: (chat_origen, id_mensaje_origen) -> {chat_destino: id_mensaje_destino}
MAPA_RESPUESTAS = OrderedDict()
MAX_MAPA = 3000

def guardar_mapeo(origen_key, mapeo):
    MAPA_RESPUESTAS[origen_key] = mapeo
    if len(MAPA_RESPUESTAS) > MAX_MAPA:
        MAPA_RESPUESTAS.popitem(last=False)

# Cache de usuarios aceptados
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
    conf.setdefault(
        "mensaje_bienvenida",
        "👋 ¡Bienvenido! Este es un chat grupal anónimo.\nEnvía la contraseña para poder participar.",
    )
    conf.setdefault("grupo_requerido", None)
    conf.setdefault("grupo_requerido_nombre", None)
    conf.setdefault("meta_multimedia", 0)  # 0 = desactivada
    conf.setdefault("dias_ventana", 1)      # Por defecto 1 día
    return conf


def set_config_campo(campo, valor):
    col_config.update_one({"_id": "config"}, {"$set": {campo: valor}}, upsert=True)


def get_usuario(user_id):
    u = col_usuarios.find_one({"_id": user_id}) or {"_id": user_id}
    u.setdefault("nombre", "")
    u.setdefault("estado", "nuevo")
    u.setdefault("baneado", False)
    u.setdefault("excepcion", False)
    u.setdefault("multimedia_activo", True)
    u.setdefault("multimedia_contador", 0)
    u.setdefault("multimedia_ventana_inicio", None)
    return u


def set_usuario_campo(user_id, campo, valor):
    col_usuarios.update_one({"_id": user_id}, {"$set": {campo: valor}}, upsert=True)


def aceptar_usuario(user_id):
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
    conf = get_config()
    meta = conf.get("meta_multimedia", 0)
    dias = conf.get("dias_ventana", 1)
    if not meta:
        return
    ventana = timedelta(days=dias)
    ahora = datetime.utcnow()
    hubo_cambios = False
    for u in col_usuarios.find({"estado": "aceptado", "baneado": {"$ne": True}}):
        if u.get("excepcion"):
            continue  # Exención activa
        inicio = u.get("multimedia_ventana_inicio") or ahora
        if ahora - inicio >= ventana:
            cumplio = u.get("multimedia_contador", 0) >= meta
            set_usuario_campo(u["_id"], "multimedia_activo", cumplio)
            set_usuario_campo(u["_id"], "multimedia_contador", 0)
            set_usuario_campo(u["_id"], "multimedia_ventana_inicio", ahora)
            hubo_cambios = True

    if hubo_cambios:
        invalidar_cache_aceptados()


def obtener_nombre(user):
    return user.first_name or user.username or f"Usuario{user.id}"


def formatear_caption(nombre, caption):
    if caption:
        return f"{nombre}:\n{caption}"
    return f"{nombre}:"

# ---------------------------------------------------------------
# LIMITADOR Y ENVÍO RÁPIDO
# ---------------------------------------------------------------
class LimitadorTasa:
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


limitador = LimitadorTasa(25)  # Envíos por segundo respetando Telegram


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
        kwargs_vn = kwargs.copy()
        kwargs_vn.pop("caption", None)
        return await app.bot.send_video_note(chat_id=chat_id, video_note=payload["file_id"], **kwargs_vn)
    elif tipo == "sticker":
        return await app.bot.send_sticker(chat_id=chat_id, sticker=payload["file_id"], **kwargs)
    elif tipo == "album":
        return await app.bot.send_media_group(chat_id=chat_id, media=payload, **kwargs)
    return None


async def _enviar_a_usuario(app, chat_id, tipo, payload, kwargs, resultado_map):
    await limitador.acquire()
    try:
        res = await _enviar_por_tipo(app, chat_id, tipo, payload, kwargs)
        if res:
            if isinstance(res, list):
                resultado_map[chat_id] = res[0].message_id
            else:
                resultado_map[chat_id] = res.message_id
    except Forbidden:
        set_usuario_campo(chat_id, "bloqueo_bot", True)
    except Exception as e:
        logging.warning(f"Error reenviando a {chat_id}: {e}")


async def encolar_para_todos(app, tipo, payload, excluir_id, reply_map=None, origen_key=None):
    destinatarios = await obtener_aceptados(excluir=excluir_id)
    if not destinatarios:
        return

    resultado_map = {}
    tasks = []
    for dest_id in destinatarios:
        kwargs = {}
        if reply_map and dest_id in reply_map:
            kwargs["reply_to_message_id"] = reply_map[dest_id]
        tasks.append(_enviar_a_usuario(app, dest_id, tipo, payload, kwargs, resultado_map))

    await asyncio.gather(*tasks, return_exceptions=True)

    if origen_key and resultado_map:
        guardar_mapeo(origen_key, resultado_map)

# ---------------------------------------------------------------
# VERIFICACIÓN DE GRUPO OBLIGATORIO Y ONBOARDING
# ---------------------------------------------------------------
async def verificar_en_grupo(context, user_id, chat_id):
    try:
        member = await context.bot.get_chat_member(chat_id, user_id)
        return member.status in (
            ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER
        )
    except Exception:
        return False


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
        return

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

    await avanzar_onboarding(update, context, conf)

# ---------------------------------------------------------------
# REENVÍO DE MENSAJES ENTRE USUARIOS
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

    registrar_multimedia(user_id, len(mensajes))
    caption = formatear_caption(nombre, caption_original)

    m0 = mensajes[0]
    origen_key = (m0.chat_id, m0.message_id)
    reply_map = None
    if m0.reply_to_message:
        reply_map = MAPA_RESPUESTAS.get((m0.chat_id, m0.reply_to_message.message_id))

    app = context.application

    if len(mensajes) == 1:
        m = mensajes[0]
        tipo = "photo" if m.photo else "video"
        file_id = m.photo[-1].file_id if m.photo else m.video.file_id
        await encolar_para_todos(app, tipo, {"file_id": file_id, "caption": caption}, user_id, reply_map=reply_map, origen_key=origen_key)
        return

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
                await encolar_para_todos(app, tipo, {"file_id": file_id, "caption": cap_bloque or formatear_caption(nombre, None)}, user_id, reply_map=reply_map, origen_key=origen_key)
            continue

        await encolar_para_todos(app, "album", media_list, user_id, reply_map=reply_map, origen_key=origen_key)


async def manejar_mensaje(update, context):
    user = update.effective_user
    msg = update.effective_message
    user_id = user.id
    chat_id = msg.chat_id

    if user_id == OWNER_ID and admin_sesion["activo"]:
        await procesar_panel_admin(update, context)
        return

    usuario = get_usuario(user_id)
    if usuario.get("baneado"):
        return

    if usuario["estado"] != "aceptado":
        await procesar_onboarding(update, context, usuario)
        return

    nombre = obtener_nombre(user)

    # Detección de respuesta a otro mensaje
    reply_map = None
    if msg.reply_to_message:
        parent_key = (chat_id, msg.reply_to_message.message_id)
        reply_map = MAPA_RESPUESTAS.get(parent_key)

    origen_key = (chat_id, msg.message_id)

    # Fotos / Videos -> agrupación por álbum
    if msg.photo or msg.video:
        if user_id not in media_buffer:
            media_buffer[user_id] = {"mensajes": [], "nombre": nombre}
        media_buffer[user_id]["mensajes"].append(msg)

        for job in context.job_queue.get_jobs_by_name(f"media_{user_id}"):
            job.schedule_removal()
        if len(media_buffer[user_id]["mensajes"]) >= 10:
            context.job_queue.run_once(_flush_media_buffer, when=0, data={"user_id": user_id}, name=f"media_{user_id}")
        else:
            context.job_queue.run_once(_flush_media_buffer, when=1.5, data={"user_id": user_id}, name=f"media_{user_id}")
        return

    # Mensajes de texto u otros tipos
    app = context.application
    texto = msg.text or msg.caption

    if msg.document:
        asyncio.create_task(encolar_para_todos(app, "document", {"file_id": msg.document.file_id, "caption": formatear_caption(nombre, texto)}, user_id, reply_map, origen_key))
    elif msg.audio:
        asyncio.create_task(encolar_para_todos(app, "audio", {"file_id": msg.audio.file_id, "caption": formatear_caption(nombre, texto)}, user_id, reply_map, origen_key))
    elif msg.animation:
        asyncio.create_task(encolar_para_todos(app, "animation", {"file_id": msg.animation.file_id, "caption": formatear_caption(nombre, texto)}, user_id, reply_map, origen_key))
    elif msg.voice:
        asyncio.create_task(encolar_para_todos(app, "voice", {"file_id": msg.voice.file_id, "caption": formatear_caption(nombre, texto)}, user_id, reply_map, origen_key))
    elif msg.video_note:
        asyncio.create_task(encolar_para_todos(app, "text", formatear_caption(nombre, None), user_id, reply_map, None))
        asyncio.create_task(encolar_para_todos(app, "video_note", {"file_id": msg.video_note.file_id}, user_id, None, origen_key))
    elif msg.sticker:
        asyncio.create_task(encolar_para_todos(app, "text", formatear_caption(nombre, None), user_id, reply_map, None))
        asyncio.create_task(encolar_para_todos(app, "sticker", {"file_id": msg.sticker.file_id}, user_id, None, origen_key))
    elif msg.text:
        asyncio.create_task(encolar_para_todos(app, "text", formatear_caption(nombre, msg.text), user_id, reply_map, origen_key))


async def cmd_aportes(update, context):
    user_id = update.effective_user.id
    usuario = get_usuario(user_id)

    if usuario["estado"] != "aceptado":
        await update.message.reply_text("Aún no formas parte del chat grupal.")
        return

    if usuario.get("excepcion"):
        await update.message.reply_text("⭐ Cuentas con una excepción activa. No requieres enviar aportes obligatorios.")
        return

    conf = get_config()
    meta = conf.get("meta_multimedia", 0)
    dias = conf.get("dias_ventana", 1)
    if not meta:
        await update.message.reply_text("📊 No hay una meta de multimedia configurada actualmente.")
        return

    contador = usuario.get("multimedia_contador", 0)
    activo = usuario.get("multimedia_activo", True)
    inicio = usuario.get("multimedia_ventana_inicio") or datetime.utcnow()
    restante = (inicio + timedelta(days=dias)) - datetime.utcnow()

    if restante.total_seconds() > 0:
        horas = int(restante.total_seconds() // 3600)
        tiempo_txt = f"~{horas} h"
    else:
        tiempo_txt = "menos de 1 h"

    estado_txt = "🟢 Activo (recibiendo mensajes)" if activo else "🔴 Inactivo (no recibirás mensajes hasta cumplir tu meta)"

    await update.message.reply_text(
        "📊 *Tus aportes*\n\n"
        f"Multimedia enviada: {contador}/{meta}\n"
        f"Ventana configurada: {dias} día(s)\n"
        f"Tiempo restante: {tiempo_txt}\n"
        f"Estado: {estado_txt}",
        parse_mode="Markdown",
    )

# ---------------------------------------------------------------
# PANEL DE ADMINISTRADOR
# ---------------------------------------------------------------
def teclado_admin(conf):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"👥 Límite de usuarios: {conf['limite_usuarios']}", callback_data="adm_limite")],
        [InlineKeyboardButton("🔑 Establecer contraseña", callback_data="adm_password")],
        [InlineKeyboardButton("💬 Mensaje de bienvenida", callback_data="adm_bienvenida")],
        [InlineKeyboardButton("👥 Unir grupo", callback_data="adm_grupo")],
        [InlineKeyboardButton(f"🎯 Meta multimedia: {conf.get('meta_multimedia', 0) or 'desactivada'}", callback_data="adm_meta")],
        [InlineKeyboardButton(f"⏳ Días ventana: {conf.get('dias_ventana', 1)} día(s)", callback_data="adm_dias")],
        [InlineKeyboardButton("⛔ Banear por ID", callback_data="adm_banear")],
        [InlineKeyboardButton("⭐ Excepciones por ID", callback_data="adm_excepcion")],
    ])


async def texto_panel(conf):
    total = await contar_aceptados()
    meta = conf.get("meta_multimedia", 0)
    dias = conf.get("dias_ventana", 1)
    return (
        "🛠️ *Panel de Administrador*\n\n"
        f"👥 Usuarios aceptados: {total}/{conf['limite_usuarios']}\n"
        f"🔑 Contraseña: {'configurada' if conf.get('contraseña') else 'sin configurar'}\n"
        f"👥 Grupo requerido: {conf.get('grupo_requerido_nombre') or 'ninguno'}\n"
        f"🎯 Meta multimedia: {meta if meta else 'desactivada'} (cada {dias} día/s)"
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
    elif data == "adm_bienvenida":
        admin_sesion["paso"] = "bienvenida"
        await query.edit_message_text("✏️ Envía el nuevo mensaje de bienvenida.")
    elif data == "adm_grupo":
        grupos = list(col_grupos.find({}))
        if not grupos:
            await query.edit_message_text(
                "⚠️ Aún no agrego el bot como *administrador* de ningún grupo.",
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
        await query.edit_message_text("✏️ Envía cuántas fotos/videos debe mandar cada usuario en la ventana de días.\nEnvía 0 para desactivar.")
    elif data == "adm_dias":
        admin_sesion["paso"] = "dias"
        await query.edit_message_text("✏️ Envía el número de días para la ventana de aportes (ejemplo: 1).")
    elif data == "adm_excepcion":
        admin_sesion["paso"] = "excepcion"
        await query.edit_message_text(
            "✏️ Envía las IDs para alternar su estado de Excepción (una por línea).\n"
            "Si la ID ya tenía excepción se le quitará, y si no la tenía se le activará."
        )


async def procesar_panel_admin(update, context):
    msg = update.effective_message
    if msg.text and msg.text.startswith("/"):
        return

    paso = admin_sesion.get("paso")
    if not paso:
        return

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
            await msg.reply_text(f"✅ Meta actualizada a {texto} foto(s)/video(s).")
        else:
            await msg.reply_text("⚠️ Envía solo un número.")
            return
    elif paso == "dias":
        if texto.isdigit() and int(texto) > 0:
            set_config_campo("dias_ventana", int(texto))
            await msg.reply_text(f"✅ Ventana de aportes actualizada a {texto} día(s).")
        else:
            await msg.reply_text("⚠️ Envía un número entero mayor a 0.")
            return
    elif paso == "excepcion":
        ids = [l.strip() for l in texto.splitlines() if l.strip().lstrip("-").isdigit()]
        res = []
        for uid in ids:
            u_id = int(uid)
            u = get_usuario(u_id)
            nuevo_estado = not u.get("excepcion", False)
            set_usuario_campo(u_id, "excepcion", nuevo_estado)
            if nuevo_estado:
                set_usuario_campo(u_id, "multimedia_activo", True)
                res.append(f"• ID {u_id}: ⭐ Excepción ACTIVADA")
            else:
                res.append(f"• ID {u_id}: Excepción DESACTIVADA")
        if ids:
            invalidar_cache_aceptados()
        await msg.reply_text("✅ Cambios de excepción realizados:\n" + ("\n".join(res) if res else "⚠️ Ninguna ID válida proporcionada."))

    admin_sesion["paso"] = None
    conf = get_config()
    await msg.reply_text(await texto_panel(conf), parse_mode="Markdown", reply_markup=teclado_admin(conf))


# ---------------------------------------------------------------
# SEGUIMIENTO DE GRUPOS
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
# KEEP-ALIVE HTTP
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
        connection_pool_size=40,
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

    # Revisión de metas cada 3 horas (10800 segundos)
    app.job_queue.run_repeating(revisar_metas_multimedia, interval=10800, first=30)

    logging.info("🤖 Bot de grupo anónimo iniciado...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
