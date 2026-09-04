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
"""

import os
import re
import logging
import asyncio
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from pymongo import MongoClient
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    InputMediaPhoto, InputMediaVideo, InputMediaDocument, InputMediaAudio,
)
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

REGEX_LINK = re.compile(r"(https?://\S+|www\.\S+|t\.me/\S+|telegram\.me/\S+)", re.IGNORECASE)

# Sesión del owner (en memoria, no necesita persistir en Mongo)
admin_sesion = {"activo": False, "paso": None}
# paso puede ser: None | "limite" | "password" | "bienvenida" | "baneo"

# Buffer temporal para armar álbumes (media_group_id -> lista de mensajes)
album_buffer = {}

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
    return conf


def set_config_campo(campo, valor):
    col_config.update_one({"_id": "config"}, {"$set": {campo: valor}}, upsert=True)


def get_usuario(user_id):
    u = col_usuarios.find_one({"_id": user_id}) or {"_id": user_id}
    u.setdefault("nombre", "")
    u.setdefault("estado", "nuevo")  # nuevo | pendiente_grupo | pendiente_password | aceptado | baneado
    u.setdefault("baneado", False)
    return u


def set_usuario_campo(user_id, campo, valor):
    col_usuarios.update_one({"_id": user_id}, {"$set": {campo: valor}}, upsert=True)


def contar_aceptados():
    return col_usuarios.count_documents({"estado": "aceptado", "baneado": {"$ne": True}})


def obtener_aceptados(excluir=None):
    ids = [u["_id"] for u in col_usuarios.find(
        {"estado": "aceptado", "baneado": {"$ne": True}}, {"_id": 1}
    )]
    if excluir is not None and excluir in ids:
        ids.remove(excluir)
    return ids


def obtener_nombre(user):
    return user.first_name or user.username or f"Usuario{user.id}"


def contiene_enlace(texto):
    return bool(texto and REGEX_LINK.search(texto))


def formatear_texto(nombre, texto):
    # Texto plano: nombre y mensaje en la misma línea -> "Fernanda: hola"
    return f"{nombre}: {texto}"


def formatear_caption(nombre, caption):
    # Multimedia: nombre en su propia línea, separado del texto -> "Fernanda:\nmensaje"
    if caption:
        return f"{nombre}:\n{caption}"
    return f"{nombre}:"


# ---------------------------------------------------------------
# COLA DE ENVÍO CON RITMO ADAPTATIVO (respeta límites de Telegram)
# ---------------------------------------------------------------
cola_envio = asyncio.Queue()


def calcular_delay(num_usuarios):
    """Entre más usuarios aceptados haya, más lento se reenvía, para
    nunca acercarse al límite de Telegram (~30 msj/seg a chats distintos)."""
    if num_usuarios <= 50:
        return 0.05   # ~20 msj/s
    elif num_usuarios <= 200:
        return 0.08   # ~12 msj/s
    elif num_usuarios <= 500:
        return 0.15   # ~6.5 msj/s
    elif num_usuarios <= 1000:
        return 0.25   # ~4 msj/s
    else:
        return 0.4    # ~2.5 msj/s


async def worker_envio(app):
    """Procesa la cola de reenvíos uno por uno, en orden, con pausa entre cada uno."""
    while True:
        chat_id, tipo, payload = await cola_envio.get()
        try:
            if tipo == "text":
                await app.bot.send_message(chat_id=chat_id, text=payload)
            elif tipo == "photo":
                await app.bot.send_photo(chat_id=chat_id, photo=payload["file_id"], caption=payload.get("caption"))
            elif tipo == "video":
                await app.bot.send_video(chat_id=chat_id, video=payload["file_id"], caption=payload.get("caption"))
            elif tipo == "document":
                await app.bot.send_document(chat_id=chat_id, document=payload["file_id"], caption=payload.get("caption"))
            elif tipo == "audio":
                await app.bot.send_audio(chat_id=chat_id, audio=payload["file_id"], caption=payload.get("caption"))
            elif tipo == "animation":
                await app.bot.send_animation(chat_id=chat_id, animation=payload["file_id"], caption=payload.get("caption"))
            elif tipo == "voice":
                await app.bot.send_voice(chat_id=chat_id, voice=payload["file_id"], caption=payload.get("caption"))
            elif tipo == "video_note":
                await app.bot.send_video_note(chat_id=chat_id, video_note=payload["file_id"])
            elif tipo == "sticker":
                await app.bot.send_sticker(chat_id=chat_id, sticker=payload["file_id"])
            elif tipo == "album":
                await app.bot.send_media_group(chat_id=chat_id, media=payload)
        except Forbidden:
            # El usuario bloqueó al bot: dejamos de intentar mandarle (no lo baneamos, solo lo marcamos)
            set_usuario_campo(chat_id, "bloqueo_bot", True)
        except Exception as e:
            logging.warning(f"Error reenviando a {chat_id}: {e}")

        cola_envio.task_done()
        await asyncio.sleep(calcular_delay(contar_aceptados()))


def encolar_para_todos(tipo, payload, excluir_id):
    for chat_id in obtener_aceptados(excluir=excluir_id):
        cola_envio.put_nowait((chat_id, tipo, payload))


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

    if contar_aceptados() >= conf["limite_usuarios"]:
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

    set_usuario_campo(user_id, "estado", "aceptado")
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
        set_usuario_campo(user_id, "estado", "aceptado")
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
            set_usuario_campo(user_id, "estado", "aceptado")
            await msg.reply_text("✅ ¡Contraseña correcta! Ya puedes escribir en el chat grupal.")
        else:
            await msg.reply_text("❌ Contraseña incorrecta. Intenta de nuevo.")
        return

    # estado "nuevo" escribiendo directo sin /start
    await avanzar_onboarding(update, context, conf)


# ---------------------------------------------------------------
# REENVÍO DE MENSAJES ENTRE USUARIOS ACEPTADOS
# ---------------------------------------------------------------
async def enviar_item(user_id, nombre, tipo, file_id, texto_o_caption):
    caption = formatear_caption(nombre, texto_o_caption)
    encolar_para_todos(tipo, {"file_id": file_id, "caption": caption}, user_id)


async def procesar_album(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data
    gid, nombre, excluir = data["gid"], data["nombre"], data["excluir"]
    mensajes = album_buffer.pop(gid, [])
    if not mensajes:
        return
    mensajes.sort(key=lambda m: m.message_id)

    caption_original = next((m.caption for m in mensajes if m.caption), None)

    if contiene_enlace(caption_original) and get_config().get("ignorar_enlaces"):
        return

    caption = formatear_caption(nombre, caption_original)

    media_list = []
    for i, m in enumerate(mensajes):
        cap = caption if i == 0 else None
        if m.photo:
            media_list.append(InputMediaPhoto(m.photo[-1].file_id, caption=cap))
        elif m.video:
            media_list.append(InputMediaVideo(m.video.file_id, caption=cap))
        elif m.document:
            media_list.append(InputMediaDocument(m.document.file_id, caption=cap))
        elif m.audio:
            media_list.append(InputMediaAudio(m.audio.file_id, caption=cap))

    if len(media_list) < 2:
        return

    encolar_para_todos("album", media_list, excluir)


async def manejar_mensaje(update, context):
    user = update.effective_user
    msg = update.effective_message
    user_id = user.id

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

    # Álbum (varias fotos/videos mandadas juntas)
    if msg.media_group_id:
        gid = msg.media_group_id
        album_buffer.setdefault(gid, []).append(msg)
        if len(album_buffer[gid]) == 1:
            context.job_queue.run_once(
                procesar_album, when=1.5,
                data={"gid": gid, "nombre": nombre, "excluir": user_id},
            )
        return

    texto = msg.text or msg.caption
    conf = get_config()
    if contiene_enlace(texto) and conf.get("ignorar_enlaces"):
        return

    if msg.photo:
        await enviar_item(user_id, nombre, "photo", msg.photo[-1].file_id, texto)
    elif msg.video:
        await enviar_item(user_id, nombre, "video", msg.video.file_id, texto)
    elif msg.document:
        await enviar_item(user_id, nombre, "document", msg.document.file_id, texto)
    elif msg.audio:
        await enviar_item(user_id, nombre, "audio", msg.audio.file_id, texto)
    elif msg.animation:
        await enviar_item(user_id, nombre, "animation", msg.animation.file_id, texto)
    elif msg.voice:
        await enviar_item(user_id, nombre, "voice", msg.voice.file_id, texto)
    elif msg.video_note:
        encolar_para_todos("text", formatear_caption(nombre, None), user_id)
        encolar_para_todos("video_note", {"file_id": msg.video_note.file_id}, user_id)
    elif msg.sticker:
        encolar_para_todos("text", formatear_caption(nombre, None), user_id)
        encolar_para_todos("sticker", {"file_id": msg.sticker.file_id}, user_id)
    elif msg.text:
        encolar_para_todos("text", formatear_texto(nombre, msg.text), user_id)


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
    ])


def texto_panel(conf):
    total = contar_aceptados()
    return (
        "🛠️ *Panel de Administrador*\n\n"
        f"👥 Usuarios aceptados: {total}/{conf['limite_usuarios']}\n"
        f"🔑 Contraseña: {'configurada' if conf.get('contraseña') else 'sin configurar'}\n"
        f"👥 Grupo requerido: {conf.get('grupo_requerido_nombre') or 'ninguno'}"
    )


async def cmd_admin_on(update, context):
    if update.effective_user.id != OWNER_ID:
        return
    admin_sesion["activo"] = True
    admin_sesion["paso"] = None
    conf = get_config()
    await update.message.reply_text(texto_panel(conf), parse_mode="Markdown", reply_markup=teclado_admin(conf))


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
        await query.edit_message_text(texto_panel(conf), parse_mode="Markdown", reply_markup=teclado_admin(conf))
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
        await query.edit_message_text(texto_panel(conf), parse_mode="Markdown", reply_markup=teclado_admin(conf))
    elif data == "adm_banear":
        admin_sesion["paso"] = "baneo"
        await query.edit_message_text("✏️ Envía las IDs a banear, una por línea.")


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
        await msg.reply_text(f"✅ {len(ids)} usuario(s) baneado(s).")

    admin_sesion["paso"] = None
    conf = get_config()
    await msg.reply_text(texto_panel(conf), parse_mode="Markdown", reply_markup=teclado_admin(conf))


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

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler(CMD_ADMIN_ON, cmd_admin_on))
    app.add_handler(CommandHandler(CMD_ADMIN_OFF, cmd_admin_off))
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CallbackQueryHandler(cb_check_grupo, pattern="^check_grupo$"))
    app.add_handler(CallbackQueryHandler(cb_admin, pattern="^adm_"))
    app.add_handler(ChatMemberHandler(on_bot_chat_member_update, ChatMemberHandler.MY_CHAT_MEMBER))
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND, manejar_mensaje))

    app.job_queue.run_once(lambda ctx: asyncio.create_task(worker_envio(app)), when=1)

    logging.info("🤖 Bot de grupo anónimo iniciado...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
