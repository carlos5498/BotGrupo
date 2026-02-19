import os
import logging
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
from pymongo import MongoClient

# --- CONFIGURACIÓN ---
logging.basicConfig(level=logging.INFO)
MY_ID = int(os.getenv("MY_ID", 0))
MONGO_URL = os.getenv("MONGO_URL")
TOKEN = os.getenv("TOKEN")

# --- CONEXIÓN A MONGODB ---
client = MongoClient(MONGO_URL)
db = client['bot_anti_repetidos']
coleccion_hashes = db['hashes_archivos']
coleccion_config = db['config_grupos']

# --- SERVIDOR WEB (Para que Render no se apague) ---
class SimpleHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200); self.end_headers()
        self.wfile.write(b"Bot Anti-Repetidos activo")

def run_web_server():
    port = int(os.environ.get("PORT", 8080))
    HTTPServer(('0.0.0.0', port), SimpleHandler).serve_forever()

# --- INTERFAZ DE BOTONES ---
def get_settings_keyboard(chat_id):
    # Buscamos config o valores por defecto
    conf = coleccion_config.find_one({"chat_id": chat_id}) or {}
    
    # Si no existe, por defecto borramos fotos y videos
    fotos = conf.get("fotos", True)
    videos = conf.get("videos", True)
    docs = conf.get("docs", False)
    
    keyboard = [
        [InlineKeyboardButton(f"Imágenes {'✅' if fotos else '❌'}", callback_data=f"set_fotos_{chat_id}")],
        [InlineKeyboardButton(f"Videos {'✅' if videos else '❌'}", callback_data=f"set_videos_{chat_id}")],
        [InlineKeyboardButton(f"Archivos {'✅' if docs else '❌'}", callback_data=f"set_docs_{chat_id}")],
        [InlineKeyboardButton("💾 Cerrar Menú", callback_data="cerrar_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)

# --- COMANDOS ---
async def config_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Solo el dueño o admins pueden configurar
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    
    # Verificar si es admin o el dueño configurado en Render
    member = await context.bot.get_chat_member(chat_id, user_id)
    if member.status not in ["administrator", "creator"] and user_id != MY_ID:
        return

    await update.message.reply_text(
        "⚙️ **Configuración Anti-Repetidos**\nSelecciona qué archivos deseas que borre automáticamente:",
        reply_markup=get_settings_keyboard(chat_id)
    )

async def botones_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    await query.answer()

    if data == "cerrar_menu":
        await query.message.delete()
        return

    # Formato: set_tipo_chatid
    _, tipo, chat_id = data.split("_")
    chat_id = int(chat_id)

    # Cambiar valor en BD
    conf = coleccion_config.find_one({"chat_id": chat_id}) or {"chat_id": chat_id, "fotos": True, "videos": True, "docs": False}
    conf[tipo] = not conf.get(tipo, True)
    
    coleccion_config.update_one({"chat_id": chat_id}, {"$set": conf}, upsert=True)
    
    # Actualizar botones sin mandar mensaje nuevo
    await query.edit_message_reply_markup(reply_markup=get_settings_keyboard(chat_id))

# --- LÓGICA DE LIMPIEZA ---
async def detectar_repetido(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message: return
    chat_id = update.effective_chat.id
    
    # Obtener config
    conf = coleccion_config.find_one({"chat_id": chat_id}) or {"fotos": True, "videos": True, "docs": False}

    archivo = None
    if update.message.photo and conf.get("fotos"):
        archivo = update.message.photo[-1]
    elif update.message.video and conf.get("videos"):
        archivo = update.message.video
    elif update.message.document and conf.get("docs"):
        archivo = update.message.document

    if archivo:
        file_hash = archivo.file_unique_id
        # Buscar en este grupo específico
        existe = coleccion_hashes.find_one({"chat_id": chat_id, "file_hash": file_hash})

        if existe:
            try:
                await update.message.delete()
            except Exception as e:
                logging.error(f"Error al borrar: {e}")
        else:
            coleccion_hashes.insert_one({"chat_id": chat_id, "file_hash": file_hash})

async def limpiar_historial(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id == MY_ID:
        chat_id = update.effective_chat.id
        coleccion_hashes.delete_many({"chat_id": chat_id})
        await update.message.reply_text("🗑️ Historial de repetidos limpio para este grupo.")

# --- MAIN ---
def main():
    threading.Thread(target=run_web_server, daemon=True).start()
    
    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("config", config_command))
    app.add_handler(CommandHandler("limpiar", limpiar_historial))
    app.add_handler(CallbackQueryHandler(botones_callback))
    
    # Filtro para fotos, videos y documentos (archivos)
    app.add_handler(MessageHandler(filters.PHOTO | filters.VIDEO | filters.Document.ALL, detectar_repetido))

    logging.info("Bot iniciado...")
    app.run_polling()

if __name__ == '__main__':
    main()
    
