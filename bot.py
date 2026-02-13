import os
import logging
import threading
import hashlib
from http.server import BaseHTTPRequestHandler, HTTPServer
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

# Configuración de logs
logging.basicConfig(level=logging.INFO)

# --- SERVIDOR WEB PARA RENDER ---
class SimpleHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200); self.end_headers()
        self.wfile.write(b"Bot Anti-Repetidos Running")

def run_web_server():
    port = int(os.environ.get("PORT", 8080))
    HTTPServer(('0.0.0.0', port), SimpleHandler).serve_forever()

# --- CONFIGURACIÓN Y MEMORIA ---
MY_ID = int(os.getenv("MY_ID", 0))
# Diccionario para saber en qué grupos está activado: {chat_id: True/False}
active_groups = {}
# Diccionario para guardar huellas digitales: {chat_id: set([hash1, hash2...])}
hashes_guardados = {}

# --- FUNCIONES DE SEGURIDAD ---
def es_dueno(update: Update):
    return update.message.from_user.id == MY_ID

# --- LÓGICA ANTI-REPETIDOS ---
async def detectar_repetido(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    
    # Solo actuar si el bot está /on en este grupo
    if not active_groups.get(chat_id):
        return

    # Obtener el file_id de la imagen o video
    archivo = None
    if update.message.photo:
        archivo = update.message.photo[-1] # La mejor calidad
    elif update.message.video:
        archivo = update.message.video

    if archivo:
        # Usamos el file_unique_id que Telegram da para identificar el mismo archivo
        # aunque se reenvíe o se mande de nuevo.
        file_unique_id = archivo.file_unique_id

        if chat_id not in hashes_guardados:
            hashes_guardados[chat_id] = set()

        if file_unique_id in hashes_guardados[chat_id]:
            try:
                await update.message.delete()
                # Opcional: Avisar que se borró por repetido
                # await context.bot.send_message(chat_id, "🚫 Imagen/Video repetido eliminado.")
            except Exception as e:
                logging.error(f"No pude borrar: {e}")
        else:
            hashes_guardados[chat_id].add(file_unique_id)

# --- COMANDOS ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if es_dueno(update):
        await update.message.reply_text("Bot Anti-Repetidos activo. Úsame en grupos con /on.")

async def on(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if es_dueno(update):
        active_groups[update.effective_chat.id] = True
        await update.message.reply_text("🛡️ Anti-Repetidos: ACTIVADO.")

async def off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if es_dueno(update):
        active_groups[update.effective_chat.id] = False
        await update.message.reply_text("🔓 Anti-Repetidos: DESACTIVADO.")

def main():
    TOKEN = os.getenv("TOKEN")
    threading.Thread(target=run_web_server, daemon=True).start()
    
    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("on", on))
    app.add_handler(CommandHandler("off", off))
    
    # Escuchar todas las fotos y videos
    app.add_handler(MessageHandler(filters.PHOTO | filters.VIDEO, detectar_repetido))

    app.run_polling()

if __name__ == '__main__':
    main()
      
