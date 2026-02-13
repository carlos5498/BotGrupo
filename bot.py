import os
import logging
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from pymongo import MongoClient

# --- CONFIGURACIÓN ---
logging.basicConfig(level=logging.INFO)
MY_ID = int(os.getenv("MY_ID", 0))
MONGO_URL = os.getenv("MONGO_URL")

# --- CONEXIÓN A MONGODB ---
client = MongoClient(MONGO_URL)
db = client['bot_anti_repetidos']
coleccion_hashes = db['hashes_archivos']
coleccion_config = db['config_grupos']

# --- SERVIDOR WEB (Para Render) ---
class SimpleHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200); self.end_headers()
        self.wfile.write(b"Bot Anti-Repetidos con MongoDB Running")

def run_web_server():
    port = int(os.environ.get("PORT", 8080))
    HTTPServer(('0.0.0.0', port), SimpleHandler).serve_forever()

# --- FUNCIONES DE SEGURIDAD ---
def es_dueno(update: Update):
    return update.message.from_user.id == MY_ID

# --- LÓGICA ANTI-REPETIDOS ---
async def detectar_repetido(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    
    # Verificar en MongoDB si el grupo está activo
    config = coleccion_config.find_one({"chat_id": chat_id})
    if not config or not config.get("activo"):
        return

    archivo = None
    if update.message.photo:
        archivo = update.message.photo[-1]
    elif update.message.video:
        archivo = update.message.video

    if archivo:
        file_unique_id = archivo.file_unique_id

        # Buscar si el hash ya existe en este grupo en MongoDB
        existe = coleccion_hashes.find_one({"chat_id": chat_id, "file_hash": file_unique_id})

        if existe:
            try:
                await update.message.delete()
                logging.info(f"Repetido borrado en {chat_id}")
            except Exception as e:
                logging.error(f"Error al borrar: {e}")
        else:
            # Guardar el nuevo hash en MongoDB
            coleccion_hashes.insert_one({
                "chat_id": chat_id,
                "file_hash": file_unique_id
            })

# --- COMANDOS ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if es_dueno(update):
        await update.message.reply_text("Bot Anti-Repetidos con Memoria en la Nube activo.")

async def on(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if es_dueno(update):
        chat_id = update.effective_chat.id
        coleccion_config.update_one(
            {"chat_id": chat_id},
            {"$set": {"activo": True}},
            upsert=True
        )
        await update.message.reply_text("🛡️ Anti-Repetidos: ACTIVADO (Guardado en Nube).")

async def off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if es_dueno(update):
        chat_id = update.effective_chat.id
        coleccion_config.update_one(
            {"chat_id": chat_id},
            {"$set": {"activo": False}},
            upsert=True
        )
        await update.message.reply_text("🔓 Anti-Repetidos: DESACTIVADO.")

async def limpiar_historial(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if es_dueno(update):
        chat_id = update.effective_chat.id
        coleccion_hashes.delete_many({"chat_id": chat_id})
        await update.message.reply_text("🗑️ Historial de este grupo borrado de la base de datos.")

def main():
    TOKEN = os.getenv("TOKEN")
    threading.Thread(target=run_web_server, daemon=True).start()
    
    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("on", on))
    app.add_handler(CommandHandler("off", off))
    app.add_handler(CommandHandler("limpiar", limpiar_historial))
    
    app.add_handler(MessageHandler(filters.PHOTO | filters.VIDEO, detectar_repetido))

    app.run_polling()

if __name__ == '__main__':
    main()
    
