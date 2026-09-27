import os
import sqlite3
import threading
import time
from flask import Flask, jsonify, request
import requests

app = Flask(__name__)

DB_PATH = '/data/webhooks.db'
EXTERNAL_URL = os.getenv(
    'EXTERNAL_WEBHOOK_URL', 'https://tu-servidor-externo.example.com/webhook'
)
RETRY_INTERVAL = int(os.getenv('RETRY_INTERVAL', 300))
MAX_RETRIES = int(os.getenv('MAX_RETRIES', 10))
MAX_RECORDS = int(os.getenv('MAX_RECORDS', 1000))

# Evento de sincronización para despertar al hilo inmediatamente al recibir un webhook
worker_trigger = threading.Event()


def init_db():
  os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
  conn = sqlite3.connect(DB_PATH)
  cursor = conn.cursor()
  cursor.execute('''
        CREATE TABLE IF NOT EXISTS webhooks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            payload TEXT NOT NULL,
            content_type TEXT DEFAULT 'application/json',
            status TEXT DEFAULT 'pending',
            retry_count INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
  conn.commit()
  conn.close()


def enforce_limits():
  try:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM webhooks')
    total_records = cursor.fetchone()[0]

    if total_records > MAX_RECORDS:
      excess = total_records - MAX_RECORDS
      cursor.execute(
          """
                DELETE FROM webhooks 
                WHERE id IN (
                    SELECT id FROM webhooks 
                    WHERE status != 'pending' 
                    ORDER BY id ASC 
                    LIMIT ?
                )
            """,
          (excess,),
      )
      deleted = cursor.rowcount
      conn.commit()
      if deleted > 0:
        print(
            '[Mantenimiento] Se eliminaron '
            f'{deleted} registros excedentes (Límite: {MAX_RECORDS}).'
        )
    conn.close()
  except Exception as e:
    print(f'[Error en Mantenimiento] {e}')


def retry_worker():
  while True:
    try:
      conn = sqlite3.connect(DB_PATH)
      cursor = conn.cursor()
      cursor.execute(
          'SELECT id, payload, content_type, retry_count FROM webhooks WHERE'
          " status = 'pending' ORDER BY id ASC LIMIT 5"
      )
      rows = cursor.fetchall()
      conn.close()

      if rows:
        # Procesamos los pendientes de inmediato
        for row in rows:
          wh_id, payload, content_type, retry_count = row
          try:
            print(
                '[Envío/Reintento] Enviando webhook ID '
                f'{wh_id} (Intento {retry_count + 1}/{MAX_RETRIES})...'
            )
            response = requests.post(
                EXTERNAL_URL,
                data=payload.encode('utf-8'),
                headers={'Content-Type': content_type},
                timeout=10,
            )

            if response.status_code == 200:
              print(
                  f'[Éxito] Webhook ID {wh_id} entregado correctamente (HTTP'
                  ' 200).'
              )
              conn = sqlite3.connect(DB_PATH)
              cursor = conn.cursor()
              cursor.execute(
                  "UPDATE webhooks SET status = 'sent' WHERE id = ?", (wh_id,)
              )
              conn.commit()
              conn.close()
            else:
              new_retry_count = retry_count + 1
              conn = sqlite3.connect(DB_PATH)
              cursor = conn.cursor()
              if new_retry_count >= MAX_RETRIES:
                print(
                    '[Descartado] Webhook ID '
                    f'{wh_id} superó el límite de {MAX_RETRIES} reintentos (HTTP'
                    f' {response.status_code}). Eliminando.'
                )
                cursor.execute('DELETE FROM webhooks WHERE id = ?', (wh_id,))
              else:
                print(
                    '[Aviso] Destino respondió HTTP'
                    f' {response.status_code}. Reintento'
                    f' {new_retry_count}/{MAX_RETRIES}.'
                )
                cursor.execute(
                    'UPDATE webhooks SET retry_count = ? WHERE id = ?',
                    (new_retry_count, wh_id),
                )
              conn.commit()
              conn.close()

          except Exception as e:
            new_retry_count = retry_count + 1
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            if new_retry_count >= MAX_RETRIES:
              print(
                  '[Descartado] Webhook ID '
                  f'{wh_id} superó el límite de {MAX_RETRIES} reintentos por'
                  f' error de red: {e}. Eliminando.'
              )
              cursor.execute('DELETE FROM webhooks WHERE id = ?', (wh_id,))
            else:
              print(
                  '[Fallo de Red] Webhook ID'
                  f' {wh_id}: {e}. Reintento {new_retry_count}/{MAX_RETRIES}.'
              )
              cursor.execute(
                  'UPDATE webhooks SET retry_count = ? WHERE id = ?',
                  (new_retry_count, wh_id),
              )
            conn.commit()
            conn.close()

        enforce_limits()
        # Si aún quedan más registros pendientes en la cola, continuamos enseguida sin dormir
        continue

      # Limpieza de registros antiguos periódicamente
      enforce_limits()

    except Exception as e:
      print(f'[Error en Worker] {e}')

    # Limpiamos el evento previo y esperamos a que llegue un nuevo webhook O a que pase el intervalo
    worker_trigger.clear()
    worker_trigger.wait(timeout=RETRY_INTERVAL)


@app.route('/webhook', methods=['POST'])
def receive_webhook():
  try:
    data = request.get_data(as_text=True)
    if not data:
      return jsonify({'error': 'Payload vacío'}), 400

    content_type = request.headers.get('Content-Type', 'application/json')

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        'INSERT INTO webhooks (payload, content_type, status, retry_count)'
        " VALUES (?, ?, 'pending', 0)",
        (data, content_type),
    )
    conn.commit()
    conn.close()

    print(f'[Recibido] Webhook con tipo {content_type} encolado exitosamente.')

    # ¡Despertamos al hilo procesador de inmediato!
    worker_trigger.set()

    return jsonify({'status': 'queued'}), 200
  except Exception as e:
    print(f'[Error al recibir] {e}')
    return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
  init_db()
  t = threading.Thread(target=retry_worker, daemon=True)
  t.start()
  app.run(host='0.0.0.0', port=5000)
