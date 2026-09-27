import os
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone

import requests
from flask import Flask, jsonify, request


app = Flask(__name__)

DB_PATH = os.getenv('DB_PATH', '/data/webhooks.db')

EXTERNAL_URL = os.getenv(
    'EXTERNAL_WEBHOOK_URL',
    'https://tu-servidor-externo.example.com/webhook',
)

# Tiempo base entre reintentos.
RETRY_INTERVAL = int(os.getenv('RETRY_INTERVAL', 300))

# Cantidad máxima de intentos por webhook.
MAX_RETRIES = int(os.getenv('MAX_RETRIES', 10))

# Máxima cantidad de registros almacenados en SQLite.
MAX_RECORDS = int(os.getenv('MAX_RECORDS', 1000))

# Intervalo mínimo entre requests al servidor externo.
RATE_LIMIT_INTERVAL = float(os.getenv('RATE_LIMIT_INTERVAL', 1))

# Timeout HTTP para el servidor externo.
REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', 10))

# Tamaño máximo del payload recibido: 1 MB por defecto.
MAX_PAYLOAD_SIZE = int(
    os.getenv('MAX_PAYLOAD_SIZE', 1024 * 1024)
)

# Cuánto tiempo conservar los webhooks enviados.
SENT_RETENTION_SECONDS = int(
    os.getenv('SENT_RETENTION_SECONDS', 86400)
)


app.config['MAX_CONTENT_LENGTH'] = MAX_PAYLOAD_SIZE


worker_trigger = threading.Event()


# ---------------------------------------------------------------------------
# DATABASE
# ---------------------------------------------------------------------------

def get_connection():
    conn = sqlite3.connect(
        DB_PATH,
        timeout=30,
    )

    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA busy_timeout=30000')

    return conn


def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        '''
        CREATE TABLE IF NOT EXISTS webhooks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            payload TEXT NOT NULL,
            content_type TEXT NOT NULL DEFAULT 'application/json',
            status TEXT NOT NULL DEFAULT 'pending',
            retry_count INTEGER NOT NULL DEFAULT 0,
            next_retry_at TEXT,
            locked_at TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            sent_at TEXT
        )
        '''
    )

    cursor.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_webhooks_pending
        ON webhooks(status, next_retry_at, id)
        '''
    )

    cursor.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_webhooks_created
        ON webhooks(created_at)
        '''
    )

    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def utc_now():
    return datetime.now(timezone.utc)


def utc_now_iso():
    return utc_now().isoformat()


def calculate_retry_delay(retry_count):
    """
    Exponential backoff:

    intento 1 -> RETRY_INTERVAL
    intento 2 -> RETRY_INTERVAL * 2
    intento 3 -> RETRY_INTERVAL * 4
    ...

    Se limita a 1 hora para evitar esperas excesivamente largas.
    """
    delay = RETRY_INTERVAL * (2 ** max(0, retry_count - 1))

    return min(delay, 3600)


def is_retryable_status(status_code):
    """
    Determina si una respuesta HTTP debería reintentarse.
    """

    # Rate limiting.
    if status_code == 429:
        return True

    # Errores temporales del servidor.
    if 500 <= status_code <= 599:
        return True

    # Otros errores 4xx se consideran definitivos.
    return False


def get_retry_after(response):
    """
    Lee Retry-After si el servidor externo lo proporciona.

    Soporta:
      Retry-After: 60

    Si no existe o no es válido, devuelve None.
    """

    value = response.headers.get('Retry-After')

    if not value:
        return None

    try:
        seconds = int(value)

        if seconds < 0:
            return None

        return seconds

    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# QUEUE MANAGEMENT
# ---------------------------------------------------------------------------

def claim_next_webhook():
    """
    Reclama atómicamente el siguiente webhook disponible.

    Esto evita que dos workers puedan procesar simultáneamente
    el mismo registro.
    """

    conn = get_connection()

    try:
        cursor = conn.cursor()

        now = utc_now_iso()

        cursor.execute(
            '''
            SELECT id, payload, content_type, retry_count
            FROM webhooks
            WHERE status = 'pending'
              AND (
                    next_retry_at IS NULL
                    OR next_retry_at <= ?
                  )
            ORDER BY id ASC
            LIMIT 1
            ''',
            (now,),
        )

        row = cursor.fetchone()

        if not row:
            return None

        webhook_id, payload, content_type, retry_count = row

        cursor.execute(
            '''
            UPDATE webhooks
            SET status = 'processing',
                locked_at = ?
            WHERE id = ?
              AND status = 'pending'
            ''',
            (now, webhook_id),
        )

        if cursor.rowcount != 1:
            conn.rollback()
            return None

        conn.commit()

        return (
            webhook_id,
            payload,
            content_type,
            retry_count,
        )

    finally:
        conn.close()


def mark_as_sent(webhook_id):
    conn = get_connection()

    try:
        cursor = conn.cursor()

        cursor.execute(
            '''
            UPDATE webhooks
            SET status = 'sent',
                sent_at = ?,
                locked_at = NULL
            WHERE id = ?
            ''',
            (utc_now_iso(), webhook_id),
        )

        conn.commit()

    finally:
        conn.close()


def schedule_retry(webhook_id, retry_count, delay):
    next_retry = utc_now() + timedelta(seconds=delay)

    conn = get_connection()

    try:
        cursor = conn.cursor()

        cursor.execute(
            '''
            UPDATE webhooks
            SET status = 'pending',
                retry_count = ?,
                next_retry_at = ?,
                locked_at = NULL
            WHERE id = ?
            ''',
            (
                retry_count,
                next_retry.isoformat(),
                webhook_id,
            ),
        )

        conn.commit()

    finally:
        conn.close()


def delete_webhook(webhook_id):
    conn = get_connection()

    try:
        cursor = conn.cursor()

        cursor.execute(
            'DELETE FROM webhooks WHERE id = ?',
            (webhook_id,),
        )

        conn.commit()

    finally:
        conn.close()


# ---------------------------------------------------------------------------
# MAINTENANCE
# ---------------------------------------------------------------------------

def cleanup_sent_webhooks():
    """
    Elimina webhooks enviados que superaron el período de retención.
    """

    cutoff = utc_now() - timedelta(
        seconds=SENT_RETENTION_SECONDS
    )

    conn = get_connection()

    try:
        cursor = conn.cursor()

        cursor.execute(
            '''
            DELETE FROM webhooks
            WHERE status = 'sent'
              AND sent_at IS NOT NULL
              AND sent_at < ?
            ''',
            (cutoff.isoformat(),),
        )

        deleted = cursor.rowcount

        conn.commit()

        if deleted > 0:
            print(
                f'[Mantenimiento] Se eliminaron '
                f'{deleted} webhooks enviados antiguos.'
            )

    finally:
        conn.close()


def enforce_limits():
    """
    Mantiene controlado el tamaño de la base.

    Primero elimina registros enviados antiguos.

    Si todavía se supera MAX_RECORDS, elimina los registros
    enviados más antiguos.

    Los registros pendientes NO se eliminan automáticamente.
    Esto evita perder webhooks que todavía no fueron entregados.
    """

    try:
        cleanup_sent_webhooks()

        conn = get_connection()

        try:
            cursor = conn.cursor()

            cursor.execute(
                'SELECT COUNT(*) FROM webhooks'
            )

            total_records = cursor.fetchone()[0]

            if total_records <= MAX_RECORDS:
                return

            excess = total_records - MAX_RECORDS

            cursor.execute(
                '''
                DELETE FROM webhooks
                WHERE id IN (
                    SELECT id
                    FROM webhooks
                    WHERE status = 'sent'
                    ORDER BY id ASC
                    LIMIT ?
                )
                ''',
                (excess,),
            )

            deleted = cursor.rowcount

            conn.commit()

            if deleted > 0:
                print(
                    '[Mantenimiento] Se eliminaron '
                    f'{deleted} registros enviados excedentes '
                    f'(límite: {MAX_RECORDS}).'
                )

            # Si quedan más registros que el límite, significa que
            # todos o parte de ellos están pendientes.
            cursor.execute(
                'SELECT COUNT(*) FROM webhooks'
            )

            remaining = cursor.fetchone()[0]

            if remaining > MAX_RECORDS:
                print(
                    '[Advertencia] La cola supera MAX_RECORDS '
                    f'({remaining}/{MAX_RECORDS}) porque no hay '
                    'registros enviados disponibles para eliminar.'
                )

        finally:
            conn.close()

    except Exception as e:
        print(
            f'[Error en Mantenimiento] {e}'
        )


# ---------------------------------------------------------------------------
# WORKER
# ---------------------------------------------------------------------------

def send_webhook(
    webhook_id,
    payload,
    content_type,
    retry_count,
):
    """
    Intenta entregar un webhook.
    """

    current_retry = retry_count + 1

    print(
        '[Envío/Reintento] Enviando webhook ID '
        f'{webhook_id} '
        f'(intento {current_retry}/{MAX_RETRIES})...'
    )

    try:
        response = requests.post(
            EXTERNAL_URL,
            data=payload.encode('utf-8'),
            headers={
                'Content-Type': content_type,
            },
            timeout=REQUEST_TIMEOUT,
        )

        status_code = response.status_code

        # Cualquier 2xx se considera éxito.
        if 200 <= status_code < 300:
            print(
                f'[Éxito] Webhook ID {webhook_id} '
                f'entregado correctamente (HTTP {status_code}).'
            )

            return True, None

        # Error no reintentable.
        if not is_retryable_status(status_code):
            print(
                f'[Error definitivo] Webhook ID {webhook_id} '
                f'recibió HTTP {status_code}. '
                'No se reintentará.'
            )

            return False, 'permanent'

        # Intentar respetar Retry-After.
        retry_after = get_retry_after(response)

        if retry_after is not None:
            delay = retry_after

            print(
                f'[Rate Limit] El servidor indicó esperar '
                f'{delay} segundos antes de reintentar '
                f'el webhook ID {webhook_id}.'
            )

        else:
            delay = calculate_retry_delay(current_retry)

            print(
                f'[Aviso] El servidor respondió HTTP {status_code}. '
                f'Se reintentará en {delay} segundos.'
            )

        return False, delay

    except requests.RequestException as e:
        delay = calculate_retry_delay(current_retry)

        print(
            f'[Fallo de Red] Webhook ID {webhook_id}: '
            f'{e}. Se reintentará en {delay} segundos.'
        )

        return False, delay

    except Exception as e:
        delay = calculate_retry_delay(current_retry)

        print(
            f'[Error] Webhook ID {webhook_id}: '
            f'{e}. Se reintentará en {delay} segundos.'
        )

        return False, delay


def retry_worker():
    """
    Worker principal.

    Procesa un webhook a la vez para controlar la tasa de requests
    hacia EXTERNAL_URL.
    """

    last_request_at = 0.0

    while True:
        try:
            webhook = claim_next_webhook()

            if webhook:
                (
                    webhook_id,
                    payload,
                    content_type,
                    retry_count,
                ) = webhook

                # Rate limiting.
                elapsed = time.monotonic() - last_request_at

                if elapsed < RATE_LIMIT_INTERVAL:
                    time.sleep(
                        RATE_LIMIT_INTERVAL - elapsed
                    )

                last_request_at = time.monotonic()

                success, result = send_webhook(
                    webhook_id,
                    payload,
                    content_type,
                    retry_count,
                )

                if success:
                    mark_as_sent(webhook_id)

                else:
                    current_retry = retry_count + 1

                    # Error definitivo.
                    if result == 'permanent':
                        print(
                            '[Descartado] Webhook ID '
                            f'{webhook_id} recibió un error '
                            'no reintentable.'
                        )

                        delete_webhook(webhook_id)

                    # Se agotaron los reintentos.
                    elif current_retry >= MAX_RETRIES:
                        print(
                            '[Descartado] Webhook ID '
                            f'{webhook_id} superó el límite de '
                            f'{MAX_RETRIES} intentos. Eliminando.'
                        )

                        delete_webhook(webhook_id)

                    else:
                        schedule_retry(
                            webhook_id,
                            current_retry,
                            result,
                        )

                # Continuamos inmediatamente para buscar otro
                # webhook que pueda estar listo.
                continue

            # No hay trabajo disponible.
            enforce_limits()

            # Esperamos a que llegue un nuevo webhook o hasta
            # que transcurra RETRY_INTERVAL.
            worker_trigger.clear()
            worker_trigger.wait(
                timeout=RETRY_INTERVAL
            )

        except Exception as e:
            print(
                f'[Error en Worker] {e}'
            )

            # Evita un loop agresivo en caso de error inesperado.
            time.sleep(1)


# ---------------------------------------------------------------------------
# HTTP ENDPOINT
# ---------------------------------------------------------------------------

@app.route('/webhook', methods=['POST'])
def receive_webhook():
    try:
        data = request.get_data(
            as_text=True
        )

        if not data:
            return jsonify({
                'error': 'Payload vacío'
            }), 400

        content_type = request.headers.get(
            'Content-Type',
            'application/json',
        )

        conn = get_connection()

        try:
            cursor = conn.cursor()

            # Verificar el límite antes de insertar.
            cursor.execute(
                'SELECT COUNT(*) FROM webhooks'
            )

            total_records = cursor.fetchone()[0]

            if total_records >= MAX_RECORDS:
                # Intentamos liberar espacio eliminando registros
                # enviados antiguos.
                conn.close()

                cleanup_sent_webhooks()

                conn = get_connection()
                cursor = conn.cursor()

                cursor.execute(
                    'SELECT COUNT(*) FROM webhooks'
                )

                total_records = cursor.fetchone()[0]

                if total_records >= MAX_RECORDS:
                    print(
                        '[Backpressure] La cola alcanzó '
                        f'MAX_RECORDS={MAX_RECORDS}.'
                    )

                    return jsonify({
                        'error': 'Queue full'
                    }), 503

            cursor.execute(
                '''
                INSERT INTO webhooks (
                    payload,
                    content_type,
                    status,
                    retry_count,
                    next_retry_at
                )
                VALUES (?, ?, 'pending', 0, NULL)
                ''',
                (
                    data,
                    content_type,
                ),
            )

            conn.commit()

            webhook_id = cursor.lastrowid

        finally:
            conn.close()

        print(
            '[Recibido] Webhook ID '
            f'{webhook_id} con tipo {content_type} '
            'encolado exitosamente.'
        )

        worker_trigger.set()

        return jsonify({
            'status': 'queued',
            'id': webhook_id,
        }), 200

    except Exception as e:
        print(
            f'[Error al recibir] {e}'
        )

        return jsonify({
            'error': 'Internal server error'
        }), 500


# ---------------------------------------------------------------------------
# ERROR HANDLER
# ---------------------------------------------------------------------------

@app.errorhandler(413)
def request_entity_too_large(error):
    return jsonify({
        'error': 'Payload demasiado grande'
    }), 413


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    init_db()

    worker_thread = threading.Thread(
        target=retry_worker,
        daemon=True,
    )

    worker_thread.start()

    app.run(
        host='0.0.0.0',
        port=5000,
        threaded=True,
    )
