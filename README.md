<p align="center"><img width="300" height="251" alt="Logo-Resilient-Proxy" src="https://github.com/user-attachments/assets/af637f31-d9aa-499a-a197-054f9e9061ce"></p>

# Resilient Proxy

Un microservicio liviano en **Python (Flask)** y **SQLite** diseñado para actuar como proxy intermedio de webhooks para **Uptime Kuma** (u otras herramientas). 

Resuelve una limitación nativa común: la falta de resiliencia ante caídas del enlace a internet o fallos temporales del servidor receptor. Si tu red local pierde internet, este proxy encola los webhooks de forma persistente en disco y los reintenta de manera constante hasta obtener un **HTTP/200**, evitando bucles infinitos y controlando el espacio en disco automáticamente.

---

## 🚀 Características Principales

* **Resiliencia ante caídas de red:** Almacena los webhooks instantáneamente en una base de datos local SQLite. Si la red cae, reintenta el envío en segundo plano.
* **Control de bucles infinitos (`MAX_RETRIES`):** Si un destino externo deja de funcionar permanentemente, el proxy descarta el evento tras alcanzar el límite de reintentos configurado.
* **Límite de retención en disco (`MAX_RECORDS`):** Pasa por un proceso automático de limpieza para evitar que la base de datos crezca indefinidamente.
* **Preservación del tipo de contenido:** Soporta tanto requests en formato **JSON** (`application/json`) como en formato **Texto Plano** (`text/plain`), respetando las cabeceras originales.
* **Optimizado para Podman:** Incluye soporte de contextos de seguridad (`:Z`) para entornos rootless.

---

## 🛠️ Instalación y Despliegue con Podman

### 1. Archivos del Proyecto
Creá una carpeta en tu servidor local con los siguientes tres archivos:

* `app.py` (con el código del servidor Flask).
* `Dockerfile`
* `docker-compose.yml` (o `podman-compose.yml`)

### 2. Puesta en Marcha
Ejecutá el siguiente comando en la terminal de tu servidor donde tengas Podman (o Docker) instalado:

```bash
podman-compose up -d --build
```
---

## ⚙️ Variables de Entorno

| Variable | Descripción | Valor por defecto |
| :--- | :--- | :--- |
| `EXTERNAL_WEBHOOK_URL` | La URL de destino final a donde deben llegar las alertas. | `https://tu-servidor-externo.example.com/webhook` |
| `RETRY_INTERVAL` | Tiempo en segundos entre cada intento de reenvío. | `30` |
| `MAX_RETRIES` | Cantidad máxima de fallos permitidos antes de eliminar el registro de la cola. | `5` |
| `MAX_RECORDS` | Límite máximo de filas almacenadas en SQLite (purga los más antiguos excedentes). | `1000` |

---

## 🔗 Un ejemplo de configuración para el caso de Uptime Kuma

1. Entrá a tu panel de **Uptime Kuma**.
2. Seleccioná la opción **Configuración** > **Notificaciones** > **Agregar notificación**.
3. Seleccioná el tipo **Webhook**.
4. En la URL del Webhook, apuntá a tu contenedor local: 
   `http://resilient-proxy:5000/webhook` (o usá la IP interna de tu red de contenedores).
5. Guardá la configuración y realizá una prueba.
