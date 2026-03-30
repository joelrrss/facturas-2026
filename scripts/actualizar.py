"""
actualizar.py
Lee Gmail, extrae transacciones nuevas y actualiza facturas.csv
Corre dentro de GitHub Actions cada lunes.
"""

import os
import base64
import csv
import re
from datetime import datetime, timedelta
from email import message_from_bytes

import pandas as pd
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

# ── CONFIGURACIÓN ─────────────────────────────────────────────────────────────

RUTA_CSV = "facturas.csv"

# Categorías por palabra clave en el nombre del comercio
REGLAS_CATEGORIA = {
    "alimentación": [
        "cuchara", "dulce cafe", "ebano", "maxipali", "pali", "auto mercado",
        "automercado", "price smart", "natural planet", "macrobiotica",
        "pops", "helados", "spoon", "uber eats", "restaurante", "soda",
        "gallo grill", "good life", "saborcito", "el saborcito", "barista",
        "cafeteria", "panaderia", "pizza", "sushi", "pollo", "burger",
    ],
    "salud": [
        "farmacia", "clinica", "doctor", "dr.", "medico", "dental",
        "consufit", "hulipractice", "optica", "laboratorio",
    ],
    "transporte": [
        "delta", "bomba", "estacion de serv", "gasolina", "uber rides",
        "uber pending", "taxi", "peaje", "parqueo", "estacionamiento",
        "auto lavado",
    ],
    "suscripciones": [
        "anthropic", "claude", "openai", "chatgpt", "notion", "youtube",
        "google cloud", "apple", "icloud", "netflix", "spotify", "adobe",
        "github", "dropbox", "microsoft",
    ],
    "servicios": [
        "ice", "kolbi", "kölbi", "app kolbi", "cobro administracion",
        "colegio ciencias", "electricidad", "agua", "internet", "telefono",
        "consufit – cobro", "hulipractice – cobro",
    ],
}


def categorizar(comercio: str) -> str:
    nombre = comercio.lower()
    for categoria, palabras in REGLAS_CATEGORIA.items():
        if any(p in nombre for p in palabras):
            return categoria
    return "otros"


# ── GMAIL ─────────────────────────────────────────────────────────────────────

def conectar_gmail():
    creds = Credentials(
        token=None,
        refresh_token=os.environ["GMAIL_REFRESH_TOKEN"],
        client_id=os.environ["GMAIL_CLIENT_ID"],
        client_secret=os.environ["GMAIL_CLIENT_SECRET"],
        token_uri="https://oauth2.googleapis.com/token",
        scopes=["https://www.googleapis.com/auth/gmail.readonly"],
    )
    return build("gmail", "v1", credentials=creds)


def buscar_mensajes(service, dias_atras=8):
    """Busca correos de los últimos N días de fuentes de gastos conocidas."""
    desde = (datetime.now() - timedelta(days=dias_atras)).strftime("%Y/%m/%d")
    query = (
        f"after:{desde} ("
        "from:notificaciones@baccredomatic.com OR "
        "from:notificaciones@bncr.fi.cr OR "
        "from:noreply@facturar.cr OR "
        "from:billing@apple.com OR "
        "from:noreply@google.com OR "
        "subject:\"Voucher Digital\" OR "
        "subject:\"realizaste un pago\" OR "
        "subject:\"realizaste una compra\" OR "
        "subject:\"Factura Electrónica\" OR "
        "subject:\"Tiquete Electrónico\""
        ")"
    )
    result = service.users().messages().list(userId="me", q=query, maxResults=100).execute()
    return result.get("messages", [])


def obtener_cuerpo(msg_data) -> str:
    """Extrae el texto plano del email."""
    payload = msg_data.get("payload", {})

    def extraer_partes(payload):
        mime = payload.get("mimeType", "")
        if mime == "text/plain":
            data = payload.get("body", {}).get("data", "")
            if data:
                return base64.urlsafe_b64decode(data + "==").decode("utf-8", errors="ignore")
        for part in payload.get("parts", []):
            resultado = extraer_partes(part)
            if resultado:
                return resultado
        return ""

    return extraer_partes(payload)


# ── PARSERS POR FUENTE ────────────────────────────────────────────────────────

def parsear_bac(cuerpo: str, asunto: str) -> dict | None:
    """Notificación BAC: 'realizaste un pago/compra en COMERCIO por ₡X,XXX'"""
    patron = r"en\s+(.+?)\s+por\s+[₡\$]?\s*([\d,\.]+)"
    match = re.search(patron, cuerpo, re.IGNORECASE)
    if not match:
        return None
    comercio = match.group(1).strip()
    monto_str = match.group(2).replace(",", "")
    try:
        monto = float(monto_str)
    except ValueError:
        return None

    # Fecha: buscar patrón DD/MM/YYYY o DD de mes de YYYY en el cuerpo
    fecha = extraer_fecha(cuerpo)
    if not fecha:
        return None

    return {
        "Fecha": fecha,
        "Comercio": comercio.upper(),
        "Monto": monto,
        "Categoria": categorizar(comercio),
        "Tipo": "Notificación BAC",
    }


def parsear_bn(cuerpo: str, asunto: str) -> dict | None:
    """Voucher Digital BN"""
    patron_monto = r"[₡\$]?\s*([\d,\.]+(?:\.\d{2})?)"
    patron_comercio = r"(?:a|en|para)\s+([A-ZÁÉÍÓÚ][^\n\r]{3,50})"

    monto_match = re.search(patron_monto, cuerpo)
    comercio_match = re.search(patron_comercio, cuerpo, re.IGNORECASE)
    fecha = extraer_fecha(cuerpo)

    if not (monto_match and fecha):
        return None

    monto_str = monto_match.group(1).replace(",", "")
    try:
        monto = float(monto_str)
    except ValueError:
        return None

    comercio = comercio_match.group(1).strip() if comercio_match else asunto
    return {
        "Fecha": fecha,
        "Comercio": comercio,
        "Monto": monto,
        "Categoria": categorizar(comercio),
        "Tipo": "Voucher Digital BN",
    }


def parsear_factura_electronica(cuerpo: str, asunto: str) -> dict | None:
    """Facturas electrónicas del Ministerio de Hacienda."""
    patron_monto = r"Total\s*[:\s]*[₡\$]?\s*([\d,\.]+)"
    patron_emisor = r"(?:Emisor|Nombre del emisor)\s*[:\s]*([^\n\r]+)"

    monto_match = re.search(patron_monto, cuerpo, re.IGNORECASE)
    emisor_match = re.search(patron_emisor, cuerpo, re.IGNORECASE)
    fecha = extraer_fecha(cuerpo)

    if not (monto_match and fecha):
        return None

    monto_str = monto_match.group(1).replace(",", "")
    try:
        monto = float(monto_str)
    except ValueError:
        return None

    comercio = emisor_match.group(1).strip() if emisor_match else "Factura electrónica"
    return {
        "Fecha": fecha,
        "Comercio": comercio,
        "Monto": monto,
        "Categoria": categorizar(comercio),
        "Tipo": "Factura electrónica",
    }


def parsear_apple(cuerpo: str, asunto: str) -> dict | None:
    patron = r"([\d,\.]+)\s*(?:USD|CRC|₡)"
    monto_match = re.search(patron, cuerpo)
    fecha = extraer_fecha(cuerpo)
    if not (monto_match and fecha):
        return None
    try:
        monto = float(monto_match.group(1).replace(",", ""))
    except ValueError:
        return None
    comercio = re.sub(r"Your receipt from ", "", asunto, flags=re.IGNORECASE).strip()
    return {
        "Fecha": fecha,
        "Comercio": comercio or "Apple",
        "Monto": monto,
        "Categoria": "suscripciones",
        "Tipo": "Invoice",
    }


MESES_ES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
    "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
    "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
}


def extraer_fecha(texto: str) -> str | None:
    """Intenta extraer una fecha del texto en varios formatos."""
    # DD/MM/YYYY o DD-MM-YYYY
    m = re.search(r"(\d{2})[/-](\d{2})[/-](\d{4})", texto)
    if m:
        return f"{m.group(1)}/{m.group(2)}/{m.group(3)}"

    # DD de mes de YYYY (español)
    m = re.search(r"(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})", texto, re.IGNORECASE)
    if m:
        mes_num = MESES_ES.get(m.group(2).lower())
        if mes_num:
            return f"{int(m.group(1)):02d}/{mes_num:02d}/{m.group(3)}"

    return None


# ── LÓGICA PRINCIPAL ──────────────────────────────────────────────────────────

def cargar_csv_actual() -> pd.DataFrame:
    if os.path.exists(RUTA_CSV):
        return pd.read_csv(RUTA_CSV)
    return pd.DataFrame(columns=["Fecha", "Comercio", "Monto", "Categoria", "Tipo"])


def es_duplicado(df: pd.DataFrame, fila: dict) -> bool:
    if df.empty:
        return False
    mask = (
        (df["Fecha"] == fila["Fecha"]) &
        (df["Comercio"].str.upper() == str(fila["Comercio"]).upper()) &
        (df["Monto"].astype(float).round(2) == round(float(fila["Monto"]), 2))
    )
    return mask.any()


def procesar_mensaje(service, msg_id: str) -> dict | None:
    msg_data = service.users().messages().get(
        userId="me", id=msg_id, format="full"
    ).execute()

    headers = {h["name"].lower(): h["value"] for h in msg_data["payload"].get("headers", [])}
    asunto = headers.get("subject", "")
    remitente = headers.get("from", "").lower()
    cuerpo = obtener_cuerpo(msg_data)

    if not cuerpo:
        return None

    if "baccredomatic" in remitente or "realizaste" in asunto.lower():
        return parsear_bac(cuerpo, asunto)
    elif "bncr" in remitente or "voucher" in asunto.lower():
        return parsear_bn(cuerpo, asunto)
    elif "facturar.cr" in remitente or "factura electr" in asunto.lower() or "tiquete" in asunto.lower():
        return parsear_factura_electronica(cuerpo, asunto)
    elif "apple" in remitente:
        return parsear_apple(cuerpo, asunto)

    return None


def main():
    print("Conectando a Gmail…")
    service = conectar_gmail()

    print("Buscando correos nuevos…")
    mensajes = buscar_mensajes(service, dias_atras=8)
    print(f"  Encontrados: {len(mensajes)} correos")

    df = cargar_csv_actual()
    print(f"  Registros actuales en CSV: {len(df)}")

    nuevas = []
    omitidas = 0
    errores = 0

    for msg in mensajes:
        try:
            fila = procesar_mensaje(service, msg["id"])
            if fila is None:
                continue
            if es_duplicado(df, fila):
                omitidas += 1
                continue
            nuevas.append(fila)
            print(f"  + {fila['Fecha']} | {fila['Comercio']} | ₡{fila['Monto']:,.2f}")
        except Exception as e:
            errores += 1
            print(f"  Error procesando mensaje {msg['id']}: {e}")

    if not nuevas:
        print("No hay transacciones nuevas esta semana.")
        return

    df_nuevas = pd.DataFrame(nuevas)
    df_actualizado = pd.concat([df_nuevas, df], ignore_index=True)

    # Ordenar por fecha descendente
    df_actualizado["_ts"] = pd.to_datetime(df_actualizado["Fecha"], format="%d/%m/%Y", errors="coerce")
    df_actualizado = df_actualizado.sort_values("_ts", ascending=False).drop(columns=["_ts"])

    df_actualizado.to_csv(RUTA_CSV, index=False, encoding="utf-8")

    print(f"\nResumen:")
    print(f"  Transacciones nuevas: {len(nuevas)}")
    print(f"  Duplicados omitidos:  {omitidas}")
    print(f"  Errores:              {errores}")
    print(f"  Total en CSV ahora:   {len(df_actualizado)}")
    total_semana = sum(f["Monto"] for f in nuevas)
    print(f"  Gasto semana:         ₡{total_semana:,.2f}")


if __name__ == "__main__":
    main()
