import requests
import smtplib
from email.message import EmailMessage

# ---------------------------
# CONFIGURATION
# ---------------------------
KIBANA_URL = "http://your-kibana-server:5601"
HEADERS = {
    "kbn-xsrf": "true",
    "Content-Type": "application/json",
    "Authorization": "Bearer YOUR_TOKEN"  # ou Basic Auth si nécessaire
}

EMAIL_SENDER = "alert@example.com"
EMAIL_RECEIVER = "admin@example.com"
SMTP_SERVER = "smtp.example.com"
SMTP_PORT = 587
SMTP_USER = "alert@example.com"
SMTP_PASSWORD = "your_password"

# ---------------------------
# FONCTION : ENVOI MAIL
# ---------------------------
def send_alert_email(dv_name):
    msg = EmailMessage()
    msg["Subject"] = f"[ALERTE] DV vide sans historique : {dv_name}"
    msg["From"] = EMAIL_SENDER
    msg["To"] = EMAIL_RECEIVER
    msg.set_content(f"La data view '{dv_name}' est vide et ne possède pas de version historique dans le space_cold.")

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.send_message(msg)
    print(f"Email envoyé pour DV : {dv_name}")

# ---------------------------
# FONCTION : RÉCUPÉRER LES DATA VIEWS
# ---------------------------
def get_data_views():
    url = f"{KIBANA_URL}/api/saved_objects/_find?type=index-pattern&per_page=10000"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    return response.json()["saved_objects"]

# ---------------------------
# FONCTION : VÉRIFIER SI LA DV EST VIDE
# ---------------------------
def is_data_view_empty(dv):
    fields = dv.get("attributes", {}).get("fields", "[]")
    return fields == "[]"

# ---------------------------
# FONCTION : VÉRIFIER SI LA VERSION HISTORIQUE EXISTE
# ---------------------------
def has_historical_version(dv_name):
    hist_id = f"hist_{dv_name}"
    url = f"{KIBANA_URL}/s/space_cold/api/saved_objects/index-pattern/{hist_id}"
    response = requests.get(url, headers=HEADERS)
    return response.status_code == 200

# ---------------------------
# FONCTION : SUPPRIMER LA DV
# ---------------------------
def delete_data_view(dv_id):
    url = f"{KIBANA_URL}/api/saved_objects/index-pattern/{dv_id}"
    response = requests.delete(url, headers=HEADERS)
    if response.status_code == 200:
        print(f"Data view supprimée : {dv_id}")
    else:
        print(f"Échec de suppression pour : {dv_id} - {response.status_code}")

# ---------------------------
# MAIN LOGIC
# ---------------------------
def main():
    dvs = get_data_views()
    print(f"{len(dvs)} Data Views trouvées.")

    for dv in dvs:
        dv_id = dv["id"]
        dv_name = dv["attributes"].get("title", "")

        if is_data_view_empty(dv):
            print(f"DV vide détectée : {dv_name}")

            if has_historical_version(dv_name):
                print(f"→ Version historique trouvée. Suppression de {dv_name}.")
                delete_data_view(dv_id)
            else:
                print(f"→ Pas de version historique trouvée pour {dv_name}. Envoi d'une alerte.")
                send_alert_email(dv_name)

if __name__ == "__main__":
    main()
