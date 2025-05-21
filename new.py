# kibana_dataviews_utils.py

import json
import smtplib
from email.message import EmailMessage
from kibana import request_kibana


def get_dvs_from_env(env, space_id):
    try:
        response = request_kibana(
            f"/s/{space_id}/api/saved_objects/_find?type=index-pattern&per_page=10000",
            "GET",
            env
        )

        if not 200 <= response.status_code <= 300:
            raise Exception("Failed to get dataviews, error: Dataviews not found")

        dataviews = response.json().get("saved_objects", [])
        dv_strings = [json.dumps(dv, sort_keys=True) for dv in dataviews]
        unique_dvs = set(dv_strings)
        all_dataviews = [json.loads(dv_string) for dv_string in unique_dvs]
        return all_dataviews

    except Exception as error:
        raise Exception(f"Failed to get dataviews, error: {error}")


def get_empty_dataviews(dataviews):
    return [dv for dv in dataviews if not dv.get("attributes", {}).get("fields")]


def get_non_empty_dataviews(dataviews):
    return [dv for dv in dataviews if dv.get("attributes", {}).get("fields")]


def get_empty_dataview_ids(dataviews):
    return [dv.get("id") for dv in get_empty_dataviews(dataviews)]


def match_with_historical_version(empty_dvs, historical_dvs):
    result = []
    hist_titles = {
        dv.get("attributes", {}).get("title"): dv.get("id")
        for dv in historical_dvs
    }

    for dv in empty_dvs:
        title = dv.get("attributes", {}).get("title", "")
        expected_hist_name = f"hist_{title}"
        hist_id = hist_titles.get(expected_hist_name, "pas trouvé")
        result.append((title, dv.get("id"), hist_id))

    return result


def delete_dataview_by_id(env, space_id, dv_id):
    try:
        response = request_kibana(
            f"/s/{space_id}/api/saved_objects/index-pattern/{dv_id}",
            "DELETE",
            env
        )
        return response.status_code in [200, 202, 204]
    except Exception as e:
        print(f"Erreur lors de la suppression de {dv_id}: {e}")
        return False


def delete_dvs_without_historical(env, space_id, match_results):
    """
    Supprime les dataviews qui n'ont pas de version historique.
    """
    for name, dv_id, hist_id in match_results:
        if hist_id == "pas trouvé":
            success = delete_dataview_by_id(env, space_id, dv_id)
            if success:
                print(f"Supprimé : {name} (ID: {dv_id})")
            else:
                print(f"Échec de suppression : {name} (ID: {dv_id})")
                
                
                
def filter_itgsoc_dvs(dvs):
    """
    Filtre les Data Views dont l'ID commence par 'itgsoc'
    
    :param dvs: Liste des Data Views (issues de l'API Kibana)
    :return: Liste filtrée de DV
    """
    return [dv for dv in dvs if dv["id"].startswith("itgsoc")]


def extract_version_from_id(dv_id):
    """
    Extrait la version depuis un ID de Data View formaté comme val1-val2-version
    
    :param dv_id: ID de la Data View
    :return: version (str)
    """
    parts = dv_id.split('-')
    if len(parts) < 3:
        return None  # Format inattendu
    return parts[-1]  # Dernière partie = version



def send_dataview_summary_email(sender_email, receiver_email, smtp_server, smtp_port, smtp_login, smtp_password, matched, space_id):
    """
    Envoie un mail résumant les dataviews vides et leur correspondance historique.
    """
    with_versions = [item for item in matched if item[2] != "pas trouvé"]
    without_versions = [item for item in matched if item[2] == "pas trouvé"]

    message = EmailMessage()
    message["Subject"] = "Résumé des Data Views Kibana Vides et Versions Historiques"
    message["From"] = sender_email
    message["To"] = receiver_email

    body = f"""
Bonjour,

Voici le rapport d'audit des data views vides dans l'espace '{space_id}' :

➡️ **Dataviews vides trouvées :** {len(matched)}

✅ **Avec versions historiques disponibles dans l’espace 'co' (elles sont supprimées) :**
"""

    if with_versions:
        for name, dv_id, hist_id in with_versions:
            body += f"\n- {name} (ID: {dv_id}) → historique : {hist_id}"
    else:
        body += "\n- Aucune"

    body += f"""

❌ **Sans version historique (non supprimées) :**
"""
    if without_versions:
        for name, dv_id, _ in without_versions:
            body += f"\n- {name} (ID: {dv_id}) → historique : non trouvé"
    else:
        body += "\n- Aucune"

    body += "\n\nBien cordialement,\nVotre script Kibana"

    message.set_content(body)

    try:
        with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
            server.login(smtp_login, smtp_password)
            server.send_message(message)
        print("📧 Mail envoyé avec succès.")
    except Exception as e:
        print(f"❌ Échec de l'envoi du mail : {e}")


# Exemple d'utilisation
if __name__ == "__main__":
    # Configuration
    env = {
        "host": "https://kibana.example.com",
        "auth_token": "your_auth_token"
    }

    smtp_conf = {
        "sender_email": "expediteur@example.com",
        "receiver_email": "destinataire@example.com",
        "smtp_server": "smtp.example.com",
        "smtp_port": 465,
        "smtp_login": "expediteur@example.com",
        "smtp_password": "motdepasse"
    }

    # Étapes
    space_id = "default"
    dv_default = get_dvs_from_env(env, space_id)
    dv_co = get_dvs_from_env(env, "co")

    empty_dvs = get_empty_dataviews(dv_default)
    match_results = match_with_historical_version(empty_dvs, dv_co)

    # Suppression de ceux sans version historique
    delete_dvs_without_historical(env, space_id, match_results)

    # Envoi du mail
    send_dataview_summary_email(
        smtp_conf["sender_email"],
        smtp_conf["receiver_email"],
        smtp_conf["smtp_server"],
        smtp_conf["smtp_port"],
        smtp_conf["smtp_login"],
        smtp_conf["smtp_password"],
        match_results,
        space_id
    )
