# kibana_dataviews_utils.py

import json
import requests
import time
import copy
from kibana import request_kibana


def get_dvs_from_env(env, space_id):
    """
    Récupère tous les dataviews (index-pattern) d'un espace dans un environnement Kibana.
    
    :param env: Informations de connexion à l'environnement Kibana
    :param space_id: ID de l'espace Kibana
    :return: Liste de dataviews (dictionnaires)
    :raises: Exception en cas d'échec
    """
    try:
        response = request_kibana(
            f"/s/{space_id}/api/saved_objects/_find?type=index-pattern&per_page=10000",
            "GET",
            env
        )

        if not 200 <= response.status_code <= 300:
            raise Exception("Failed to get dataviews, error: Dataviews not found")

        dataviews = response.json().get("saved_objects", [])

        # Dump with keys sorted
        dv_strings = [json.dumps(dv, sort_keys=True) for dv in dataviews]

        # Supprimer les doublons
        unique_dvs = set(dv_strings)

        # Recharger en objets Python
        all_dataviews = [json.loads(dv_string) for dv_string in unique_dvs]

        return all_dataviews

    except Exception as error:
        raise Exception(f"Failed to get dataviews, error: {error}")


def get_empty_dataviews(dataviews):
    """
    Retourne les dataviews dont le champ 'fields' est vide ou inexistant.
    """
    return [
        dv for dv in dataviews
        if not dv.get("attributes", {}).get("fields")
    ]


def get_non_empty_dataviews(dataviews):
    """
    Retourne les dataviews dont le champ 'fields' est renseigné.
    """
    return [
        dv for dv in dataviews
        if dv.get("attributes", {}).get("fields")
    ]


def print_empty_dataview_ids(dataviews):
    """
    Affiche les IDs des dataviews vides.
    """
    empty_dvs = get_empty_dataviews(dataviews)
    for dv in empty_dvs:
        print(dv.get("id"))


def get_empty_dataview_ids(dataviews):
    """
    Retourne la liste des IDs des dataviews vides.
    """
    return [dv.get("id") for dv in get_empty_dataviews(dataviews)]


def match_with_historical_version(empty_dvs, historical_dvs):
    """
    Pour chaque dataview vide, cherche un équivalent nommé 'hist_<nom>'
    dans la liste des dataviews du space 'co'.

    :param empty_dvs: dataviews vides dans un autre espace (ex: default)
    :param historical_dvs: dataviews du space 'co'
    :return: Liste de tuples (nom_dv_vide, id_dv_vide, hist_dv_id ou 'pas trouvé')
    """
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


# Exemple d'utilisation
if __name__ == "__main__":
    # Exemple d'environnement, à adapter
    env = {
        "host": "https://kibana.example.com",
        "auth_token": "your_auth_token"
    }

    # Récupération des dataviews
    dv_default = get_dvs_from_env(env, "default")
    dv_co = get_dvs_from_env(env, "co")

    # Dataviews vides
    empty_dvs = get_empty_dataviews(dv_default)

    # Comparaison avec les versions historiques dans 'co'
    match_results = match_with_historical_version(empty_dvs, dv_co)

    # Affichage
    print("\nCorrespondance des dataviews vides avec leur version historique :")
    for name, id_empty, hist_id in match_results:
        print(f"{name} (ID: {id_empty}) → Équivalent historique: {hist_id}")
