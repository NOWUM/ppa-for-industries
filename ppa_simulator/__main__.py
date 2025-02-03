# SPDX-FileCopyrightText: 2024 Jonathan Sejdija
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import os
from multiprocessing import Pool

from dotenv import load_dotenv
from tqdm import tqdm

from .simulator import Simulator


def simulate_profile(args):
    """
    Wrapper-Funktion zur Simulation eines Profils mit einer bestimmten ID und einem Szenario.
    """
    db_uri, profile_id, scenario, start_date, end_date = args
    simulator = Simulator(db_uri, start_date, end_date)
    simulator.simulate(profile_id, scenario)


def main(
    DB_URI: str,
    profile_id: int = None,
    scenario: list = [0.9, 0.95, 0.98, 1, 1.02, 1.05, 1.1],
):
    """
    Führt die Simulation für die angegebene Datenbank-URI, Profil-ID und das Szenario aus.

    Parameter:
    ----------
    DB_URI : str
        Die URI der Datenbank mit den Lastprofildaten.
    profile_id : int
        Die ID des zu simulierenden Lastprofils.
    scenario : list
        Eine Liste mit Faktoren zur Simulation von Marktpreisschwankungen.
    """
    start_date, end_date = "2019-01-01", "2020-01-01"

    if profile_id is None:
        # Anzahl der Profile und Argumente für Multiprocessing vorbereiten
        num_profiles = 5359  # Gesamtanzahl der Profile
        args_list = [
            (DB_URI, i, scenario, start_date, end_date) for i in range(num_profiles)
        ]

        # Fortschrittsbalken initialisieren
        with Pool() as pool:
            with tqdm(total=num_profiles) as pbar:
                for _ in pool.imap_unordered(simulate_profile, args_list):
                    pbar.update(1)
    else:
        # Simulation für ein einzelnes Profil ausführen
        simulate_profile((DB_URI, profile_id, scenario, start_date, end_date))


if __name__ == "__main__":
    load_dotenv()
    DB_URI = os.getenv("URI")
    main(DB_URI, 531)
