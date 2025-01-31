# SPDX-FileCopyrightText: 2024 Jonathan Sejdija
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import os

from dotenv import load_dotenv

from .simulator import Simulator


def main(DB_URI: str, profile_id: int = None, scenario: list = [0.9, 0.95, 0.98, 1, 1.02, 1.05, 1.1]):
    """
    Runs the simulation for the specified database URI, profile ID and scenario.

    Parameters:
    ----------
    DB_URI : str
        The URI of the database containing load profile data.
    profile_id : int
        The ID of the load profile to simulate.
    scenario : list
        A list containing factors that the market price is multiplied by, simulating market price volatility.
    """
    start_date, end_date = "2019-01-01", "2020-01-01"
    simulator = Simulator(DB_URI, start_date, end_date)
    if profile_id is None:
        for i in range(5359):
            simulator.simulate(i, scenario)
    else:
        simulator.simulate(profile_id)


if __name__ == "__main__":
    load_dotenv()
    DB_URI = os.getenv("URI")
    main(DB_URI, 531)
