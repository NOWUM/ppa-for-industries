# SPDX-FileCopyrightText: 2024 Jonathan Sejdija
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import os

from dotenv import load_dotenv

from .simulator import Simulator


def main(profile_id: int, DB_URI: str):
    """
    Runs the simulation for the specified profile ID and database URI.

    Parameters:
    ----------
    profile_id : int
        The ID of the load profile to simulate.
    DB_URI : str
        The URI of the database containing load profile data.
    """
    start_date, end_date = "2019-01-01", "2020-01-01"
    Simulator(DB_URI, start_date, end_date).simulate(profile_id)


if __name__ == "__main__":
    load_dotenv()
    DB_URI = os.getenv("URI")
    # for i in range(5359):
    #     main(i, DB_URI)

    main(531, DB_URI)
