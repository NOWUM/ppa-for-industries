# SPDX-FileCopyrightText: 2024 Jonathan Sejdija
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import pandas as pd
from sqlalchemy import create_engine


class DBHandler:
    """
    A class used to handle database operations with OEDS.

    Attributes:
    ----------
    db_uri : str
        The URI of the database containing load profile data.
    """
    def __init__(self, db_uri):
        """
        Initializes the DBHandler with the database URI.

        Parameters:
        ----------
        db_uri : str
            The URI of the database containing load profile data.
        """
        self.db_uri = db_uri
        
    def get_load_data(self, profile_id: int) -> pd.DataFrame:
        """
        Retrieves load data from the database for the specified profile ID.

        Parameters:
        ----------
        profile_id : int
            The ID of the load profile to retrieve.

        Returns:
        -------
        pd.DataFrame
            A DataFrame containing the load data in kWh.
        """
        sql = f"""
            select * from vea_industrial_load_profiles.load
            where id = {profile_id}
            order by timestamp asc
        """
        data = pd.read_sql(sql, self.db_uri)
        data["value"] /= 4
        data = data.drop(columns=['id'])
        data = data.rename(columns={'value': 'load(kwh)'})
        return data
    
    def get_price_data(self, start_date: str=None, end_date: str=None) -> pd.DataFrame:
        """
        Retrieves price data from the database.

        Parameters:
        ----------
        start_date : str, optional
            The start date for the price data (default is None).
        end_date : str, optional
            The end date for the price data (default is None).

        Returns:
        -------
        pd.DataFrame
            A DataFrame containing the price data in €/kWh.
        """
        sql = "select timestamp, price from smard.prices"
        if start_date and end_date:
            sql += f" where timestamp between '{start_date}' and '{end_date}'"
        return pd.read_sql(sql, self.db_uri)
    
    def get_master_data(self, profile_id: int) -> pd.DataFrame:
        """
        Retrieves master data from the database for the specified profile ID.

        Parameters:
        ----------
        profile_id : int
            The ID of the master data to retrieve.

        Returns:
        -------
        pd.DataFrame
            A DataFrame containing the master data.
        """
        sql = f"select * from vea_industrial_load_profiles.master where id = {profile_id}"
        return pd.read_sql(sql, self.db_uri).T
    
    def get_weather_data(self, nuts_id, start_date: str=None, end_date:str=None) -> pd.DataFrame:
        """
        Retrieves weather data from the database for the area.

        Parameters:
        ----------
        nuts_id : str
            The NUTS ID of the area to retrieve weather data for.
        start_date : str, optional
            The start date for the weather data (default is None).
        end_date : str, optional
            The end date for the weather data (default is None).

        Returns:
        -------
        pd.DataFrame
            A DataFrame containing the weather data.
        """
        sql = f"select time, wind_speed from weather.ecmwf_eu where nuts_id = '{nuts_id}'"
        if start_date and end_date:
            sql += f" and time between '{start_date}' and '{end_date}'"
        return pd.read_sql(sql, self.db_uri).rename(columns={'time': 'timestamp'})
    
    def write_data(self, data: pd.DataFrame, table_name: str):
        """
        Writes data to the database.

        Parameters:
        ----------
        data : pd.DataFrame
            The data to write to the database.
        table_name : str
            The name of the table to write the data to.
        """
        connection = create_engine(self.db_uri)                        
        data.to_sql(table_name, connection, schema='vea_results_timeseries', if_exists='append', index=False)
        connection.dispose()