# SPDX-FileCopyrightText: 2024 Jonathan Sejdija
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import pandas as pd
from dateutil.relativedelta import relativedelta
from plz_to_nuts import convert_plz_to_nuts

from .db_handler import DBHandler
from .models import PowerPurchaseAgreement, WindTurbine


class Simulator(DBHandler):
    """
    A class used to simulate the wind turbine's power generation and market value.

    Attributes:
    ----------
    db_uri : str
        The URI of the database containing load profile data.
    start_date : str
        The start date for the simulation.
    end_date : str
        The end date for the simulation.
    """
    def __init__(self, DB_URI, start_date, end_date):
        """
        Initializes the Simulator with the database URI, start date, and end date.

        Parameters:
        ----------
        DB_URI : str
            The URI of the database containing load profile data.
        start_date : str
            The start date for the simulation.
        end_date : str
            The end date for the simulation.
        """
        super().__init__(DB_URI)
        self.start_date = start_date
        self.end_date = end_date
        
    def check_granularity_and_merge(self, df1, df2):
        """
        Checks the granularity of the two DataFrames and merges them.

        Parameters:
        ----------
        df1 : pd.DataFrame
            The first DataFrame.
        df2 : pd.DataFrame
            The second DataFrame.

        Returns:
        -------
        pd.DataFrame
            The merged DataFrame.
        """
        df1['timestamp'] = pd.to_datetime(df1['timestamp'], utc=True)
        df2['timestamp'] = pd.to_datetime(df2['timestamp'], utc=True)
        df1_granularity = df1['timestamp'].diff().min()
        df2_granularity = df2['timestamp'].diff().min()
        if df1_granularity < df2_granularity:
            df1_resampled = df1.set_index('timestamp').resample(df2_granularity).mean().reset_index()
            df2_resampled = df2
        else:
            df2_resampled = df2.set_index('timestamp').resample(df1_granularity).ffill().reset_index()
            df1_resampled = df1
        return pd.merge(df1_resampled, df2_resampled, on='timestamp', how='inner')
    
    def cast_time_series_to_year(self, dataframe, target_year, timestamp_column='timestamp'):
        """
        Casts the time series in the DataFrame to the target year.

        Parameters:
        ----------
        dataframe : pd.DataFrame
            The DataFrame containing the time series.
        target_year : int
            The target year to cast the time series to.
        timestamp_column : str, optional
            The name of the timestamp column (default is 'timestamp').

        Returns:
        -------
        pd.DataFrame
            The DataFrame with the time series cast to the target year.
        """
        def adjust_to_target_year(date, target_year):
            try:
                adjusted_date = date.replace(year=target_year)
            except ValueError:
                adjusted_date = date + relativedelta(year=target_year, day=31)
            return adjusted_date

        adjusted_df = dataframe.copy()
        adjusted_df[timestamp_column] = pd.to_datetime(adjusted_df[timestamp_column])
        adjusted_df[timestamp_column] = adjusted_df[timestamp_column].apply(
            lambda x: adjust_to_target_year(x, target_year)
        )
        return adjusted_df.sort_values(by='timestamp').reset_index(drop=True).drop_duplicates(subset=timestamp_column)


    def simulate(self, profile_id: int):
        """
        Simulates the wind turbine's power generation and market value.

        Parameters:
        ----------
        profile_id : int
            The ID of the load profile to simulate.
        """
        master_data = self.get_master_data(profile_id)
        load_data = self.get_load_data(profile_id)
        price_data = self.get_price_data(self.start_date, self.end_date)
        wind_speed_df = self.get_weather_data(convert_plz_to_nuts(str(master_data.loc['zip_code'].values[0]))[1], self.start_date, self.end_date)
        wind_turbine = WindTurbine(rotor_radius=110, cut_in_speed=3, rated_speed=12, cut_out_speed=25)
        power_df = wind_turbine.calculate_power_with_windpowerlib(wind_speed_df)
        all_data_df = wind_turbine.calculate_market_value(self.check_granularity_and_merge(power_df, price_data))    
        print(f'Market Value of the wind turbine for the year 2019: {all_data_df["Market Value (€)"].sum()} €')
        ppa = PowerPurchaseAgreement(all_data_df)
        print(f'Fixed Energy Price of the PPA: {ppa.fixed_energy_price} €/MWh')
        
        load_data = self.cast_time_series_to_year(load_data, 2019)        
        all_data_df = self.check_granularity_and_merge(all_data_df, load_data)
        
        all_data_df['Load (MWh)'] = all_data_df['Load (kWh)'] / 1000
        all_data_df['PPA Surplus (MWh)'] = (all_data_df['Actual Power (MWh)'] - all_data_df['Load (MWh)']).clip(lower=0)
        all_data_df['Scenario As Is (€)'] = all_data_df['price'] * all_data_df['Load (MWh)']
        
        # Berechnung der Kosten:
        # - Fehlende Energie wird zugekauft (positiver Bedarf)
        # - Überschüssige Energie wird zum Marktpreis verkauft
        all_data_df['Scenario With PPA (€)'] = (
            (all_data_df['Load (MWh)'] - all_data_df['Actual Power (MWh)']).clip(lower=0) * all_data_df['price'] +  # Zukaufkosten
            all_data_df['Actual Power (MWh)'] * ppa.fixed_energy_price -                                            # PPA Kosten
            all_data_df['PPA Surplus (MWh)'] * all_data_df['price']                                                 # Verkauf von Überschüssen
        )
        
        print(f'The Cost of  the As-Is Scenario: {all_data_df["Scenario As Is (€)"].sum()} €')
        print(f'The Cost of the PPA Scenario: {all_data_df["Scenario With PPA (€)"].sum()} €')