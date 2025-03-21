# SPDX-FileCopyrightText: 2024 Jonathan Sejdija
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import logging

import pandas as pd
from dateutil.relativedelta import relativedelta
from plz_to_nuts import convert_plz_to_nuts

from .db_handler import DBHandler
from .models import PowerPurchaseAgreement, WindTurbine

# Set up logging
logging.basicConfig(
    filename="ppa_simulator.log",
    encoding="utf-8",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class Simulator(DBHandler):
    """
    A class used to simulate the wind turbine's power generation and market value.

    Attributes:
    ----------
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

    def check_granularity_and_merge(self, df1, df2, method="mean"):
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
        df1["timestamp"] = pd.to_datetime(df1["timestamp"], utc=True)
        df2["timestamp"] = pd.to_datetime(df2["timestamp"], utc=True)
        df1_granularity = df1["timestamp"].diff().min()
        df2_granularity = df2["timestamp"].diff().min()
        if df1_granularity < df2_granularity:
            df1_resampled = (
                df1.set_index("timestamp")
                .resample(df2_granularity)
                .agg(method)
                .reset_index()
            )
            df2_resampled = df2
        else:
            df2_resampled = (
                df2.set_index("timestamp")
                .resample(df1_granularity)
                .agg(method)
                .reset_index()
            )
            df1_resampled = df1
        return pd.merge(df1_resampled, df2_resampled, on="timestamp", how="inner")

    def cast_time_series_to_year(
        self, dataframe, target_year, timestamp_column="timestamp"
    ):
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
        return (
            adjusted_df.sort_values(by="timestamp")
            .reset_index(drop=True)
            .drop_duplicates(subset=timestamp_column)
        )

    def simulate(self, profile_id: int, scenarios: list):
        """
        Simulates the wind turbine's power generation and market value.

        Parameters:
        ----------
        profile_id : int
            The ID of the load profile to simulate.
        scenarios : list
            A list of factors that the market price is multiplied by, simulating market price volatility.
        """
        master_data = self.get_master_data(profile_id)
        load_data = self.get_load_data(profile_id)
        price_data = self.get_price_data(self.start_date, self.end_date)
        wind_speed_df = self.get_weather_data(
            convert_plz_to_nuts(str(master_data.loc["zip_code"].values[0]))[1],
            self.start_date,
            self.end_date,
        )
        if wind_speed_df.empty:
            logger.error(
                f"No wind speed data found for the specified location {master_data.loc['zip_code'].values[0]}, nuts code {convert_plz_to_nuts(str(master_data.loc['zip_code'].values[0]))[1]}"
            )
            return

        yearly_consumption = load_data["load(kwh)"].sum() * 1e-3
        
        wind_turbine = WindTurbine(
            rotor_radius=110, cut_in_speed=3, rated_speed=12, cut_out_speed=25
        )
        
        
        
        power_df = wind_turbine.calculate_power_with_windpowerlib(wind_speed_df)
        average_yearly_power = power_df["actual_power_single_turbine(w)"].mean()* 1e-6 * len(wind_speed_df)
        numer_of_wind_turbines = yearly_consumption / average_yearly_power
        
        all_data_df = wind_turbine.calculate_market_value(
            self.check_granularity_and_merge(power_df, price_data), numer_of_wind_turbines
        )
        logger.info(
            f"Market Value of the wind turbine for the year 2019: {all_data_df['market_value_single_turbine(€)'].sum()} €"
        )
        logger.info(
            f"Market Value of the needed number of wind turbines for the year 2019: {all_data_df['market_value_needed_turbines(€)'].sum()} €"
        )
        ppa = PowerPurchaseAgreement(all_data_df)
        logger.info(f"Fixed Energy Price of the PPA: {ppa.fixed_energy_price} €/MWh")

        load_data = self.cast_time_series_to_year(load_data, 2019)
        all_data_df = self.check_granularity_and_merge(all_data_df, load_data, method = "sum")

        all_data_df["load(mwh)"] = all_data_df["load(kwh)"] / 1000
        all_data_df["ppa_surplus(mwh)"] = (
            all_data_df["actual_power_needed_turbines(mwh)"] - all_data_df["load(mwh)"]
        ).clip(lower=0)

        # Berechnung der Kosten:
        # - Fehlende Energie wird zugekauft (positiver Bedarf)
        # - Überschüssige Energie wird zum Marktpreis verkauft
        for scenario in scenarios:
            all_data_df[f"scenario_as_is_{scenario}(€)"] = (
                all_data_df["price"] * all_data_df["load(mwh)"] * scenario
            )
            all_data_df[f"scenario_with_ppa_{scenario}(€)"] = (
                (all_data_df["load(mwh)"] - all_data_df["actual_power_needed_turbines(mwh)"]).clip(
                    lower=0
                )
                * all_data_df["price"]
                * scenario                              # Zukaufkosten
                + all_data_df["actual_power_needed_turbines(mwh)"]
                * ppa.fixed_energy_price                # PPA Kosten
                - all_data_df["ppa_surplus(mwh)"]
                * all_data_df["price"]
                * scenario                              # Verkauf von Überschüssen
            )
            logger.info(
                f"The Cost of  the As-Is Scenario {scenario}: {all_data_df[f'scenario_as_is_{scenario}(€)'].sum()} €"
            )
            logger.info(
                f"The Cost of the PPA Scenario {scenario}: {all_data_df[f'scenario_with_ppa_{scenario}(€)'].sum()} €"
            )

        all_data_df["plz"] = master_data.loc["zip_code"].values[0]
        all_data_df["nuts_id"] = convert_plz_to_nuts(
            str(master_data.loc["zip_code"].values[0])
        )[1]
        all_data_df["profile_id"] = profile_id
        all_data_df["sector_group_id"] = master_data.loc["sector_group_id"].values[0]
        all_data_df["sector_group"] = master_data.loc["sector_group"].values[0]

        try:
            self.write_data(all_data_df, "ppa_results")
            logger.info(
                f"Results for profile {profile_id} saved to database successfully"
            )
        except Exception as e:
            logger.error(f"Error while saving results for profile {profile_id}: {e}")
