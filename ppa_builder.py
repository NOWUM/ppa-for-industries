import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
import math
from plz_to_nuts import convert_plz_to_nuts
from dateutil.relativedelta import relativedelta

class DBHandler:
    """
    A class used to handle database operations with OEDS.
    Attributes:
    ----------
    db_uri : str
        The URI of the database containing load profile data.
    """
    def __init__(self, db_uri):
        self.db_uri = db_uri
        
    def get_load_data(self, profile_id: int) -> pd.DataFrame:
        """
        Retrieves load data from the database for the specified profile ID.

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
        data = data.rename(columns={'value': 'Load (kWh)'})
        return data
    
    def get_price_data(self, start_date: str=None, end_date: str=None) -> pd.DataFrame:
        """
        Retrieves price data from the database.

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

        Returns:
        -------
        pd.DataFrame
            A DataFrame containing the weather data.
        """
        sql = f"select time, wind_speed from weather.ecmwf_eu where nuts_id = '{nuts_id}'"
        if start_date and end_date:
            sql += f" and time between '{start_date}' and '{end_date}'"
        return pd.read_sql(sql, self.db_uri).rename(columns={'time': 'timestamp'})
    
class WindTurbine:
    def __init__(self, rotor_radius, cut_in_speed, rated_speed, cut_out_speed, air_density=1.225, efficiency=0.4):
        self.rotor_radius = rotor_radius 
        # durchschnittlicher Rotor-Radius 2023 für Onshore liegt bei 110m
        # https://www.statista.com/statistics/1085649/onshore-wind-turbines-average-rotor-diameter-globally/
        self.cut_in_speed = cut_in_speed
        self.rated_speed = rated_speed
        self.cut_out_speed = cut_out_speed
        self.air_density = air_density
        self.efficiency = efficiency
        self.area = math.pi * (rotor_radius ** 2)
        # Berechnung der Nennleistung bei der Rated Speed
        self.rated_power = self.efficiency * 0.5 * self.air_density * self.area * (self.rated_speed ** 3)

    def calculate_power(self, wind_speed_df: pd.DataFrame) -> pd.DataFrame:
        # Berechnung der theoretischen Leistung
        wind_speed_df['Theoretical Power (W)'] = 0.5 * self.air_density * self.area * (wind_speed_df['wind_speed'] ** 3)
        # Berechnung der tatsächlichen Leistung unter Berücksichtigung der Effizienz
        wind_speed_df['Actual Power (W)'] = self.efficiency * wind_speed_df['Theoretical Power (W)']
         # Setze die Leistung auf 0, wenn die Windgeschwindigkeit unter der Cut-in- oder über der Cut-out-Geschwindigkeit liegt
        wind_speed_df.loc[wind_speed_df['wind_speed'] < self.cut_in_speed, 'Actual Power (W)'] = 0
        wind_speed_df.loc[wind_speed_df['wind_speed'] > self.cut_out_speed, 'Actual Power (W)'] = 0
        # Begrenze die Leistung auf die Nennleistung ab der Rated Speed
        wind_speed_df.loc[(wind_speed_df['wind_speed'] >= self.rated_speed) & 
                          (wind_speed_df['wind_speed'] <= self.cut_out_speed), 'Actual Power (W)'] = self.rated_power
        return wind_speed_df
    
    def calculate_market_value(self, power_and_price_df):
        # Umrechnung der Leistung von W in MWh
        power_and_price_df['Actual Power (MWh)'] = power_and_price_df['Actual Power (W)'] * 1e-6

        # Berechnung des Marktwerts
        power_and_price_df['Market Value (€)'] = power_and_price_df['Actual Power (MWh)'] * power_and_price_df['price']

        return power_and_price_df
    
class PowerPurchaseAgreement:
    def __init__(self, market_value: pd.DataFrame):
        self.fixed_energy_price = self.calculate_average_value(market_value)
    
    def calculate_average_value(self, market_value: pd.DataFrame) -> float:
        return market_value['Market Value (€)'].sum() / market_value['Actual Power (MWh)'].sum()
    
    
class Simulator(DBHandler):
    def __init__(self, DB_URI, start_date, end_date):
        super().__init__(DB_URI)
        self.start_date = start_date
        self.end_date = end_date
        
    def check_granularity_and_merge(self, df1, df2):
        # verify timestamp format
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
        Passt die Zeitreihe in einem DataFrame an ein Zieljahr an und behält den Wochentag bei.
        Falls ein Datum im Zieljahr ungültig ist (z. B. 29. Februar in einem Nicht-Schaltjahr),
        wird es auf den letzten gültigen Tag des Monats gesetzt.

        Parameters:
            dataframe (pd.DataFrame): Der DataFrame mit einer Zeitspalte.
            target_year (int): Das Zieljahr, auf das die Zeitreihe angepasst werden soll.
            timestamp_column (str): Der Name der Spalte mit den Zeitstempeln (Standard: 'timestamp').

        Returns:
            pd.DataFrame: Ein neuer DataFrame mit angepassten Zeitstempeln.
        """
        def adjust_to_target_year(date, target_year):
            try:
                # Ersetze das Jahr und passe den Wochentag an
                adjusted_date = date.replace(year=target_year)
            except ValueError:
                # Falls der Tag im Zieljahr ungültig ist, setze auf den letzten gültigen Tag des Monats
                adjusted_date = date + relativedelta(year=target_year, day=31)
            return adjusted_date

        # Kopiere den DataFrame, um Originaldaten nicht zu verändern
        adjusted_df = dataframe.copy()
        
        # Stelle sicher, dass die Spalte als datetime interpretiert wird
        adjusted_df[timestamp_column] = pd.to_datetime(adjusted_df[timestamp_column])
        
        # Passe jede Zeile an das Zieljahr an
        adjusted_df[timestamp_column] = adjusted_df[timestamp_column].apply(
            lambda x: adjust_to_target_year(x, target_year)
        )
        
        return adjusted_df.sort_values(by='timestamp').reset_index(drop=True).drop_duplicates(subset=timestamp_column)


    def simulate(self, profile_id: int):
        master_data = self.get_master_data(profile_id)
        load_data = self.get_load_data(profile_id)
        price_data = self.get_price_data(self.start_date, self.end_date)
        wind_speed_df = self.get_weather_data(convert_plz_to_nuts(str(master_data.loc['zip_code'].values[0]))[1], self.start_date, self.end_date)
        wind_turbine = WindTurbine(rotor_radius=110, cut_in_speed=3, rated_speed=12, cut_out_speed=25)
        power_df = wind_turbine.calculate_power(wind_speed_df)
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
        
        # return all_data_df
        print(f'The Cost of  the As-Is Scenario: {all_data_df["Scenario As Is (€)"].sum()} €')
        print(f'The Cost of the PPA Scenario: {all_data_df["Scenario With PPA (€)"].sum()} €')
        
    

def main(profile_id: int, DB_URI: str):
    start_date, end_date = "2019-01-01", "2020-01-01"
    Simulator(DB_URI, start_date, end_date).simulate(profile_id)

    
if __name__ == "__main__":
    load_dotenv()
    DB_URI = os.getenv("URI")
    main(531, DB_URI)
    
