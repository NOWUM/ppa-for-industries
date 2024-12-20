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
        data = data.rename(columns={'value': 'Load (kWh)'})
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
    
class WindTurbine:
    """
    A class representing a wind turbine.

    Attributes:
    ----------
    rotor_radius : float
        The radius of the wind turbine's rotor.
    cut_in_speed : float
        The minimum wind speed required for the turbine to start generating power.
    rated_speed : float
        The wind speed at which the turbine generates its rated power.
    cut_out_speed : float
        The maximum wind speed at which the turbine can operate.
    air_density : float, optional
        The air density (default is 1.225 kg/m^3).
    efficiency : float, optional
        The efficiency of the turbine (default is 0.4).
    """
    def __init__(self, rotor_radius, cut_in_speed, rated_speed, cut_out_speed, air_density=1.225, efficiency=0.4):
        """
        Initializes the WindTurbine with its parameters.

        Parameters:
        ----------
        rotor_radius : float
            The radius of the wind turbine's rotor.
        cut_in_speed : float
            The minimum wind speed required for the turbine to start generating power.
        rated_speed : float
            The wind speed at which the turbine generates its rated power.
        cut_out_speed : float
            The maximum wind speed at which the turbine can operate.
        air_density : float, optional
            The air density (default is 1.225 kg/m^3).
        efficiency : float, optional
            The efficiency of the turbine (default is 0.4).
        """
        self.rotor_radius = rotor_radius 
        self.cut_in_speed = cut_in_speed
        self.rated_speed = rated_speed
        self.cut_out_speed = cut_out_speed
        self.air_density = air_density
        self.efficiency = efficiency
        self.area = math.pi * (rotor_radius ** 2)
        self.rated_power = self.efficiency * 0.5 * self.air_density * self.area * (self.rated_speed ** 3)

    def calculate_power(self, wind_speed_df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculates the power generated by the wind turbine based on the wind speed.

        Parameters:
        ----------
        wind_speed_df : pd.DataFrame
            A DataFrame containing the wind speed data.

        Returns:
        -------
        pd.DataFrame
            A DataFrame containing the calculated power data.
        """
        wind_speed_df['Theoretical Power (W)'] = 0.5 * self.air_density * self.area * (wind_speed_df['wind_speed'] ** 3)
        wind_speed_df['Actual Power (W)'] = self.efficiency * wind_speed_df['Theoretical Power (W)']
        wind_speed_df.loc[wind_speed_df['wind_speed'] < self.cut_in_speed, 'Actual Power (W)'] = 0
        wind_speed_df.loc[wind_speed_df['wind_speed'] > self.cut_out_speed, 'Actual Power (W)'] = 0
        wind_speed_df.loc[(wind_speed_df['wind_speed'] >= self.rated_speed) & 
                          (wind_speed_df['wind_speed'] <= self.cut_out_speed), 'Actual Power (W)'] = self.rated_power
        return wind_speed_df
    
    def calculate_market_value(self, power_and_price_df):
        """
        Calculates the market value of the wind turbine's power generation.

        Parameters:
        ----------
        power_and_price_df : pd.DataFrame
            A DataFrame containing the power and price data.

        Returns:
        -------
        pd.DataFrame
            A DataFrame containing the calculated market value data.
        """
        power_and_price_df['Actual Power (MWh)'] = power_and_price_df['Actual Power (W)'] * 1e-6
        power_and_price_df['Market Value (€)'] = power_and_price_df['Actual Power (MWh)'] * power_and_price_df['price']
        return power_and_price_df
    
class PowerPurchaseAgreement:
    """
    A class representing a Power Purchase Agreement (PPA).

    Attributes:
    ----------
    fixed_energy_price : float
        The fixed energy price of the PPA.
    """
    def __init__(self, market_value: pd.DataFrame):
        """
        Initializes the PowerPurchaseAgreement with the market value data.

        Parameters:
        ----------
        market_value : pd.DataFrame
            A DataFrame containing the market value data.
        """
        self.fixed_energy_price = self.calculate_average_value(market_value)
    
    def calculate_average_value(self, market_value: pd.DataFrame) -> float:
        """
        Calculates the average value of the market value data.

        Parameters:
        ----------
        market_value : pd.DataFrame
            A DataFrame containing the market value data.

        Returns:
        -------
        float
            The average value of the market value data.
        """
        return market_value['Market Value (€)'].sum() / market_value['Actual Power (MWh)'].sum()
    
    
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
        
        print(f'The Cost of  the As-Is Scenario: {all_data_df["Scenario As Is (€)"].sum()} €')
        print(f'The Cost of the PPA Scenario: {all_data_df["Scenario With PPA (€)"].sum()} €')
        
    

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
    main(531, DB_URI)
