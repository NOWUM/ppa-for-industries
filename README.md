# PPAs-For-Industries
This is a Tool for the Open-Energy-Data-Server which analyzes PPAs for various industries in Germany.
Set up your own OEDS: https://github.com/NOWUM/open-energy-data-server

Install using:

```
pip install -e .
```
SQL Queries for OEDS:
```SQL
SELECT "Hersteller", COUNT("Hersteller") FROM mastr."EinheitenWind"
WHERE "Inbetriebnahmedatum" > '2022-12-31'
GROUP BY("Hersteller")
ORDER BY count desc
```
This shows you that Vestas (1660) is the most used manufacturer.
```SQL
SELECT "Typenbezeichnung", "Nettonennleistung", COUNT("Typenbezeichnung"), "Nabenhoehe", "Rotordurchmesser"
FROM mastr."EinheitenWind"
WHERE "Inbetriebnahmedatum" > '2022-12-31'
AND "Hersteller" = 1660
GROUP BY("Typenbezeichnung", "Nettonennleistung", "Nabenhoehe", "Rotordurchmesser")
ORDER BY count desc
```
This shows that the Vestas V164/9500 (9.5MW) is the most new built turbine.

Prior to running this package make sure to create a results table in your OEDS database, so that the simulation results get saved properly:
```SQL
CREATE TABLE vea_results_timeseries.ppa_results
(
timestamp TIMESTAMP NOT NULL,
wind_speed REAL,
roughness_length REAL,
"actual_power_single_turbine(w)" REAL,
price REAL,
"actual_power_single_turbine(mwh)" REAL,
"actual_power_needed_turbines(mwh)" REAL,
"market_value_single_turbine(€)" REAL,
"market_value_needed_turbines(€)" REAL,
"load(kwh)" REAL,
"load(mwh)" REAL,
"ppa_surplus(mwh)" REAL,
"scenario_as_is_0.9(€)" REAL,
"scenario_as_is_0.95(€)" REAL,
"scenario_as_is_0.98(€)" REAL,
"scenario_as_is_1(€)" REAL,
"scenario_as_is_1.02(€)" REAL,
"scenario_as_is_1.05(€)" REAL,
"scenario_as_is_1.1(€)" REAL,
"scenario_with_ppa_0.9(€)" REAL,
"scenario_with_ppa_0.95(€)" REAL,
"scenario_with_ppa_0.98(€)" REAL,
"scenario_with_ppa_1(€)" REAL,
"scenario_with_ppa_1.02(€)" REAL,
"scenario_with_ppa_1.05(€)" REAL,
"scenario_with_ppa_1.1(€)" REAL,
plz VARCHAR(10),
nuts_id VARCHAR(20),
profile_id INTEGER NOT NULL,
sector_group_id VARCHAR(10),
sector_group VARCHAR(100),
PRIMARY KEY (timestamp, profile_id)
);
```

```SQL
SELECT
    profile_id,
    ROUND(SUM("scenario_as_is_1(€)")::numeric, 2) AS scenario_as_is,
    ROUND(SUM("scenario_with_ppa_1(€)")::numeric, 2) AS scenario_ppa,
    ROUND(ABS(SUM("scenario_as_is_1(€)")::numeric - SUM("scenario_with_ppa_1(€)")::numeric), 2) AS difference
FROM vea_results_timeseries.ppa_results
GROUP BY profile_id
HAVING ROUND(SUM("scenario_as_is_1(€)")::numeric, 2) <> ROUND(SUM("scenario_with_ppa_1(€)")::numeric, 2)
ORDER BY difference DESC;
```