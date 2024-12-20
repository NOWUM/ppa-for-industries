# PPAs-For-Industries
This is a Tool for the Open-Energy-Data-Server which analyzes PPAs for various industries in Germany.
Set up your own OEDS: https://github.com/NOWUM/open-energy-data-server

install using:

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