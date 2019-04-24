# IP Country Code lookup importer

This is a simple command line utility to import the [SQLite Geo-IP
database](https://dbhub.io/justinclift/Geo-IP.sqlite) into PostgreSQL.

Created this because I need to do some bulk lookups (~3.5 million) on
historical data in a PostgreSQL database.  The code I was initially
doing it with yesterday uses a mix of SQLite and PG calls, and isn't
fast enough (~120/second maximum).

With both the historical data and the country code lookup data in
PostgreSQL, it should be possible to try some approaches with a much
faster throughput.
