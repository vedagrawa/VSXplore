#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

@author: vedagrawal
"""

import pandas as pd
import sqlite3

class ExoplanetAPI:
    
    con = None

    @staticmethod
    def connect(dbfile):
        """
        Establish a connection to the SQLite database file.
        
        Parameters:
        - dbfile: The path to the SQLite database file.
        """
        # Store the connection in the class variable `con`.
        ExoplanetAPI.con = sqlite3.connect(dbfile)

    @staticmethod
    def execute(query, debug=False):
        """
        Execute a SQL query and return the results in a pandas DataFrame.
        
        Parameters:
        - query: The SQL query string to be executed.
        - debug: If True, print the query before execution (default: False).
        """
        # If debugging, print the query.
        if debug:
            print("Running: ", query)
        # Use pandas to execute the query and return the results as a DataFrame.
        return pd.read_sql_query(query, ExoplanetAPI.con)

    @staticmethod
    def get_exoplanets(columns='*', discovery_method=None, disc_year=None,
                       orbital_period=None, host_star_vmag=None,
                       planet_radius=None, host_star_temp=None, limit=0, order_by=None):
        """
        Get selected exoplanet records based on a variety of filters.
        
        Parameters:
        - columns: Columns to be included in the output (default: '*').
        - discovery_method: Filter for the discovery method of the exoplanet.
        - disc_year: Filter for the year the exoplanet was discovered.
        - orbital_period: Tuple with minimum and maximum orbital period to filter by.
        - host_star_vmag: Tuple with minimum and maximum visual magnitude of the host star.
        - planet_radius: Tuple with minimum and maximum planet radius to filter by.
        - host_star_temp: Tuple with minimum and maximum temperature of the host star.
        - limit: Maximum number of records to return (default: 0, which means no limit).
        - order_by: Column name to order the results by.
        """
       
        # Initialize an empty list to hold the WHERE clause conditions.
        where_clauses = []
       
        # Start building the SQL query string.
        query = "SELECT {} FROM exoplanets".format(columns)


        if discovery_method:
            where_clauses.append(f'"Discovery Method" = "{discovery_method}"')
        if disc_year:
            where_clauses.append(f'"Discovery Year" = {disc_year}')
        if orbital_period:
            min_op, max_op = orbital_period
            if min_op:
                where_clauses.append(f'"Orbital Period [days]" >= {min_op}')
            if max_op:
                where_clauses.append(f'"Orbital Period [days]" <= {max_op}')
        if host_star_vmag:
            min_vmag, max_vmag = host_star_vmag
            if min_vmag:
                where_clauses.append(f'"Host Star V mag" >= {min_vmag}')
            if max_vmag:
                where_clauses.append(f'"Host Star V mag" <= {max_vmag}')
        if planet_radius:
            min_radius, max_radius = planet_radius
            if min_radius:
                where_clauses.append(f'"Planet Radius [Earth Radius]" >= {min_radius}')
            if max_radius:
                where_clauses.append(f'"Planet Radius [Earth Radius]" <= {max_radius}')
        if host_star_temp:
            min_temp, max_temp = host_star_temp
            if min_temp:
                where_clauses.append(f'"Stellar Effective Temperature [K]" >= {min_temp}')
            if max_temp:
                where_clauses.append(f'"Stellar Effective Temperature [K]" <= {max_temp}')

        # Combine all WHERE clause conditions with "AND" and append to the query.
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)

        # Append ORDER BY clause if provided.
        if order_by:
            query += f' ORDER BY "{order_by}"'

        # Append LIMIT clause if provided.
        if limit > 0:
            query += f" LIMIT {limit}"

        # Execute the final query and return the results.
        return ExoplanetAPI.execute(query)

    @staticmethod
    def get_discovery_methods():
        """
        Get all unique Discovery Methods in the database.
        
        Returns a DataFrame with all unique Discovery Methods.
        """
        query = 'SELECT DISTINCT "Discovery Method" FROM exoplanets'
        return ExoplanetAPI.execute(query)

    

# Usage example:
db_path = '/Users/vedagrawal/Downloads/exoplanet25.db'

# Connect to the database using the provided path.
ExoplanetAPI.connect(db_path)

# Example call to get exoplanets discovered by the Transit method 
transit_planets = ExoplanetAPI.get_exoplanets(discovery_method='Transit')

# Print the resulting DataFrame to see the fetched data.
print(transit_planets)
