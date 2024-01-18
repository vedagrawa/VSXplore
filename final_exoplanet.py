#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 17 20:22:18 2023

@author: vedagrawal
"""

# import statements
import pandas as pd
import sqlite3

# Define a dictionary with the relevant columns to keep

columns_to_keep = {
    'pl_name': 'Planet Name',
    'hostname': 'Host Name',
    'discoverymethod': 'Discovery Method',
    'disc_year': 'Discovery Year',
    'sy_snum': 'Number of Stars',
    'sy_pnum': 'Number of Planets',
    'pl_orbper': 'Orbital Period [days]',
    'pl_orbsmax': 'Orbit Semi-Major Axis [au]',
    'pl_rade': 'Planet Radius [Earth Radius]',
    'pl_bmasse': 'Planet Mass or Mass*sin(i) [Earth Mass]',
    'pl_orbeccen': 'Eccentricity',
    'pl_insol': 'Insolation Flux [Earth Flux]',
    'pl_eqt': 'Equilibrium Temperature [K]',
    'st_spectype': 'Spectral Type',
    'st_teff': 'Stellar Effective Temperature [K]',
    'st_rad': 'Stellar Radius [Solar Radius]',
    'st_mass': 'Stellar Mass [Solar mass]',
}

# Define a function to select and rename columns

def select_and_rename_columns(dataframe):
    filtered_data = dataframe[list(columns_to_keep.keys())]
    filtered_data = filtered_data.rename(columns=columns_to_keep)
    return filtered_data

# Read the raw data (Replace 'exoplanets.csv' with the actual file path)

exoplanets = pd.read_csv('exoplanet1.csv', low_memory=False)

# Apply the select_and_rename_columns function

filtered_exoplanets = select_and_rename_columns(exoplanets)

# SQLite Database for the filtered dataset (Change the database file name as needed)

con = sqlite3.connect('exoplanet25.db')

filtered_exoplanets.to_sql("exoplanets", con, if_exists='replace')

# Read data from the filtered database for validation

exoplanets_from_db = pd.read_sql_query("SELECT * FROM exoplanets", con)


