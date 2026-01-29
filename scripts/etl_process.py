# Extract, Transform, and Load
import pandas as pd
from sqlalchemy import create_engine
import os

# 1. Setup paths and SQL connection
db_path = "project_data.db"
engine = create_engine(f"sqlite:///{db_path}")

def run_etl():
    print("Starting ETL Process")

    # --- Data Source 1: WAGE INDEX (Hagstofa) ---
    # Skip the first row because it contains a descriptive title
    wage_df = pd.read_csv('data/LAU04000_20260129-082434.csv', sep=';', skiprows=1, encoding='utf-8')
    
    # Cleaning: 1989M01 -> 1989-01-01
    wage_df['Dagsetning'] = pd.to_datetime(wage_df['Mánuður'].str.replace('M', '-'), format='%Y-%m')

    # Remove old date format
    wage_df = wage_df.drop(columns=['Mánuður'])
    
    # Ensure numbers are floats
    wage_df['Vísitölugildi'] = wage_df['Vísitölugildi'].astype(float)
    
    # --- Data Source 2: INFLATION (Seðlabanki) ---
    inflation_df = pd.read_csv('data/Verðbólga.csv', sep=';', encoding='utf-8')
    
    # Cleaning: 01.01.2009 -> 2009-01-01
    inflation_df['Dagsetning'] = pd.to_datetime(inflation_df['Dagsetning'], format='%d.%m.%Y')
    
    # Fix commas in inflation values "," -> "."
    for col in ['Vísitala neysluverðs', 'Verðbólgumarkmið']:
        inflation_df[col] = inflation_df[col].str.replace(',', '.').astype(float)
    
    # Remove old date format
    # inflation_df = inflation_df.drop(columns=['Dagsetning'])  # Redundant

    # --- PART 3: LOAD TO SQL ---
    # Rename columns in "Launavísitala" for clarity in joined table
    wage_df = wage_df.rename(columns={
    'Vísitölugildi': 'Laun_Vísitala',
    'Mánaðarbreyting, %': 'Laun_Breyting_Mán',
    'Ársbreyting, %': 'Laun_Breyting_Ár'})

    # This creates the tables automatically in your SQLite file 
    wage_df.to_sql('Launavísitala', engine, if_exists='replace', index=False)
    inflation_df.to_sql('Verðbólga', engine, if_exists='replace', index=False)

    # --- 4. Merge the tables based on date ("Dagsetning")---
    # We use the new 'Dagsetning' name for the join
    query = """
    SELECT w.*, i."Vísitala neysluverðs", i."Verðbólgumarkmið"
    FROM Launavísitala w
    INNER JOIN Verðbólga i ON w.Dagsetning = i.Dagsetning
    """

    merged_df = pd.read_sql(query, engine)
    merged_df.to_sql('Launavísitala&Verðbólga', engine, if_exists='replace', index=False)

    # Export into CSV
    merged_df.to_csv("data/merged_data.csv", index=False, sep=';', encoding='utf-8-sig')

    print(f"   - Wage Index: {len(wage_df)} rows transferred.")
    print(f"   - Inflation: {len(inflation_df)} rows transferred.")
    print("Data loaded into 'WageIndex' and 'Inflation' tables.")
    print(f"   - Merged rows: {len(merged_df)}")

if __name__ == "__main__":
    run_etl()