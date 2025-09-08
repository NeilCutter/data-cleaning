from webapp import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import os
from dotenv import load_dotenv

load_dotenv()

USERNAME:str = os.getenv("DB_USER")
PASSWORD:str = os.getenv("DB_PASS")
HOST:str = os.getenv("DB_HOST")
PORT:int = os.getenv("DB_PORT")
DATABASE:str = os.getenv("DB_NAME")
MONTH:int = int(os.getenv("MONTH"))
YEAR:int = int(os.getenv("YEAR"))

connection_url = URL.create(
    "mssql+pyodbc",
    username=USERNAME,
    password=PASSWORD,
    host=HOST,
    port=PORT,
    database=DATABASE,
    query={
        "driver": "ODBC Driver 17 for SQL Server",
        "TrustServerCertificate": "yes",
    }
)
engine = create_engine(connection_url)

MONTH_NAMES = {
    1: "January",
    2: "February",
    3: "March",
    4: "April",
    5: "May",
    6: "June",
    7: "July",
    8: "August",
    9: "September",
    10: "October",
    11: "November",
    12: "December",
}

def export_to_excel(df, years, months, destination, columns):
    for year in years:
        for month in months:
            df[(df["year"] == year) & (df["month"] == month)].to_excel(
                rf"{destination}\{MONTH_NAMES[month]}-{year}.xlsx",
                index=False,
                columns=columns
            )
    df = df[(df["year"] == YEAR) & (df["month"] == MONTH)]
    df = df[columns]

    if destination.split("\\")[6] == "sales":
        df.to_sql("OFFTAKE_ROBINSON_RAW_DAILY", con=engine, index=False, if_exists="append")
    elif destination.split("\\")[6] == "ecom":
        df.to_sql("OFFTAKE_ROBINSON-ECOM_RAW_DAILY", con=engine, index=False, if_exists="append")
    elif destination.split("\\")[6] == "GRANEX":
        df.to_sql("OFFTAKE_ROBINSON_RAW_DAILY", con=engine, index=False, if_exists="append")
    elif destination.split("\\")[5] == "ssd":
        df.to_sql("OFFTAKE_SSD_RAW_DAILY", con=engine, index=False, if_exists="append")
    elif destination.split("\\")[5] == "uj":
        df.to_sql("OFFTAKE_UNCLE-JOHNS_DAILY", con=engine, index=False, if_exists="append")
    elif destination.split("\\")[5] == "sm":
        df.to_sql("OFFTAKE_SM_RAW_DAILY", con=engine, index=False, if_exists="append")
    elif destination.split("\\")[5] == "waltermart":
        df.to_sql("WALTERMART_DSR", con=engine, index=False, if_exists="append")
    elif destination.split("\\")[7] == "sales":
        df.to_sql("OFFTAKE_711_RAW", con=engine, index=False, if_exists="append")
    else:
        print("No Equivalent File")
        
        
    # if destination.split("\\")[6] == "sales":
    #     df.to_sql("OFFTAKE_ROBINSON_RAW_DAILY_ADJ", con=engine, index=False, if_exists="append")
    # elif destination.split("\\")[6] == "ecom":
    #     df.to_sql("OFFTAKE_ROBINSON-ECOM_RAW_DAILY", con=engine, index=False, if_exists="append")
    # elif destination.split("\\")[6] == "GRANEX":
    #     df.to_sql("OFFTAKE_ROBINSON_RAW_DAILY_ADJ", con=engine, index=False, if_exists="append")
    # else:
    #     print("No Equivalent File")