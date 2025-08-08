from webapp import pandas as pd
from webapp import os
from webapp import glob
from dotenv import load_dotenv

load_dotenv()

MONTH:str = os.getenv("MONTH")
YEAR:str = os.getenv("YEAR")

def filenames_to_dates(PATH):
    files = list(filter(os.path.isfile, glob.glob(PATH + "\\*")))
    files.sort(key=os.path.getctime)
    
    for file in files:
        DAY = str(files.index(file) + 1)
        if file.split("\\")[5] != "Southstar":
            # os.rename(file, rf"{PATH}\{MONTH}.{DAY.zfill(2)}.{YEAR}.xlsx")
            os.rename(file, rf"{PATH}\{MONTH}.{DAY}.{YEAR}.xlsx")
        else:
            # os.rename(file, rf"{PATH}\{MONTH}.{DAY.zfill(2)}.{YEAR}.xls")
            os.rename(file, rf"{PATH}\{MONTH}.{DAY}.{YEAR}.xls")