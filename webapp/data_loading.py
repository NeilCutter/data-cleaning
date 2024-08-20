from webapp import pandas as pd

MONTH_NAME = {
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
                rf"{destination}\{MONTH_NAME[month]}-{year}.xlsx",
                index=False,
                columns=columns,
            )
