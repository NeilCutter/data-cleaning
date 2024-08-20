from webapp import app, render_template, request, flash, redirect, url_for
from webapp import pandas as pd
from webapp import glob
from webapp import warnings
from webapp import data_loading
from webapp import os
import xml.etree.ElementTree as et


@app.route("/")
def home_page():
    return render_template("index.html")

@app.route("/robinsons")
def robinsons():
     return render_template("robinsons_data.html")

@app.route("/uncle_john")
def uncle_john():
    return render_template("uncle_john_data.html")

@app.route("/uj_transfers")
def uj_transfers():
    return render_template("uj_transfers.html")

@app.route("/sm")
def sm():
    return render_template("sm_data.html")

@app.route("/eleven")
def eleven():
    return render_template("eleven_data.html")

@app.route("/scan_and_outbound")
def scan_and_outbound():
    return render_template("scan_and_outbound.html")

@app.route("/waltermart")
def waltermart():
    return render_template("waltermart_data.html")

@app.route("/southstar")
def southstar():
    return render_template("southstar_data.html")

@app.route("/ssd_transfers")
def ssd_transfers():
    return render_template("ssd_transfers.html")

@app.route("/robinsons_data", methods=["GET", "POST"])
def robinsons_cleaning():
    try:
        path = request.form["path"]
        destination = request.form["destination"]
        dataset = []
        
        file_name = glob.glob1(path, "*.xlsx")
        # Loading data and cleaning
        for file in file_name:
            df = pd.read_excel(rf"{path}\{file}", skipfooter=2, engine="openpyxl")

            # Skip headers
            df = df.set_index("Unnamed: 0")
            num_row = df.index.get_loc("SKU CODE")
            df = df.reset_index()
            df.iloc[:num_row].index.tolist()
            df = df.drop(df.iloc[:num_row].index.tolist())
            new_header = df.iloc[0]
            df.columns = new_header
            df = df.reset_index(drop=True)
            df = df.drop([0, 1])

            # Data type Convertion
            convert_dtype = {
                "SKU CODE": str,
                "UPC": str,
                "STORE CODE": str,
                "UNITS SOLD TY": int,
                "NET SALES TY": float,
                "TAX TY": float,
                "GROSS SALES TY": float,
            }
            df = df.astype(convert_dtype)

            # Adding new column df[DATE]
            month = file.split(".")[0]
            day = file.split(".")[1]
            year = file.split(".")[2]
            df["DATE"] = f"{month}-{day}-{year}"
            dataset.append(df)

        df = pd.concat(dataset)
        columns = df.columns.tolist()

        # Adding additional columns for partition
        df["month"] = pd.DatetimeIndex(df["DATE"]).month
        df["year"] = pd.DatetimeIndex(df["DATE"]).year

        years = df["year"].unique().tolist()
        months = df["month"].unique().tolist()

        # Exporting cleaned data to excel
        data_loading.export_to_excel(df, years, months, destination, columns)

    except (OSError, FileNotFoundError, UnboundLocalError, ValueError):
        flash("Invalid Input. Please enter the correct PATH location.", 'error')
        return redirect(url_for("robinsons"))
    except PermissionError:
        print("Missing curly braces")
    else:
        flash("Cleaning completed without issues", "info")
        return redirect(url_for("robinsons"))
    return render_template("robinsons_data.html")


@app.route("/uncle_john_data", methods=["GET", "POST"])
def uncle_john_cleaning():
    try:
        path = request.form["path"]
        destination = request.form["destination"]
        dataset = []
        
        file_name = glob.glob1(path, "*.xlsx")
        # Loading data and cleaning
        for file in file_name:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                df = pd.read_excel(
                    rf"{path}\{file}", skipfooter=2, engine="openpyxl"
                )

                # Skip headers
                df = df.set_index("Unnamed: 0")
                num_row = df.index.get_loc("SKU CODE")
                df = df.reset_index()
                df.iloc[:num_row].index.tolist()
                df = df.drop(df.iloc[:num_row].index.tolist())
                new_header = df.iloc[0]
                df.columns = new_header
                df = df.reset_index(drop=True)
                df = df.drop([0, 1])

                # Data type Convertion
                convert_dtype = {
                    "SKU CODE": str,
                    "UPC": str,
                    "STORE CODE": str,
                    "UNITS SOLD TY": int,
                    "NET SALES TY": float,
                    "TAX TY": float,
                    "GROSS SALES TY": float,
                }
                df = df.astype(convert_dtype)

                # Adding new column df[DATE]
                month = file.split(".")[0]
                day = file.split(".")[1]
                year = file.split(".")[2]
                df["DATE"] = f"{month}-{day}-{year}"
                dataset.append(df)

        df = pd.concat(dataset)
        columns = df.columns.tolist()

        # Adding additional columns for partition
        df["month"] = pd.DatetimeIndex(df["DATE"]).month
        df["year"] = pd.DatetimeIndex(df["DATE"]).year

        years = df["year"].unique().tolist()
        months = df["month"].unique().tolist()

        # Exporting cleaned data to excel
        data_loading.export_to_excel(df, years, months, destination, columns)

    except (OSError, FileNotFoundError, UnboundLocalError, ValueError):
        flash("Invalid Input. Please enter the correct PATH location.", 'error')
        return redirect(url_for("uncle_john"))
    except PermissionError:
        print("Missing curly braces")
    else:
        flash("Cleaning completed without issues", "info")
        return redirect(url_for("uncle_john"))
    return render_template("uncle_john_data.html")


@app.route("/sm_data", methods=["GET", "POST"])
def sm_cleaning():
    path = request.form["path"]
    destination = request.form["destination"]
    file_name = glob.glob1(path, "*.xml")

    dataset = []

    try:
        # Loading data and cleaning
        for file in file_name:
            tree = et.parse(rf"{path}\{file}")
            root = tree.getroot()
            for document in root.findall(".//document"):
                for article in document.find(".//details").findall(".//article"):
                    data = {
                        "CompanyName": document.find(".//header")
                        .find(".//CompanyName")
                        .text,
                        "DocumentType": document.find(".//header")
                        .find(".//DocumentType")
                        .text,
                        "PostDate": document.find(".//header").find(".//PostDate").text,
                        "PostTime": document.find(".//header").find(".//PostTime").text,
                        "VendorCode": document.find(".//header")
                        .find(".//VendorCode")
                        .text,
                        "VendorName": document.find(".//header")
                        .find(".//VendorName")
                        .text,
                        "TransactDate": document.find(".//header")
                        .find(".//TransactDate")
                        .text,
                        "Note": document.find(".//header").find(".//Note").text,
                        "ArticleNumber": article.find(".//ArticleNumber").text,
                        "BarcodeDescription": article.find(
                            ".//BarcodeDescription"
                        ).text,
                        "UOM": article.find(".//UOM").text,
                        "Qty": article.find(".//Qty").text,
                        "NVAT": article.find(".//NVAT").text,
                        "VAT": article.find(".//VAT").text,
                        "TOTAL": article.find(".//TOTAL").text,
                        "TotalAmount": document.find(".//footer")
                        .find(".//TotalAmount")
                        .text,
                        "SiteCode": document.find(".//footer").find(".//SiteCode").text,
                        "SiteName": document.find(".//footer").find(".//SiteName").text,
                        "ImportantRemarks": document.find(".//footer")
                        .find(".//ImportantRemarks")
                        .text,
                    }
                    dataset.append(data)
            df = pd.DataFrame(dataset)
            df.sort_values(by=["TransactDate"], inplace=True)

            # Data type Convertion
            df["Qty"] = df["Qty"].replace(r"\.0+", "", regex=True)
            df["VAT"] = df["VAT"].replace(r"\,", "", regex=True)
            df["NVAT"] = df["NVAT"].replace(r"\,", "", regex=True)
            df["TOTAL"] = df["TOTAL"].replace(r"\,", "", regex=True)
            df["TotalAmount"] = df["TotalAmount"].replace(r"\,", "", regex=True)

            convert_dtype = {
                "Qty": int,
                "NVAT": float,
                "VAT": float,
                "TOTAL": float,
                "TotalAmount": float,
            }
            df = df.astype(convert_dtype)

            # Adding additional columns for partition
            columns = df.columns.tolist()
            df["year"] = pd.DatetimeIndex(df["TransactDate"]).year
            df["month"] = pd.DatetimeIndex(df["TransactDate"]).month

            months = df["month"].unique().tolist()
            years = df["year"].unique().tolist()

        # Exporting cleaned data to excel
        data_loading.export_to_excel(df, years, months, destination, columns)

    except (OSError, FileNotFoundError, UnboundLocalError, ValueError):
        flash("Invalid Input. Please enter the correct PATH location.", 'error')
        return redirect(url_for("sm"))
    except PermissionError:
        print("Missing curly braces")
    else:
        flash("Cleaning completed without issues", "info")
        return redirect(url_for("sm"))
    return render_template("sm_data.html")


@app.route("/eleven_data", methods=["GET", "POST"])
def eleven_cleaning():
    path = request.form["path"]
    destination = request.form["destination"]
    file_name = glob.glob1(path, "*.xlsx")

    try:
        # Loading data
        appended_data = []
        for file in file_name:
            df = pd.read_excel(rf"{path}\{file}", engine="openpyxl")
            appended_data.append(df)

        if len(appended_data) != 0:
            df = pd.concat(appended_data)

        df.sort_values(by=["transactiondate"], inplace=True)
        columns = df.columns.tolist()

        # Adding additional columns for partition
        df["year"] = pd.DatetimeIndex(df["transactiondate"]).year
        df["month"] = pd.DatetimeIndex(df["transactiondate"]).month

        months = df["month"].unique().tolist()
        years = df["year"].unique().tolist()

        # Exporting cleaned data to excel
        data_loading.export_to_excel(df, years, months, destination, columns)

    except (OSError, FileNotFoundError, UnboundLocalError, ValueError):
        flash("Invalid Input. Please enter the correct PATH location.", 'error')
        return redirect(url_for("eleven"))
    except PermissionError:
        print("Missing curly braces")
    else:
        flash("Cleaning completed without issues", "info")
        return redirect(url_for("eleven"))
    return render_template("eleven_data.html")

@app.route("/waltermart_data", methods=["GET", "POST"])
def waltermart_cleaning():
    try:
        path = request.form["path"]
        destination = request.form["destination"]
        file_name = os.listdir(path)
        dataset = []

        for file in file_name:
            df_1 = pd.read_xml(rf"{path}\{file}", xpath=".//header")
            df_2 = pd.read_xml(rf"{path}\{file}", xpath=".//article")
            df_3 = pd.read_xml(rf"{path}\{file}", xpath=".//footer")
            df_4 = pd.concat([df_1, df_2], axis=1)
            df = pd.concat([df_4, df_3], axis=1)

            dataset.append(df)

            df = pd.concat(dataset)
            df.sort_values(by=["TransactDate"], inplace=True)

            df["Qty"] = df["Qty"].replace(r"\.0+", "", regex=True)
            df["TOTAL"] = df["TOTAL"].replace(r"\,", "", regex=True)
            df["TotalAmount"] = df["TotalAmount"].replace(r"\,", "", regex=True)

            convert_dtype = {"Qty": int,
                            "TOTAL": float,
                            "TotalAmount": float}

            df = df.astype(convert_dtype)
            orig_columns = df.columns.tolist()
            column_names = ['Company Name','Document Title','Date','Time','Vendor Code','Vendor Name','Transaction Date','Note',
                            'SKU#','BarcodeDescription','UOM','Qty/Kilo','Sales Amount','Total','Site Code','Site Name','ImportantRemarks']
            df.rename(columns=dict(zip(orig_columns, column_names)), inplace=True)

            df["Barcode"] = df["BarcodeDescription"].apply(lambda x: x.split()[0])
            df["Description"] = df["BarcodeDescription"].replace(regex=r'^\d+\s+', value="")

            columns = ['Company Name', 'Document Title', 'Date', 'Time', 'Vendor Name', 'Vendor Code', 'Transaction Date', 
           'Note', 'SKU#','Barcode', 'Description', 'UOM', 'Qty/Kilo', 'Sales Amount', 'Site Code', 'Site Name', 'Total']

            df["year"] = pd.DatetimeIndex(df['Transaction Date']).year
            df["month"] = pd.DatetimeIndex(df['Transaction Date']).month

            years = df["year"].unique().tolist()
            months = df["month"].unique().tolist()

            data_loading.export_to_excel(df, years, months, destination, columns)

    except (OSError, FileNotFoundError, UnboundLocalError, ValueError):
        flash("Invalid Input. Please enter the correct PATH location.", 'error')
        return redirect(url_for("waltermart"))
    except PermissionError:
        print("Missing curly braces")
    else:
        flash("Cleaning completed without issues", "info")
        return redirect(url_for("waltermart"))
    return render_template("waltermart_data.html")


@app.route("/southstar_data", methods=["GET", "POST"])
def southstar_cleaning():
    try:
        path = request.form["path"]
        destination = request.form["destination"]    
        file_name = glob.glob1(path, "*.xls")

        dataset = []
        # Loading data and cleaning
        for file in file_name:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                df = pd.read_excel(
                    rf"{path}\{file}", skipfooter=2
                )

                # Skip headers
                df = df.set_index("Unnamed: 0")
                num_row = df.index.get_loc("Product Code\n")
                df = df.reset_index()
                df.iloc[:num_row].index.tolist()
                df = df.drop(df.iloc[:num_row].index.tolist())
                new_header = df.iloc[0]
                df.columns = new_header
                df.reset_index(drop=True, inplace=True)
                df = df.drop([0, 1])


                orig_columns = df.columns.tolist()
                new_columns = [column.replace("\n", " ").strip() for column in orig_columns]
                df.rename(columns=dict(zip(orig_columns, new_columns)), inplace = True)

                # Data type Convertion
                convert_dtype = {
                    "Units Sold TY": int,
                    "Units Sold LY": int,
                    "Net Sales TY (Ex-VAT)": float,
                    "Net Sales LY (Ex-VAT)": float,
                }
                df = df.astype(convert_dtype)

                # Adding new column df[DATE]
                month = file.split(".")[0]
                day = file.split(".")[1]
                year = file.split(".")[2]
                df["DATE"] = f"{month}-{day}-{year}"
                dataset.append(df)

        df = pd.concat(dataset)
        columns = df.columns.tolist()

        # Adding additional columns for partition
        df["month"] = pd.DatetimeIndex(df["DATE"]).month
        df["year"] = pd.DatetimeIndex(df["DATE"]).year

        years = df["year"].unique().tolist()
        months = df["month"].unique().tolist()

        # Exporting cleaned data to excel
        data_loading.export_to_excel(df, years, months, destination, columns)

    except (OSError, FileNotFoundError, UnboundLocalError, ValueError):
        flash("Invalid Input. Please enter the correct PATH location.", 'error')
        return redirect(url_for("southstar"))
    except PermissionError:
        print("Missing curly braces")
    else:
        flash("Cleaning completed without issues", "info")
        return redirect(url_for("southstar"))
    return render_template("southstar_data.html")


@app.route("/ssd_sku_transfers", methods=["GET", "POST"])
def ssd_sku_transfers():
    try:
        path = request.form["path"]
        destination = request.form["destination"]    
        file_name = glob.glob1(path, "*.xls")

        dataset = []

        for file in file_name:
            df = pd.read_excel(rf"{path}\{file}", skipfooter=2)
            # Skip headers
            df = df.set_index("Unnamed: 0")
            num_row = df.index.get_loc("Product Code\n")
            df = df.reset_index()
            df.iloc[:num_row].index.tolist()
            df = df.drop(df.iloc[:num_row].index.tolist())
            new_header = df.iloc[0]
            df.columns = new_header
            df = df.reset_index(drop=True)
            df = df.drop([0, 1])

            month = file.split(".")[0]
            day = file.split(".")[1]
            year = file.split(".")[2]
            df["DATE"] = f"{month}-{day}-{year}"
            dataset.append(df)

        df = pd.concat(dataset)        
        orig_columns = df.columns.tolist()
        new_columns = [column.replace("\n", " ").strip() for column in orig_columns]
        df.rename(columns=dict(zip(orig_columns, new_columns)), inplace = True)

        columns = df.columns.tolist()

        # Adding additional columns for partition
        df["month"] = pd.DatetimeIndex(df["DATE"]).month
        df["year"] = pd.DatetimeIndex(df["DATE"]).year

        years = df["year"].unique().tolist()
        months = df["month"].unique().tolist()
        
        data_loading.export_to_excel(df, years, months, destination, columns)

    except (OSError, FileNotFoundError, UnboundLocalError, ValueError):
        flash("Invalid Input. Please enter the correct PATH location.", 'error')
        return redirect(url_for("ssd_transfers"))
    except PermissionError:
        print("Missing curly braces")
    else:
        flash("Cleaning completed without issues", "info")
        return redirect(url_for("ssd_transfers"))
    return render_template("ssd_transfers.html")

@app.route("/uj_sku_transfers", methods=["GET", "POST"])
def uj_sku_transfers():
    path = request.form["path"]
    destination = request.form["destination"]

    try:
        df = pd.read_excel(rf"{path}", skipfooter=2, engine="openpyxl")

        # Skip headers
        df = df.set_index("Unnamed: 0")
        num_row = df.index.get_loc("DEPARTMENT CODE")
        df = df.reset_index()
        df.iloc[:num_row].index.tolist()
        df = df.drop(df.iloc[:num_row].index.tolist())
        new_header = df.iloc[0]
        df.columns = new_header
        df = df.reset_index(drop=True)
        df = df.drop([0, 1])


        df.to_excel(rf"{destination}\Vendor SKU Transfer Department.xlsx", index=False)

    except (OSError, FileNotFoundError, UnboundLocalError, ValueError):
        flash("Invalid Input. Please enter the correct PATH location.", 'error')
        return redirect(url_for("uj_transfers"))
    except PermissionError:
        print("Missing curly braces")
    else:
        flash("Cleaning completed without issues", "info")
        return redirect(url_for("uj_transfers"))
    return render_template("uj_transfers.html")

@app.route("/supplier_scan_and_outbound", methods=["GET", "POST"])
def supplier_scan_and_outbound():
    path = request.form["path"]
    destination = request.form["destination"]
    supplier = request.form["supplier"]
    file_name = glob.glob1(path, "*.xlsx")
    dataset = []
    
    print(supplier)
    try:
        for file in file_name:
            df = pd.read_excel(rf"{path}\{file}")
            dataset.append(df)
        
        df = pd.concat(dataset)

        if supplier == "Outbound":
            df.to_excel(rf"{destination}\Supplier_Outbound.xlsx", index=False)
        elif supplier == "Scan":
            df.to_excel(rf"{destination}\SupplierOSAwithDeliveryandSales.xlsx", index=False)
        else:
            flash("Please select the right file type for this cleaning", 'error')
            return redirect(url_for("scan_and_outbound"))

    except (OSError, FileNotFoundError, UnboundLocalError, ValueError):
        flash("Invalid Input. Please enter the correct PATH location.", 'error')
        return redirect(url_for("scan_and_outbound"))
    except PermissionError:
        print("Missing curly braces")
    else:
        flash("Cleaning completed without issues", "info")
        return redirect(url_for("scan_and_outbound"))
    return render_template("scan_and_outbound.html")




    
