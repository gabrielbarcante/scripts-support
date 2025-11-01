import pandas as pd
from typing import List, Any
import os
from datetime import datetime, timedelta
import random

from ..error import NotFoundError


def read_excel(file_path: str, excel_sheet: str | int = 0, **kwargs) -> pd.DataFrame:
    """Reads an Excel file and returns a pandas DataFrame.

    Args:
        file_path (str): The path to the Excel file.
        excel_sheet (str | int, optional): The sheet name or index to read. If None, reads the first sheet. Default is 0.

    Returns:
        pd.DataFrame: DataFrame containing the data from the specified sheet.
    """

    excel = pd.ExcelFile(file_path)

    if isinstance(excel_sheet, str) and excel_sheet not in excel.sheet_names:
        raise NotFoundError(
            message=f"Sheet '{excel_sheet}' not found in file '{file_path}'."
        )

    df = pd.read_excel(excel, sheet_name=excel_sheet, **kwargs)

    return df


def query_excel(file_path: str, columns_query: List[str], values_search: List[Any], column_return: str, excel_sheet: str | int = 0, **kwargs_read_excel) -> pd.DataFrame:
    """Queries an Excel file for specific values in given columns and returns the corresponding values from another column.

    Args:
        file_path (str): The path to the Excel file.
        columns_query (List[str]): List of column names to query.
        values_search (List[str]): List of values to search for in the query columns.
        column_return (str): The column name from which to return values.
        excel_sheet (str | int, optional): The sheet name or index to read. Default is 0.

    Returns:
        pd.DataFrame: DataFrame containing the results of the query.
    """

    df = read_excel(file_path, excel_sheet, **kwargs_read_excel)

    missing_cols = [col for col in columns_query +
                    [column_return] if col not in df.columns]
    if missing_cols:
        raise NotFoundError(
            message=f"Columns not found in the Excel sheet: {', '.join(missing_cols)}")

    mask = pd.Series([True] * len(df))

    for col, val in zip(columns_query, values_search):
        mask &= df[col] == val

    result_df = df.loc[mask, [column_return]]

    return result_df


def generate_test_excel(file_path: str = "test_file.xlsx", num_rows: int = 100) -> str:
    """Generates a test Excel file with fictional data.

    Args:
        file_path (str): The path where to save the Excel file. Default is "test_file.xlsx".
        num_rows (int): Number of data rows to generate. Default is 100.

    Returns:
        str: Path of the created Excel file.
    """

    # Generating fictional data
    data = []

    for i in range(num_rows):
        data.append(
            {
                "ID": i + 1,
                "Name": f"Person {i + 1}",
                "Email": f"person{i + 1}@email.com",
                "Age": random.randint(18, 80),
                "Salary": round(random.uniform(2000, 15000), 2),
                "Department": random.choice(
                    ["IT", "HR", "Sales", "Marketing", "Finance"]
                ),
                "Hire_Date": (
                    datetime.now() - timedelta(days=random.randint(30, 3650))
                ).strftime("%Y-%m-%d"),
                "Active": random.choice([True, False]),
                "City": random.choice(
                    [
                        "New York",
                        "Los Angeles",
                        "Chicago",
                        "Houston",
                        "Phoenix",
                    ]
                ),
                "Phone": f"({random.randint(11, 99)}) {random.randint(90000, 99999)}-{random.randint(1000, 9999)}",
            }
        )

    # Creating DataFrame
    df = pd.DataFrame(data)

    # Saving to Excel
    with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
        # Main sheet with all data
        df.to_excel(writer, sheet_name="Employee_Data", index=False)

        # Sheet with statistics
        statistics = {
            "Metric": [
                "Total Employees",
                "Average Age",
                "Average Salary",
                "Active Employees",
                "Inactive Employees",
            ],
            "Value": [
                len(df),
                round(df["Age"].mean(), 2),
                round(df["Salary"].mean(), 2),
                len(df[df["Active"] == True]),
                len(df[df["Active"] == False]),
            ],
        }
        df_stats = pd.DataFrame(statistics)
        df_stats.to_excel(writer, sheet_name="Statistics", index=False)

        # Sheet with data by department
        df_department = (
            df.groupby("Department")
            .agg({"ID": "count", "Age": "mean", "Salary": "mean"})
            .round(2)
        )
        df_department.columns = ["Employees", "Average_Age", "Average_Salary"]
        df_department.to_excel(writer, sheet_name="By_Department")

    print(f"Excel file created successfully: {os.path.abspath(file_path)}")
    print(f"Number of rows generated: {num_rows}")
    print(f"Sheets created: Employee_Data, Statistics, By_Department")

    return os.path.abspath(file_path)
