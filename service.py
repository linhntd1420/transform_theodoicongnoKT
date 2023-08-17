import config_info
import clickhouse_connect
import pandas as pd
import re
from oauth2client.service_account import ServiceAccountCredentials
import gspread


def connect_worksheet(sheet_name):
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]

    credentials = ServiceAccountCredentials.from_json_keyfile_name("ggsheet_access.json",
                                                                   scopes)
    gg_sheet = gspread.authorize(credentials)
    file_ggsheet = gg_sheet.open_by_url(
        "https://docs.google.com/spreadsheets/d/1JvztCFt1y1jGUeNNd-V0Utg-UWViFYwRnl9bpr8MjrM/edit?usp=sharing")
    sheet = file_ggsheet.worksheet(sheet_name)
    return sheet


def connect_clickhouse():
    try:
        cur = clickhouse_connect.get_client(host=config_info.host,
                                            port=config_info.port,
                                            username=config_info.user,
                                            password=config_info.password)
        cols = cur.query('select * from ub_rawdata.theo_doi_cong_no_ke_toan').column_names
        data = cur.query('select * from ub_rawdata.theo_doi_cong_no_ke_toan').result_set
        df = pd.DataFrame(data=data, columns=cols)
        return df
    except Exception as e:
        print(e)


def func_regex(date_row):
    if date_row is not None:
        format_date = re.findall("^\d{1,2}/\d{1,2}/\d{4}", date_row)
        if len(format_date) == 1:
            date_row = format_date[0]
            try:
                date_row = pd.to_datetime(date_row, format='%d/%m/%Y')
            except:
                date_row = pd.to_datetime(date_row, format='%m/%d/%Y', errors='coerce')
        else:
            date_row = None
    return date_row


def transform_data():
    df = connect_clickhouse()
    df = df.dropna(subset=['ma_misa'])
    int_col_list = ["so_tien_phai_thu", "so_tien_da_thu_duoc"]
    for col in int_col_list:
        df[col] = df[col].str.replace(r"[^-\d]", "", regex=True)
        df[col] = df[col].fillna(0)
        df[col].mask(df[col] == '-', 0, inplace=True)
        df[col].mask(df[col] == '', 0, inplace=True)
        df[col] = df[col].astype('int64')
    df['tong_thu'] = df[int_col_list].sum(axis=1)
    date_col_list = ["ngay_thang_xuat_chung_tu", "ngay_chung_tu", "ngay_nhan_chung_tu", "ngay_den_han_thanh_toan",
                     "ngay_tt", "check_payment_date"]
    for date_col in date_col_list:
        df[date_col] = df[date_col].apply(func_regex)
        df[date_col] = df[date_col].dt.strftime('%Y-%m-%d')
        df[date_col] = df[date_col].fillna('')
    return df
