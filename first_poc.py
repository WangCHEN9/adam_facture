import re
from pathlib import Path
from typing import Dict, List

import pdfplumber
import pandas as pd
import numpy as np
from loguru import logger


HEIGHT = 841.92004
WIDTH = 595.32001
BOUNDING_BOX_1 = (0, 0, WIDTH, HEIGHT * 1.8/22.5) 
BOUNDING_BOX_2 = (0, 0, WIDTH, HEIGHT/3) 


MY_COMPANY_INFO = {
    "tel": "0149371029",
    "Fax": "0148346795",
    "Capital": "8 000,00 Euros",
    "R.C.S.": "RCS BOBIGNY 800024358",
    "SIRET": "80002435800015",
    "N° TVA intracom": "FR49800024358",
    "company_name": "SARL ZHC",
    "address": "3 RUE DE LA HAIE COQ 93300 AUBERVILLIERS",
}


# total 22.5cm
# fature / date / client = 1.8cm

def get_corp_1_info(page) -> Dict:
    corp_1 = page.crop(BOUNDING_BOX_1)
    lines = corp_1.extract_text_lines()
    facture_number, date, client = lines[-1]["text"].split(" ")
    corp_1_dict = {
        "facture_number": facture_number,
        "date": date,
        "client": client,
    }
    return corp_1_dict

def get_main_table_df(page) -> pd.DataFrame:
    corp_2 = page.outside_bbox(BOUNDING_BOX_2)
    tables = corp_2.find_tables({"min_words_vertical": 2})
    table = tables[0]
    raw_data = table.extract()
    raw_1 = [x.split("\n") for x in raw_data[1] if any(x.split("\n"))]
    number_of_items = min([len(x) for x in raw_1]) # RIB will have more rows, but all columns has it's information, so min is number of items, and always in top

    raw_1_cleaned = []
    for i in raw_data[1]:
        row_list = i.split("\n")
        if len(row_list) >= number_of_items:
            row_list = row_list[:number_of_items]
        elif len(row_list) == 1 and not any(row_list):
            row_list = ["" for i in range(number_of_items)]
        else:
            logger.error(f"something wrong when reading main table")
            raise
        raw_1_cleaned.append(row_list)

    raw_1_cleaned = np.array(raw_1_cleaned)
    if raw_1_cleaned.shape[0] == 6:
        raw_t = raw_1_cleaned.T
    else:
        logger.warning(f"got empty main table for page {page.page_number}")
        raw_t = []
    df = pd.DataFrame(data=raw_t, columns=["Désignation", "Quantité", "P.U. HT", "% REM", "Remise HT", "Montant HT"])
    return df


def extract_invoice_data(pdf_path: Path) -> List[Dict]:
    invoices = []
    current_facture_id = None
    current_df = None
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            logger.info(f"Reading page number: {page.page_number}")
            corp_1 = get_corp_1_info(page=page)
            print(corp_1)
            if corp_1["facture_number"] == current_facture_id:
                logger.warning(f"got same facture_number as previous page")
            df = get_main_table_df(page=page)
            print(df)
            current_df = df
            current_facture_id = corp_1["facture_number"]
    return invoices


if __name__ == "__main__":
    pdf_path = Path(r"./data/Facture - FEV 2024.pdf")
    invoices = extract_invoice_data(pdf_path)