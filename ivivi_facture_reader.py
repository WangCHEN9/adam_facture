
from pathlib import Path
from typing import List, Union, Dict
import re
from datetime import datetime

import pdfplumber
from loguru import logger
import pandas as pd
import numpy as np

from data_model import Party, Item_unit, Declaration_unit, CN8, Envelope, DateTime, Function, Instat
from article_info import Article_Info


class IviviFactureReader:

    party = Party(**{
        "partyId":"FR0853863996400013",
        "partyName":"IVIVI",
    })
    party_tag = r'<Party partyType="TDP" partyRole="sender">'
    envelopeId = "S4U3"
    declarationTypeCode = 1     # 1 or 4 depends on company,

    def __init__(self, pdf_path:Path, article_info: Article_Info, output_path:Path) -> None:
        self.pdf_path = pdf_path
        self.article_info = article_info
        self.output_path = output_path
        self._previous_page_metadata = {}

    def run(self):
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages[:1]:
                logger.info(f"extracting information from page number: {page.page_number}")
                metadata_dict, df_item = self._get_metadata_and_df_item_from_page(page)
                envelope = self._get_envelope(df=df_item, metadata_dict=metadata_dict)
                instat = Instat(Envelope=envelope)
                instat.export_to_xml(output_path=self.output_path, party_tag=self.party_tag)

    def _get_metadata_and_df_item_from_page(self, page):
        tables = page.find_tables()
        metadata_dict = None
        df_item = pd.DataFrame([])
        for table in tables:
            raw_data = self._remove_empty_items(table.extract())
            if metadata_dict is None:
                metadata_dict = self._get_metadata_dict(raw_data)
            if df_item.empty:
                df_item = self._get_item_df(raw_data)

        if metadata_dict and not df_item.empty:
            self._previous_page_metadata = metadata_dict
            return (metadata_dict, df_item)
        if not metadata_dict and not df_item.empty:
            logger.warning(f"not find metadata_dict, using previous page's : {self._previous_page_metadata}")
            return (self._previous_page_metadata, df_item)
        logger.error(f"Something wrong while extracting data from pdf page")
        raise

    def _remove_empty_items(self, input_list: List) -> List:
        output_list = []
        for i in input_list:
            if isinstance(i, list):
                if any(i):
                    output_list.append(i)
                else:
                    logger.warning(f"cleaned empty list {i}")
            else:
                if i:
                    output_list.append(i)
                else:
                    logger.warning(f"cleaned empty item {i}")
        return output_list

    def _get_metadata_dict(self, raw_data: List) -> Union[Dict, None]:
        item_to_match = ['Numéro', 'Date', 'Code client', 'Date échéance', 'Mode de règlement', 'N° de Tva intracom']
        array = np.array(raw_data)
        if array.shape == (2, 6):
            if raw_data[0] == item_to_match:
                result_dict = dict(zip(array[0], array[1]))
                return result_dict

    def _get_item_df(self, raw_data: List) -> pd.DataFrame:
        item_to_match = ['Code', 'Description', 'Qté', 'P.U. HT', 'Montant HT', 'TVA']
        array = np.array(raw_data)
        if array.shape == (2, 6):
            if raw_data[0] == item_to_match:
                result_dict = dict(zip(array[0], array[1]))
                data = {x: y.split("\n") for x, y in result_dict.items()}
                df = pd.DataFrame(data)
                numeric_columns = ['Qté', 'P.U. HT', 'Montant HT', 'TVA']
                for col in numeric_columns:
                    df[col] = df[col].str.replace(',', '.').astype(float)
                return df
        return pd.DataFrame({i: [] for i in item_to_match}) # return empty df

    def _get_chars_only(self, input_str:str) -> str:
        chars_only = re.match(r'^[A-Za-z]+', input_str)

        if chars_only:
            return chars_only.group()  # Extract the matched part
        else:
            logger.warning("No alphabetic characters at the start")

    def _get_items(self, df:pd.DataFrame, metadata_dict:Dict) -> List[Item_unit]:
        output_list = []
        for index, data in df.iterrows():
            item_number = index + 1
            logger.debug(f"preparing item for item number {item_number}, with data: {data.to_dict()}")
            article_name=data["Description"]
            item = Item_unit(
                itemNumber=item_number,
                CN8=self._get_cn8(article_name=article_name),
                MSConsDestCode="FR",
                countryOfOriginCode=self._get_chars_only(metadata_dict["N° de Tva intracom"]),
                netMass=self._get_weight(article_name=article_name) * data["Qté"],
                quantityInSU=data["Qté"],
                invoicedAmount=data["Montant HT"],
                partnerId=metadata_dict["N° de Tva intracom"],
                statisticalProcedureCode=11,
                NatureOfTransaction={
                    "natureOfTransactionACode":1,
                },
                modeOfTransportCode=3,
                regionCode="93",
            )
            output_list.append(item)
        return output_list

    def _get_declarations(self, df:pd.DataFrame, metadata_dict:Dict) -> List[Declaration_unit]:
        day, month, year = metadata_dict["Date"].split(r"/")

        declaration = Declaration_unit(
            declarationId = metadata_dict["Numéro"][-6:],
            referencePeriod = f"{year}-{month}",
            PSIId = self.party.partyId,
            Function = Function(functionCode="O"),
            declarationTypeCode = self.declarationTypeCode,
            flowCode = "D",
            currencyCode = "EUR",
            Item = self._get_items(df=df, metadata_dict=metadata_dict)
        )
        return [declaration]


    def _get_cn8(self, article_name:str) -> CN8:
        cn8 = CN8(
            CN8Code=str(self.article_info.get_article_info(article_name=article_name, target_col='CODE'))
        )
        return cn8
    
    def _get_weight(self, article_name:str) -> float:
        weight = self.article_info.get_article_info(article_name = article_name, target_col='POIDS/ARTICLE')
        return weight

    def _get_datetime(self) -> DateTime:
        current_datetime = datetime.now()

        # Format the current date and time to match the expected format
        formatted_date = current_datetime.strftime('%Y-%m-%d')  # YYYY-MM-DD
        formatted_time = current_datetime.strftime('%H:%M:%S')  # HH:MM:SS

        datetime_instance = DateTime(date=formatted_date, time=formatted_time)
        return datetime_instance

    def _get_envelope(self, df:pd.DataFrame, metadata_dict:Dict) -> Envelope:
        logger.info(f"preparing envelope for party: {self.party}")
        envelope = Envelope(
            envelopeId=self.envelopeId,
            DateTime=self._get_datetime(),
            Party=self.party,
            softwareUsed=None,
            Declaration=self._get_declarations(df=df, metadata_dict=metadata_dict)
        )
        return envelope


if __name__ == "__main__":
    pdf_path = Path(r"data/Facture 01-11 au 15-11.pdf")
    source_excel = Path(r"data/DONNEES DOUANE PYTHON.xlsx")
    output_path = r'output/xml_output.xml'
    a = Article_Info(source_excel, 'IVIVI')

    x = IviviFactureReader(pdf_path, a, output_path)
    x.run()