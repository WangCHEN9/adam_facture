
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

    def __init__(self, pdf_path:Path, article_info: Article_Info, output_folder_path:Path) -> None:
        self.pdf_path = pdf_path
        self.article_info = article_info
        self.output_xml_path = output_folder_path / f"{self.party.partyName}_{self.pdf_path.stem}.xml"
        self._previous_page_metadata = {}

    def run(self):
        with pdfplumber.open(pdf_path) as pdf:
            dfs = []
            for page in pdf.pages:
                try:
                    logger.info(f"extracting information from page number: {page.page_number}")
                    df_item = self._get_full_df_from_page(page=page)
                    print(df_item)
                    if not df_item.empty:
                        dfs.append(df_item)
                except ValueError as e:
                    logger.info(f"Error while processing page : {page.page_number}")
                    continue
            df = pd.concat(dfs, axis=0)
            envelope = self._get_envelope(df=df)
            instat = Instat(Envelope=envelope)
            instat.export_to_xml(output_xml_path=self.output_xml_path, party_tag=self.party_tag)
            instat.validate_xml(xml_file=self.output_xml_path)

    def _check_is_second_page(self, page) -> str:
        text = page.extract_text_simple()
        if text.startswith(f"Facture N°"):
            first_line = text.split("\n")[0] 
            facture_number = first_line.split(" ")[-1]
            logger.debug(f"{page.page_number} is not a first page for facture number: {facture_number}")
            return facture_number
        else:
            logger.debug(f"{page.page_number} is the first page for the facture")

    def _get_full_df_from_page(self, page) -> pd.DataFrame:
        facture_number = self._check_is_second_page(page)
        if not facture_number:
            is_first_page = True
        else:
            is_first_page = False

        tables = page.find_tables()
        metadata_dict = None
        df_item = pd.DataFrame([])
        for table in tables:
            raw_data = self._remove_empty_items(table.extract())
            if not metadata_dict:
                metadata_dict = self._get_metadata_dict(raw_data)
                if metadata_dict:
                    if not metadata_dict.get("N° de Tva intracom"):
                        logger.error(f"missing N° de Tva intracom !")
                logger.debug(f"got metadata_dict: {metadata_dict}")
            if df_item.empty:
                df_item = self._get_item_df(raw_data)

        if is_first_page:
            if metadata_dict:
                self._previous_page_metadata = metadata_dict
            if df_item.empty:
                logger.error(f"can't find item table or is empty while this is the first page for the facture, please double check page number: {page.page_number}")
        else:
            if self._previous_page_metadata["Numéro"] == facture_number:
                logger.success(f"not find metadata_dict, using previous page's : {self._previous_page_metadata}")
                metadata_dict = self._previous_page_metadata
            else:
                raise ValueError(f"can't find metadata dict")

        for k, v in metadata_dict.items():  # add metadata dict into df_items
            df_item[k] = v
        return df_item

    def _remove_empty_items(self, input_list: List) -> List:
        output_list = []
        for i in input_list:
            if isinstance(i, list):
                list_without_none = [x for x in i if x]
                if (len(i) - len(list_without_none)) / len(i) < 0.5 :
                    output_list.append(i)
                else:
                    logger.info(f"cleaned at least half empty list {i}")
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
                number_of_items = self._get_number_of_items(raw_data[1])
                result_dict = dict(zip(raw_data[0], raw_data[1]))
                data = {x: y.split("\n")[:number_of_items] for x, y in result_dict.items()}
                df = pd.DataFrame(data)
                numeric_columns = ['Qté', 'P.U. HT', 'Montant HT', 'TVA']
                for col in numeric_columns:
                    df[col] = df[col].str.replace(',', '.')
                    df[col] = df[col].str.replace(' ', '')
                    df[col] = df[col].astype(float)
                return df
        return pd.DataFrame({i: [] for i in item_to_match}) # return empty df

    def _get_number_of_items(self, raw_1_data: List) -> List:
        codes = raw_1_data[0].split("\n")
        return len(codes)

    def _get_chars_only(self, input_str:str) -> str:
        if input_str:
            chars_only = re.match(r'^[A-Za-z]+', input_str)
            if chars_only:
                return chars_only.group()  # Extract the matched part
            else:
                logger.error(f"No alphabetic characters at the start for {input_str}")
        else:
            return None

    def _get_items(self, df:pd.DataFrame) -> List[Item_unit]:
        output_list = []
        for index, data in df.iterrows():
            item_number = index + 1
            article_name=data["Description"]
            cn8 = self._get_cn8(article_name=article_name)
            if cn8:
                item = Item_unit(
                    itemNumber=item_number,
                    CN8=cn8,
                    MSConsDestCode="FR",
                    countryOfOriginCode=self._get_chars_only(data["N° de Tva intracom"]),
                    netMass=int(self._get_weight(article_name=article_name) * data["Qté"]),
                    quantityInSU=data["Qté"],
                    invoicedAmount=int(data["Montant HT"]),
                    partnerId=data["N° de Tva intracom"],
                    statisticalProcedureCode=11,
                    NatureOfTransaction={
                        "natureOfTransactionACode":1,
                    },
                    modeOfTransportCode=3,
                    regionCode="93",
                )
                output_list.append(item)
        return output_list

    def _get_declarations(self, df:pd.DataFrame) -> List[Declaration_unit]:
        metadata_dict = df.iloc[0]
        day, month, year = metadata_dict["Date"].split(r"/")
        
        declaration = Declaration_unit(
            declarationId = metadata_dict["Numéro"][-6:],
            referencePeriod = f"{year}-{month}",
            PSIId = self.party.partyId,
            Function = Function(functionCode="O"),
            declarationTypeCode = self.declarationTypeCode,
            flowCode = "D",
            currencyCode = "EUR",
            Item = self._get_items(df=df)
        )
        return [declaration]


    def _get_cn8(self, article_name:str) -> CN8:
        cn8_code = self.article_info.get_article_info(article_name=article_name, target_col='CODE')
        if cn8_code:
            cn8 = CN8(
                CN8Code=str(cn8_code)
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

    def _get_envelope(self, df:pd.DataFrame) -> Envelope:
        logger.info(f"preparing envelope for party: {self.party}")
        envelope = Envelope(
            envelopeId=self.envelopeId,
            DateTime=self._get_datetime(),
            Party=self.party,
            softwareUsed=None,
            Declaration=self._get_declarations(df=df)
        )
        return envelope


if __name__ == "__main__":
    pdf_path = Path(r"data/Facture 01-11 au 15-11.pdf")
    article_info_excel = Path(r"data/DONNEES DOUANE PYTHON.xlsx")
    output_folder_path = Path(r'output')
    article_info = Article_Info(source_excel=article_info_excel)

    x = IviviFactureReader(pdf_path=pdf_path, article_info=article_info, output_folder_path=output_folder_path)
    x.run()