
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
    if_xml: bool = True

    def __init__(self, pdf_path:Path, article_info: Article_Info, output_folder_path:Path) -> None:
        self.pdf_path = pdf_path
        self.article_info = article_info
        self.output_xml_path = output_folder_path / f"{self.pdf_path.stem}.xml"
        self._previous_page_metadata = {}
        self._pages_to_double_check = []
        self.df_item_all = None

    @property
    def pages_to_double_check(self) -> List:
        return self._pages_to_double_check

    def run(self) -> Union[pd.DataFrame, None]:
        instat = self.get_instat()
        df_envelope = instat.Envelope.to_df()   # not always match with df_item_all, because df_item could have item doesn't match code in data\DONNEES DOUANE PYTHON.xlsx
        instat.export_to_xml(output_xml_path=self.output_xml_path, party_tag=self.party_tag)
        logger.warning(f"All page_numbers (skipped) to double check : {self._pages_to_double_check}")
        instat.validate_xml(xml_file=self.output_xml_path)
        if self.df_item_all is not None:
            return self.df_item_all

    def _get_remise(self, page, next_page=None) -> float:
        remise = self._get_remise_from_text(text=page.extract_text_simple())
        if remise is not None:
            return remise
        else:
            if next_page:
                remise = self._get_remise_from_text(text=next_page.extract_text_simple())
                logger.warning(f"can't get remise from page: {page.page_number}, looking at next page: {next_page.page_number}")
                if remise is not None:
                    return remise
        raise ValueError(f"Can't find remise for page: {page.page_number}")

    def _get_remise_from_text(self, text) -> Union[float, None]:
        match = re.search(r'Remise (\d+,\d+)%', text)

        # Check if a match was found
        if match:
            remise = match.group(1)  # Extract the matched text
            remise = float(remise.replace(",", ".")) / 100
            logger.info(f"Got Remise: {remise}")
            return remise

    def get_instat(self) -> Instat:
        with pdfplumber.open(self.pdf_path) as pdf:
            dfs = []
            for page_index, page in enumerate(pdf.pages):
                if page_index < len(pdf.pages) - 1:
                    next_page = pdf.pages[page_index+1]
                else:
                    next_page = None
                text = page.extract_text_simple()
                if page.page_number == 1:
                    # just to double check if the pdf is matched with party name
                    if self.party.partyName not in text:
                        raise ValueError(f"{self.party.partyName} not found in {self.pdf_path}, page: {page.page_number}, probably wrong input pdf")
                try:
                    logger.info(f"extracting information from page number: {page.page_number}")
                    df_item = self._get_full_df_from_page(page=page, next_page=next_page)
                    logger.debug(df_item)
                    if not df_item.empty:
                        # Check if all string lengths in the TVA column are greater than 3, which is a valid TVA
                        is_good_tva = df_item['N° de Tva intracom'].str.len().gt(3).all()
                        logger.debug(f"Checked is_good_tva: {is_good_tva}")
                        if is_good_tva:
                            dfs.append(df_item)
                        else:
                            logger.warning(f"Skipped because N° de Tva intracom is not good")
                            self._pages_to_double_check.append(page.page_number)
                except Exception as e:
                    logger.error(f"Error while processing page : {page.page_number}, skipped, error: {e}")
                    self._pages_to_double_check.append(page.page_number)
                    continue
            df = pd.concat(dfs, axis=0)
            self.df_item_all = df.copy()
            envelope = self._get_envelope(df=df)
            instat = Instat(Envelope=envelope)
            return instat

    def _check_is_second_page(self, page) -> str:
        text = page.extract_text_simple()
        if text.startswith(f"Facture N°"):
            first_line = text.split("\n")[0] 
            facture_number = first_line.split(" ")[-1]
            logger.debug(f"{page.page_number} is not a first page for facture number: {facture_number}")
            return facture_number
        else:
            logger.debug(f"{page.page_number} is the first page for the facture")

    def _get_full_df_from_page(self, page, next_page) -> pd.DataFrame:
        facture_number = self._check_is_second_page(page)
        remise = self._get_remise(page=page, next_page=next_page)
        if not facture_number:
            is_first_page = True
        else:
            is_first_page = False

        tables = page.find_tables()
        metadata_dict = None
        df_item = pd.DataFrame([])
        for table in tables:
            # loop over tables to get df_item and metadata_dict
            raw_data = self._remove_empty_items(table.extract())    # remove things like ["", None, None, None, None]
            if not metadata_dict:
                metadata_dict = self._get_metadata_dict(raw_data)
                if metadata_dict:
                    metadata_dict["page_number"] = page.page_number
                    metadata_dict["remise"] = remise
                    if not metadata_dict.get("N° de Tva intracom"):
                        logger.warning(f"missing N° de Tva intracom !")
                logger.debug(f"got metadata_dict: {metadata_dict}")
            if df_item.empty:
                df_item = self._get_item_df(raw_data)

        # use previous page's metadata if current page has previous page Numéro
        if is_first_page:
            if metadata_dict:
                self._previous_page_metadata = metadata_dict
            if df_item.empty:
                logger.error(f"can't find item table or is empty while this is the first page for the facture, please double check page number: {page.page_number}")
                self._pages_to_double_check.append(page.page_number)
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
                    logger.debug(f"cleaned at least half empty list {i}")
            else:
                if i:
                    output_list.append(i)
                else:
                    logger.warning(f"cleaned empty item {i}")
        return output_list

    def _get_metadata_dict(self, raw_data: List) -> Union[Dict, None]:
        item_to_match = ['Numéro', 'Date', 'Code client', 'Date échéance', 'Mode de règlement', 'N° de Tva intracom']
        array = np.array(raw_data)
        if array.shape == (2, len(item_to_match)):
            if raw_data[0] == item_to_match:
                result_dict = dict(zip(array[0].tolist(), array[1].tolist()))
                return result_dict

    def _get_item_df(self, raw_data: List) -> pd.DataFrame:
        item_to_match = ['Code', 'Description', 'Qté', 'P.U. HT', 'Montant HT', 'TVA']
        array = np.array(raw_data)
        if array.shape == (2, len(item_to_match)):
            if raw_data[0] == item_to_match:
                result_dict = dict(zip(raw_data[0], raw_data[1]))
                data = self._prepare_data_for_item_df(result_dict=result_dict, raw_1_data=raw_data[1])
                df = pd.DataFrame(data)
                numeric_columns = ['Qté', 'P.U. HT', 'Montant HT', 'TVA']
                for col in numeric_columns:
                    df[col] = df[col].str.replace(',', '.')
                    df[col] = df[col].str.replace(' ', '')
                    df[col] = df[col].astype(float)
                # round Montant HT
                df['Montant HT'] = df['Montant HT'].round()
                return df
        return pd.DataFrame({i: [] for i in item_to_match}) # return empty df

    def _get_index_of_items(self, raw_1_data: List) -> List:
        codes = raw_1_data[0].split("\n")
        codes_indices = [index for index, value in enumerate(codes) if value is not None]
        tvas = raw_1_data[-2].split("\n")   # col name = 'Montant HT'
        tva_indices = []
        for index, value in enumerate(tvas):
            if value is not None:
                if value != "0,00":
                    tva_indices.append(index)
        return (codes_indices, tva_indices)

    def _prepare_data_for_item_df(self, result_dict, raw_1_data) -> Dict:
        (codes_indices, tva_indices) = self._get_index_of_items(raw_1_data)
        output = {}
        for x, y in result_dict.items():
            y_raw_list = y.split("\n")
            if x == "Description":
                # clean Description for edge cases
                def is_valid_description(s):
                    result = not (s and s[0].isdigit())  # Returns False if the first character is a digit, if char , return True
                    wrong_starts_with_list = ["NOIR", "ARGENT"] # dirty fix for wrong item description
                    for wrong in wrong_starts_with_list:
                        if s.startswith(wrong):
                            result = False
                            logger.warning(f"Remove wrong description: {s}, because it startswith: {wrong}")
                    return result
                y_raw_list = [x for x in y_raw_list if is_valid_description(x)]
            if x == "Code":
                output[x] = [y_raw_list[i] for i in codes_indices]
            else:
                output[x] = [y_raw_list[i] for i in tva_indices]
        return output

    def _get_chars_only(self, input_str:str) -> str:
        if input_str:
            chars_only = re.match(r'^[A-Za-z]+', input_str)
            if chars_only:
                output = chars_only.group()  # Extract the matched part
                if output == "ESB":
                    output = "ES"
                return output
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
            if not cn8:
                logger.error(f"Error while creating item for \n{data}")
                logger.error(f"Skipped")
                self._pages_to_double_check.append(data["page_number"])
                continue
            invoicedAmount=round(data["Montant HT"] * (1 - data["remise"]))
            item = Item_unit(
                itemNumber=item_number,
                CN8=cn8,
                MSConsDestCode=self._get_chars_only(data["N° de Tva intracom"]),
                countryOfOriginCode="CN",
                netMass=round(self._get_weight(article_name=article_name) * data["Qté"]),
                quantityInSU=data["Qté"],
                invoicedAmount=invoicedAmount,
                statisticalProcedureCode=21,
                partnerId=data["N° de Tva intracom"],
                invoicedNumber=data["Numéro"][-8:],
                NatureOfTransaction={
                    "natureOfTransactionACode":1,
                    "natureOfTransactionBCode":1,
                },
                modeOfTransportCode=3,
                regionCode="93",
            )
            output_list.append(item)
                
        return output_list

    def _get_declarations(self, df:pd.DataFrame) -> List[Declaration_unit]:

        has_no_nulls = not df['Numéro'].isnull().any()
        if not has_no_nulls:
            raise ValueError(f"Got df with null value in column Numéro")    # make sure Numéro is not empty
        declarations = []
        for _, group_data in df.groupby("Numéro"):     # each facture is 1 declaration
            metadata_dict = group_data.iloc[0]
            _, month, year = metadata_dict["Date"].split(r"/")
            items = self._get_items(df=group_data)
            if items:
                # no declaration if items is empty
                declaration = Declaration_unit(
                    declarationId = f"{year}{month}",
                    referencePeriod = f"{year}-{month}",
                    PSIId = self.party.partyId,
                    Function = Function(functionCode="O"),
                    declarationTypeCode = self.declarationTypeCode,
                    flowCode = "D",
                    currencyCode = "EUR",
                    Item = items,
                )
                declarations.append(declaration)
        return declarations


    def _get_cn8(self, article_name:str) -> Union[CN8, None]:
        cn8_code = self.article_info.get_article_info(article_name=article_name, target_col='CODE')
        if cn8_code:
            cn8 = CN8(
                CN8Code=str(cn8_code)
            )
            return cn8
    
    def _get_weight(self, article_name:str) -> float:
        weight = self.article_info.get_article_info(article_name = article_name, target_col='POIDS/ARTICLE')
        if weight:
            return weight
        else:
            return 0.0

    def _get_datetime(self) -> DateTime:
        current_datetime = datetime.now()

        # Format the current date and time to match the expected format
        formatted_date = current_datetime.strftime('%Y-%m-%d')  # YYYY-MM-DD
        formatted_time = current_datetime.strftime('%H:%M:%S')  # HH:MM:SS

        datetime_instance = DateTime(date=formatted_date, time=formatted_time)
        return datetime_instance

    def _get_envelope(self, df:pd.DataFrame) -> Envelope:
        logger.debug(f"preparing envelope for party: {self.party}")
        envelope = Envelope(
            envelopeId=self.envelopeId,
            DateTime=self._get_datetime(),
            Party=self.party,
            softwareUsed=None,
            Declaration=self._get_declarations(df=df)
        )
        return envelope
