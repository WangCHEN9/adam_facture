
from pathlib import Path
from typing import List, Union, Dict
import re
from datetime import datetime

import pdfplumber
from loguru import logger
import pandas as pd
import numpy as np
import pycountry

from data_model import Party, Item_unit, Declaration_unit, CN8, Envelope, DateTime, Function, Instat
from article_info import Article_Info


class SarlZhcFactureReader:

    party = Party(**{
        "partyId":"FR4980002435800015",
        "partyName":"ZHC",
    })
    party_tag = r'<Party partyType="TDP" partyRole="sender">'
    envelopeId = "L5BA"
    declarationTypeCode = 1     # 1 or 4 depends on company,
    if_xml: bool = True

    def __init__(self, pdf_path:Path, article_info: Article_Info, output_folder_path:Path) -> None:
        self.pdf_path = pdf_path
        self.article_info = article_info
        self.output_xml_path = output_folder_path / f"{self.pdf_path.stem}.xml"
        self._previous_page_metadata = {}
        self._pages_to_double_check = []
        self.df_item_all = None
        self.HEIGHT = 841.92004
        self.WIDTH = 595.32001

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

    def _get_address_dict(self, page) -> Dict:
        BOUNDING_BOX = (self.WIDTH * 0.42, self.HEIGHT * 0.08, self.WIDTH , self.HEIGHT * 0.30) 
        corp_1 = page.crop(BOUNDING_BOX)
        lines = corp_1.extract_text_lines()
        country = None
        tva_number = None
        for x in lines:
            if self.is_tva(x["text"]):
                tva_number = x["text"].strip().split(":")[-1]
                country = self.get_country_from_tva(tva_number)
        return {"dest_country": country, "N° TVA": tva_number}

    def is_tva(self, text) -> bool:
        match = "TVA intracom client"
        if match in str(text):
            return True
        else:
            return False

    def get_country_from_tva(self, tva:str) -> str:
        if tva.startswith("ES"):
            return "ES"
        if tva.startswith("ATU"):
            return "AT"
        if tva.startswith("EL"):
            return "GR"
        match = re.match(r'([^\d]+)', tva)
        if match:
            return match.group(1)
        else:
            raise ValueError(f"Invalid TVA format: {tva}")

    def _get_corp_1_info(self, page) -> Dict:

        BOUNDING_BOX_1 = (self.WIDTH * 3/8, 0, self.WIDTH , self.HEIGHT * 1.8/22.5) 
        corp_1 = page.crop(BOUNDING_BOX_1)
        lines = corp_1.extract_text_lines()
        res = lines[-1]["text"].split(" ")
        facture_number, date, client = res
        corp_1_dict = {
            "Facture N°": facture_number,
            "Date": date,
            "Client": client,
        }
        return corp_1_dict

    def get_instat(self) -> Instat:
        with pdfplumber.open(self.pdf_path) as pdf:
            dfs = []
            for page_index, page in enumerate(pdf.pages):
                text = page.extract_text_simple()
                if page.page_number == 1:
                    # just to double check if the pdf is matched with party name
                    if self.party.partyName not in text:
                        raise ValueError(f"{self.party.partyName} not found in {self.pdf_path}, page: {page.page_number}, probably wrong input pdf")
                try:
                    logger.info(f"extracting information from page number: {page.page_number}")
                    df_item = self._get_full_df_from_page(page=page)
                    print(df_item)
                    if not df_item.empty:
                        # Check if all string lengths in the TVA column are greater than 3, which is a valid TVA
                        is_good_tva = df_item['N° TVA'].str.len().gt(3).all()
                        # check if dest_country is FR(France) or not using starts with "FR" or "GB"
                        is_to_fr_or_gb = df_item['dest_country'].str.startswith("FR").any() or df_item['dest_country'].str.startswith("GB").any() or df_item['dest_country'].str.startswith("CH").any() or df_item['dest_country'].str.startswith("CHE").any() or df_item['dest_country'].str.startswith("PH").any()
                        logger.debug(f"Checked is_good_tva: {is_good_tva}")
                        if is_good_tva and not is_to_fr_or_gb:
                            dfs.append(df_item)
                        else:
                            logger.warning(f"Skipped because N° TVA is not good or dest_country is FR or GB: {page.page_number}")
                            self._pages_to_double_check.append(page.page_number)
                    else:
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

    def _get_full_df_from_page(self, page) -> pd.DataFrame:

        tables = page.find_tables()
        metadata_dict = self._get_corp_1_info(page)
        address_dict = self._get_address_dict(page)
        metadata_dict = {**metadata_dict, **address_dict}
        metadata_dict["page_number"] = page.page_number
        table = tables[0]
        raw_data = self._remove_empty_items(table.extract())    # remove things like ["", None, None, None, None]
                
        df_item = self._get_item_df(raw_data)
        df_item = df_item[df_item["Désignation"] != "FRAIS DE TRANSPORT"]
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

    def _get_item_df(self, raw_data: List) -> pd.DataFrame:
        item_to_match = ['Désignation', 'Quantité', 'P.U. HT', '% REM', 'Remise HT', 'Montant HT']
        array = np.array(raw_data)
        if array.shape == (2, len(item_to_match)):
            if raw_data[0] == item_to_match:
                result_dict = dict(zip(raw_data[0], raw_data[1]))
                data = self._prepare_data_for_item_df(result_dict=result_dict, raw_1_data=raw_data[1])
                df = pd.DataFrame(data)
                numeric_columns = ['Quantité', 'P.U. HT', '% REM', 'Remise HT', 'Montant HT']
                for col in numeric_columns:
                    df[col] = df[col].str.replace(',', '.')
                    df[col] = df[col].str.replace(' ', '')
                    df[col] = df[col].astype(float)
                df['remis_check'] = (df['Quantité'] * df['P.U. HT'] * df['% REM']/100).round(2) == df['Remise HT']
                # round Montant HT
                df['Montant HT'] = df['Montant HT'].round()
                if not df['remis_check'].all():
                    raise ValueError("One or more rows failed the Remis_check")
                return df
        return pd.DataFrame({i: [] for i in item_to_match}) # return empty df

    def _get_index_of_items(self, raw_1_data: List) -> List:
        codes = raw_1_data[2].split("\n")   # P.U. HT
        codes_indices = [index for index, value in enumerate(codes) if value is not None]

        return codes_indices

    def _prepare_data_for_item_df(self, result_dict, raw_1_data) -> Dict:
        codes_indices = self._get_index_of_items(raw_1_data)
        number_of_items = len(codes_indices)
        logger.debug(f"number_of_items: {number_of_items}")
        easy_mode = True
        logger.info(f"easy mode: {easy_mode}")
        output = {}
        for x, y in result_dict.items():
            y_raw_list = y.split("\n")
            output[x] = self.extend_or_short_list(y_raw_list, number_of_items)
        return output

    def extend_or_short_list(self, input_list, target_length, pad_value="0"):
        if not any(input_list):
            input_list = []
        if target_length > len(input_list):
            return input_list + [pad_value] * (target_length - len(input_list))
        else:
            return input_list[:target_length]

    def _get_items(self, df:pd.DataFrame) -> List[Item_unit]:
        output_list = []
        for index, data in df.iterrows():
            item_number = index + 1
            article_name=data["Désignation"]
            cn8 = self._get_cn8(article_name=article_name)
            if not cn8:
                logger.error(f"Error while creating item for \n{data}")
                logger.error(f"Skipped")
                self._pages_to_double_check.append(data["page_number"])
                continue
            invoicedAmount=round(data["Montant HT"] * (1 - data["% REM"]/100))
            item = Item_unit(
                itemNumber=item_number,
                CN8=cn8,
                MSConsDestCode=data["dest_country"],
                countryOfOriginCode="CN",
                netMass=round(self._get_weight(article_name=article_name) * data["Quantité"]),
                quantityInSU=data["Quantité"],
                invoicedAmount=invoicedAmount,
                statisticalProcedureCode=21,
                partnerId=data["N° TVA"],
                invoicedNumber=data["Facture N°"][-8:],  
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

        has_no_nulls = not df['Facture N°'].isnull().any()
        if not has_no_nulls:
            raise ValueError(f"Got df with null value in column Facture N°")    # make sure Facture N° is not empty
        declarations = []
        for _, group_data in df.groupby("Facture N°"):     # each facture is 1 declaration
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
