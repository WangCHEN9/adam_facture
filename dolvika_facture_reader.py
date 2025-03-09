
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


class DolvikaFactureReader:

    party = Party(**{
        "partyId":"FR3451291046400019",  
        "partyName":"DOLVIKA",
    })
    party_tag = r'<Party partyType="TDP" partyRole="sender">'
    envelopeId = "S4FW"
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
        self.metadata_all = {}

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

    def _get_number_date_info(self, page) -> Dict:

        BOUNDING_BOX_1 = (0, self.HEIGHT * 0.30, self.WIDTH , self.HEIGHT * 0.34) 
        res = self._cut_for_number_date(page, BOUNDING_BOX_1)
        if res:
            return res
        else:
            BOUNDING_BOX_2 = (0, self.HEIGHT * 0.25, self.WIDTH , self.HEIGHT * 0.30) 
            res = self._cut_for_number_date(page, BOUNDING_BOX_2)
            if res:
                return res
            else:
                raise ValueError(f"Can't get numero & date")
    
    def _cut_for_number_date(self, page, box):
        corp_1 = page.crop(box)
        lines = corp_1.extract_text_lines()
        pattern = r"(.+?)\s(\d{2}/\d{2}/\d{4})"
        pattern_2 = pattern + r".*\s+CEE\s+(.+)"
        for line in lines:
            match = re.search(pattern, line["text"])
            if match:
                facture_number = match.group(1).replace(" ", "")
                Date = match.group(2)
                corp_1_dict = {
                    "Numéro": facture_number,
                    "Date": Date,
                }
                match_2 = re.search(pattern_2, line["text"])
                if match_2:
                    corp_1_dict["CEE"] = match_2.group(3)
                else:
                    corp_1_dict["CEE"] = ""
                return corp_1_dict

    def is_country(self, name) -> bool:
        if str(name).upper() in ["BELGIQUE", "MAYOTTE", "SUISSE", "ALLEMAGNE", "ESPAGNE", "ITALIE", "PORTUGAL", "ROYAUME-UNI", "PAYS-BAS", "LUXEMBOURG", "AUTRICHE", "DANEMARK", "IRLANDE", "SUEDE", "FINLANDE", "GRECE", "POLOGNE", "REPUBLIQUE TCHEQUE", "SLOVAQUIE", "HONGRIE", "SLOVENIE", "ESTONIE", "LETTONIE", "LITUANIE", "CROATIE", "ROUMANIE", "BULGARIE", "CHYPRE", "MALTE", "ISLANDE", "NORVEGE", "LIECHTENSTEIN", "ANDORRE", "MONACO", "SAN MARINO", "VATICAN", "GIBRALTAR", "FAROE", "GUERNESEY", "JERSEY"]:
            return True
        names = [name.lower(), name.split(" ")[0].lower()]
        def _is_country(name):
            return any(country.name.lower() == name.lower() for country in pycountry.countries)
        return any(_is_country(x) for x in names)

    def _get_address_dict(self, page) -> Dict:
        BOUNDING_BOX = (self.WIDTH/2, self.HEIGHT * 0.10, self.WIDTH , self.HEIGHT * 0.28) 
        corp_1 = page.crop(BOUNDING_BOX)
        lines = corp_1.extract_text_lines()
        country = None
        tva_number = None
        for x in lines:
            if self.is_country(x["text"]):
                country = x["text"].split(" ")[0]
            if x["text"].startswith("N° TVA"):
                tva_number = x["text"].split(":")[-1].strip()
        return {"dest_country": country, "N° TVA": tva_number}

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
                        # check if dest_country is FR(France) or not using starts with "FR"
                        is_to_fr = df_item['dest_country'].str.startswith("FR").all()
                        logger.debug(f"Checked is_good_tva: {is_good_tva}")
                        if is_good_tva and not is_to_fr:
                            dfs.append(df_item)
                        else:
                            logger.warning(f"Skipped because N° TVA is not good or is to FR: {page.page_number}")
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

    def _set_or_get_metadata_dict_from_self(self, metadata_dict:Dict) -> Dict:
        if not self.metadata_all.get(metadata_dict["Numéro"]):
            # if first time see the Numéro, add it into self
            self.metadata_all[metadata_dict["Numéro"]] = metadata_dict
        return self.metadata_all[metadata_dict["Numéro"]]

    def _get_full_df_from_page(self, page) -> pd.DataFrame:

        metadata_dict = self._get_number_date_info(page)
        address_dict = self._get_address_dict(page)
        metadata_dict = {**metadata_dict, **address_dict}
        metadata_dict = self._set_or_get_metadata_dict_from_self(metadata_dict)
        metadata_dict["page_number"] = page.page_number
        logger.debug(f"Got metadata_dict: {metadata_dict}")
        BOUNDING_BOX = (0, self.HEIGHT * 0.38, self.WIDTH , self.HEIGHT) 
        corp_1 = page.crop(BOUNDING_BOX)
        lines = corp_1.extract_text_lines()
        pattern = r"(^\d*|BB|PSE|PRE50)\s+([\w\s']+)(\s+\d+,\d{2})+\s+(\d+\s?\d*,\d{2})(\s[1])?$"
        line_texts = []
        for x in lines:
            match = re.search(pattern, x["text"])
            if match:
                line_texts.append(x["text"].replace(match.group(2), match.group(2).replace(" ", "")).replace(match.group(4), match.group(4).replace(" ", "")))
        print(line_texts)
                
        df_item = self._get_item_df(line_texts)
        for k, v in metadata_dict.items():  # add metadata dict into df_items
            df_item[k] = v
        return df_item

    def _get_item_df(self, raw_data: List) -> pd.DataFrame:
        item_to_match = ["Code article", "Désignation", "Quantité", "P.U. HT", "Rem. %", "Montant HT", "TVA"]
        df_data = []
        for data in raw_data:
            splited_text: List = data.split(" ")
            if splited_text[-1] != "1": #last one is TVA (always=1)
                splited_text.append("1")
            if len(splited_text) == len(item_to_match) - 1:
                splited_text.insert(4, "0")
            elif len(splited_text) < len(item_to_match) - 1:
                raise ValueError(f"Missing column data during df_item preparison")
            df_data.append(splited_text.copy())
        df = pd.DataFrame.from_records(df_data, columns=item_to_match)
        numeric_columns = ["Quantité", "P.U. HT", "Montant HT"]
        for col in numeric_columns:
            df[col] = df[col].str.replace(',', '.')
            df[col] = df[col].str.replace(' ', '')
            df[col] = df[col].astype(float)
        return df

    def get_country_code(self, country_name):
        if country_name == "MAYOTTE":
            return "FR"
        elif country_name == "SUISSE":
            return "CH"
        try:
            country = pycountry.countries.lookup(country_name)
            return country.alpha_2  # Returns the ISO 3166-1 Alpha-2 code (e.g., 'US', 'FR')
        except LookupError:
            logger.warning(f"Can't get_country_code from {country_name}, return first 2 chars")
            if country_name:
                return country_name[:2]
            else:
                logger.warning(f"Got empty country_name: {country_name}")

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
            remise = float(data["Rem. %"].replace(",", ".")) / 100
            if remise > 0:
                logger.info(f"got remise: {remise} for page: {data['page_number']}")
            invoicedAmount=round(data["Montant HT"] * (1 - remise))
            item = Item_unit(
                itemNumber=item_number,
                CN8=cn8,
                MSConsDestCode=self.get_country_code(data["dest_country"]),
                countryOfOriginCode="FR",
                netMass=round(self._get_weight(article_name=article_name) * data["Quantité"]),
                quantityInSU=data["Quantité"],
                invoicedAmount=invoicedAmount,
                statisticalProcedureCode=21,
                partnerId=data["N° TVA"],
                invoicedNumber=data["Numéro"], 
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
                    declarationId = metadata_dict["Numéro"][-6:],
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
