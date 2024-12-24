from typing import List, Optional, Dict, Union
from pydantic import BaseModel, Field
import xmlschema
from pathlib import Path
from loguru import logger
import xml.etree.ElementTree as ET
from lxml import etree


class DateTime(BaseModel):
    date: str = Field(..., pattern=r"^20\d{2}-\d{2}-\d{2}$", description="Date of file creation in format YYYY-MM-DD")
    time: Optional[str] = Field(None, pattern=r"^\d{2}:\d{2}:\d{2}$", description="Time of file creation in HH:MM:SS format")

class Party(BaseModel):
    partyId: str = Field(..., min_length=18, max_length=18, description="Declaration identifier (18 characters)")
    partyName: str = Field(..., max_length=14, description="Name of declarant")

class Function(BaseModel):
    functionCode: str = Field(..., pattern=r"^O$", description="Function code, always 'O' for original declaration")

class CN8(BaseModel):
    CN8Code: Optional[str] = Field(None, min_length=8, max_length=8, description="Combined nomenclature code (8 characters)")
    SUCode: Optional[str] = Field(None, description="Supplementary unit code")
    additionalGoodsCode: Optional[str] = Field(None, description="Additional goods code")

class Item_unit(BaseModel):
    itemNumber: int = Field(..., gt=0, le=999999, description="Positive line number")
    CN8: Optional[CN8] 
    MSConsDestCode: Optional[str] = Field(None, description="ISO country code of destination/provenance")
    countryOfOriginCode: Optional[str] = Field(None, description="ISO country code of origin")
    netMass: Optional[int] = Field(None, ge=0, le=9999999999, description="Net mass of goods")
    quantityInSU: Optional[int] = Field(None, ge=0, le=9999999999, description="Quantity in supplementary units")
    invoicedAmount: int = Field(..., gt=0, le=99999999999, description="Invoice amount in euros")
    partnerId: Optional[str] = Field(None, description="Partner's VAT number (ISO country + number)")
    statisticalProcedureCode: int = Field(..., description="Statistical procedure code")
    NatureOfTransaction: Optional[Dict]
    modeOfTransportCode: Optional[int] = Field(None, ge=1, le=9, description="Mode of transport code")
    regionCode: Optional[str] = Field(None, pattern=r"^(\d{2}|2A|2B)$", description="Region code")

class Declaration_unit(BaseModel):
    declarationId: str = Field(..., min_length=6, max_length=6, description="Declaration identifier (6 characters numeric)")
    referencePeriod: str = Field(..., pattern=r"^20\d{2}-\d{2}$", description="Reference period in format YYYY-MM")
    PSIId: str = Field(..., min_length=18, max_length=18, description="PSI Identifier (18 characters)")
    Function: Function
    declarationTypeCode: int = Field(..., ge=1, le=5, description="Declaration type code (1, 4, or 5)")
    flowCode: str = Field(..., pattern=r"^(A|D)$", description="Flow code: A for Introduction, D for dispatch")
    currencyCode: str = Field(..., pattern=r"^EUR$", description="Currency code, always EUR")
    Item: List[Item_unit]

class Envelope(BaseModel):
    envelopeId: str = Field(..., max_length=4, description="Envelope identifier (4 alphanumeric characters)")
    DateTime: DateTime
    Party: Party
    softwareUsed: Optional[str] = Field(None, max_length=14, description="Software used for XML generation")
    Declaration: List[Declaration_unit]

class Instat(BaseModel):
    Envelope: Envelope

    def export_to_xml(self, output_xml_path: Path, party_tag: str, root_tag: str = "INSTAT"):
        """
        Export the Pydantic model instance to an XML file.
        """
        def dict_to_xml(tag, d):
            """
            Turn a simple dict of key/value pairs into XML.
            """
            elem = ET.Element(tag)
            for key, val in d.items():
                if key == "@attributes" and isinstance(val, dict):
                    for attr_key, attr_val in val.items():
                        elem.set(str(attr_key), str(attr_val))
                        logger.info(f"set tag attr: {attr_key} to {attr_val}")
                elif isinstance(val, dict):
                    child = ET.SubElement(elem, key)
                    child.extend(list(dict_to_xml(key, val)))
                elif isinstance(val, list):
                    for sub_val in val:
                        child = ET.SubElement(elem, key)
                        if isinstance(sub_val, dict):
                            child.extend(list(dict_to_xml(key, sub_val)))
                        else:
                            child.text = str(sub_val)
                else:
                    child = ET.SubElement(elem, key)
                    child.text = str(val)
            if tag == "Party":
                print(elem.tag)
            return elem

        # Convert the Pydantic model to a dictionary
        data_dict = self.model_dump()

        # Create the root element of XML
        root = dict_to_xml(root_tag, data_dict)

        # Create the XML tree and write to file
        tree = ET.ElementTree(root)
        logger.info(f"writing xml file to {output_xml_path}")
        tree.write(output_xml_path, encoding="utf-8", xml_declaration=True)

        self.replace_string_in_file(file_path=output_xml_path, old_string="<Party>", new_string=party_tag)

    def replace_string_in_file(self, file_path: Path, old_string: str, new_string: str):
        # Read the file's content
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()

        # Replace the old string with the new string
        modified_content = content.replace(old_string, new_string)

        # Write the modified content back to the file
        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(modified_content)

    def validate_xml(self, xml_file:Path, xsd_file=Path(r"xsd_valide.xsd")):
        # Parse the XSD schema file
        with open(xsd_file, 'r') as schema_file:
            schema_root = etree.parse(schema_file)
            xmlschema = etree.XMLSchema(schema_root)

        # Parse the XML file
        with open(xml_file, 'r') as xml_file:
            xml_root = etree.parse(xml_file)

        # Validate the XML file against the XSD schema
        is_valid = xmlschema.validate(xml_root)

        if is_valid:
            logger.success("XML is valid according to the XSD schema.")
        else:
            logger.error("XML is not valid. Errors:")
            for error in xmlschema.error_log:
                logger.error(error)
