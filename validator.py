from lxml import etree

def validate_xml(xml_file, xsd_file):
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
        print("XML is valid according to the XSD schema.")
    else:
        print("XML is not valid. Errors:")
        for error in xmlschema.error_log:
            print(error)


if __name__ == "__main__":
    xml_file = "output/xml_output.xml"
    xsd_file = "xsd_valide.xsd"
    validate_xml(xml_file, xsd_file)