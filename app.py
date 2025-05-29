import streamlit as st
from pathlib import Path
from article_info import Article_Info
from ivivi_facture_reader import IviviFactureReader
from jessy_facture_reader import JessyFactureReader
from dolvika_facture_reader import DolvikaFactureReader
from mod_facture_reader import ModFactureReader
from sarl_zhc_facture_reader import SarlZhcFactureReader
from zhc_facture_reader import ZhcFactureReader
from loguru import logger
import shutil
import pandas as pd
from io import BytesIO

# Streamlit App
def main(output_folder_path, log_folder_path):

    @st.cache_resource
    def configure_logging(log_file_path):
        # Clean up existing log file
        if log_file_path.exists():
            log_file_path.unlink()
        logger.add(log_file_path, level="INFO")

    st.title("PDF to XML Processor")
    st.write("Upload a PDF file to process and generate XML & logs.")

    # Sidebar for company selection
    st.sidebar.title("Company Selection")

    func_mapping = {
        "IVIVI": IviviFactureReader,
        "Jessy & co": JessyFactureReader,
        "DOLVIKA": DolvikaFactureReader,
        "MODE CMD": ModFactureReader,
        "SARL ZHC": SarlZhcFactureReader,
        "Z.H.C": ZhcFactureReader,
    }
    company_name = st.sidebar.selectbox("Select Company", list(func_mapping.keys()))
    process_func = func_mapping[company_name]

    article_info_excel = Path("data/DONNEES DOUANE PYTHON.xlsx")

    # Load Article_Info
    article_info = Article_Info(source_excel=article_info_excel)

    # File uploader
    uploaded_file = st.file_uploader("Choose a PDF file", type="pdf")
    if "process_done" not in st.session_state:
        st.session_state["process_done"] = False

    if uploaded_file is not None:
        # Save the uploaded file to a temporary location
        temp_pdf_path = output_folder_path / uploaded_file.name
        with temp_pdf_path.open("wb") as f:
            shutil.copyfileobj(uploaded_file, f)

        # Process the PDF
        st.write("Processing the PDF... It will take few minutes")
        try:
            xml_file_path = output_folder_path / f"{temp_pdf_path.stem}.xml"
            excel_file_path = output_folder_path / f"{temp_pdf_path.stem}.xlsx"
            log_file_path = log_folder_path / f"{temp_pdf_path.stem}.log"

            reader = process_func(
                pdf_path=temp_pdf_path, 
                article_info=article_info, 
                output_folder_path=output_folder_path
            )
            if not reader.if_xml:
                st.warning(f"No XML file will be generated for {reader.party.partyName}, only excel file will be generated")
            else:
                st.success(f"Both XML and excel file will be generated for {reader.party.partyName}")
            if not st.session_state["process_done"]:
                logger.debug(f"start to process, as session_state process_done: {st.session_state['process_done']}")
                with st.status("Running"):
                    configure_logging(log_file_path)
                    df = reader.run()
                    if df is not None:
                        df.to_excel(excel_file_path, index=False)
                    st.session_state["process_done"] = True

            if excel_file_path.exists():
                with open(excel_file_path, "rb") as f:
                    st.download_button(
                        label="Download PDF Scan result as excel",
                        data=f,
                        file_name=excel_file_path.name,
                        mime="application/vnd.ms-excel",
                    )
                st.success(f"PDF Scan result as excel, ready to be downloaded")

            # Provide download links for XML and log files
            if xml_file_path.exists():
                with open(xml_file_path, "rb") as f:
                    st.download_button(
                        label="Download XML",
                        data=f,
                        file_name=xml_file_path.name,
                        mime="application/xml"
                    )
                st.success(f"Successful created xml file, and validated with xsd validation")
            else:
                if reader.if_xml:
                    st.error(f"Error while creating xml file")

            if log_file_path.exists():
                with open(log_file_path, "rb") as f:
                    st.download_button(
                        label="Download Log",
                        data=f,
                        file_name=f"{xml_file_path.stem}.log",
                        mime="text/plain"
                    )
                st.warning(f"Please make sure, you download the log, and check if there are any warnings & errors")
            else:
                st.error(f"Error while getting log file")

            st.success("Processing complete, Please refresh page if you want process another PDF file!")

        except Exception as e:
            st.error(f"An error occurred: {e}")
            logger.exception("An error occurred during processing.")

if __name__ == "__main__":
        
    # Set up paths
    output_folder_path = Path("output")
    output_folder_path.mkdir(parents=True, exist_ok=True)

    log_folder_path = output_folder_path / "log"
    log_folder_path.mkdir(parents=True, exist_ok=True)

    main(output_folder_path, log_folder_path)

