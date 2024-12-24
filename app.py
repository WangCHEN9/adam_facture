import streamlit as st
from pathlib import Path
from article_info import Article_Info
from ivivi_facture_reader import IviviFactureReader
from loguru import logger
import shutil

# Streamlit App
def main():
    st.title("PDF to XML Processor")
    st.write("Upload a PDF file to process and generate XML and logs.")

    # Sidebar for company selection
    st.sidebar.title("Company Selection")
    company_name = st.sidebar.selectbox("Select Company", ["IVIVI"])

    func_mapping = {
        "IVIVI": IviviFactureReader,
    }
    process_func = func_mapping[company_name]

    # Set up paths
    output_folder_path = Path("output")
    output_folder_path.mkdir(parents=True, exist_ok=True)
    
    log_folder_path = output_folder_path / "log"
    log_folder_path.mkdir(parents=True, exist_ok=True)

    article_info_excel = Path("data/DONNEES DOUANE PYTHON.xlsx")

    # Load Article_Info
    article_info = Article_Info(source_excel=article_info_excel)

    # File uploader
    uploaded_file = st.file_uploader("Choose a PDF file", type="pdf")

    @st.cache_data
    def get_result(_process_obj):
        return _process_obj.get_instat()

    if uploaded_file is not None:
        # Save the uploaded file to a temporary location
        temp_pdf_path = output_folder_path / uploaded_file.name
        with temp_pdf_path.open("wb") as f:
            shutil.copyfileobj(uploaded_file, f)

        log_file_path = log_folder_path / f"{temp_pdf_path.stem}.log"

        # Clean up existing log file
        if log_file_path.exists():
            log_file_path.unlink()

        # Configure logger
        logger.add(log_file_path, level="INFO")

        # Process the PDF
        st.write("Processing the PDF... It will take few minutes")
        try:
            xml_file_path = output_folder_path / f"{temp_pdf_path.stem}.xml"
            reader = process_func(
                pdf_path=temp_pdf_path, 
                article_info=article_info, 
                output_folder_path=output_folder_path
            )
            with st.status("Running"):
                instat = get_result(reader)
            instat.export_to_xml(output_xml_path=xml_file_path, party_tag=reader.party_tag)
            logger.warning(f"All page_numbers (skipped) to double check : {reader._pages_to_double_check}")
            instat.validate_xml(xml_file=xml_file_path)

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
                st.error(f"Error while creating xml file")

            if log_file_path.exists():
                with open(log_file_path, "r") as f:
                    log_lines = f.readlines()
                    last_5_lines = log_lines[-5:] if len(log_lines) >= 5 else log_lines
                    st.text("\n".join(last_5_lines))
                with open(log_file_path, "rb") as f:
                    st.download_button(
                        label="Download Log",
                        data=f,
                        file_name=log_file_path.name,
                        mime="text/plain"
                    )
            else:
                st.error(f"Error while getting log file")

            st.success("Processing complete.")

        except Exception as e:
            st.error(f"An error occurred: {e}")
            logger.exception("An error occurred during processing.")

if __name__ == "__main__":
    main()
