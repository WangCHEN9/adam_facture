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
            ivivi_reader = IviviFactureReader(
                pdf_path=temp_pdf_path, 
                article_info=article_info, 
                output_folder_path=output_folder_path
            )
            with st.status("Running"):
                ivivi_reader.run()

            # Provide download links for XML and log files
            xml_file_path = output_folder_path / f"{temp_pdf_path.stem}.xml"
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
