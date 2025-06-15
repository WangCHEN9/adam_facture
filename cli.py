from pathlib import Path
from article_info import Article_Info
from ivivi_facture_reader import IviviFactureReader
from jessy_facture_reader import JessyFactureReader
from dolvika_facture_reader import DolvikaFactureReader
from mod_facture_reader import ModFactureReader
from sarl_zhc_facture_reader import SarlZhcFactureReader
from zhc_facture_reader import ZhcFactureReader
from loguru import logger
import pandas as pd
import sys

func_mapping = {
    "IVIVI": IviviFactureReader,
    "Jessy & co": JessyFactureReader,
    "DOLVIKA": DolvikaFactureReader,
    "MODE CMD": ModFactureReader,
    "SARL ZHC": SarlZhcFactureReader,
    "Z.H.C": ZhcFactureReader,
}

def detect_company_from_folder(path: Path) -> str:
    folder_name = path.name.upper()
    for company_name in func_mapping.keys():
        if company_name in folder_name:
            return company_name
    raise ValueError(f"Company name not detected in folder: {folder_name}, supported companies: {', '.join(func_mapping.keys())}")


def main():
    working_dir = Path.cwd()
    input_path = working_dir
    output_path = working_dir / "output"
    excel_path = working_dir / "DONNEES DOUANE PYTHON.xlsx"

    # Create output folders if not exist
    output_path.mkdir(parents=True, exist_ok=True)
    log_dir = output_path / "log"
    log_dir.mkdir(parents=True, exist_ok=True)

    # Detect company
    company_name = detect_company_from_folder(working_dir)
    if not company_name:
        print(f"❌ Cannot detect company name from folder: {working_dir.name}")
        print(f"Supported companies: {', '.join(func_mapping.keys())}")
        input("Press Enter to exit...")
        sys.exit(1)

    # Load article info
    if not excel_path.exists():
        print(f"❌ Required Excel file not found: {excel_path}")
        input("Press Enter to exit...")
        sys.exit(1)

    article_info = Article_Info(source_excel=excel_path)
    reader_class = func_mapping[company_name]

    # Process PDFs
    pdf_files = list(input_path.glob("*.pdf"))
    if not pdf_files:
        print(f"⚠️ No PDF files found in {input_path}")
        input("Press Enter to exit...")
        return

    for pdf_file in pdf_files:
        log_file_path = log_dir / f"{pdf_file.stem}.log"
        if log_file_path.exists():
            log_file_path.unlink()
        logger.add(log_file_path, level="DEBUG")

        try:
            reader = reader_class(
                pdf_path=pdf_file,
                article_info=article_info,
                output_folder_path=output_path,
            )
            df = reader.run()
            if isinstance(df, pd.DataFrame):
                df.to_excel(output_path / f"{pdf_file.stem}.xlsx", index=False)
            print(f"✅ Processed: {pdf_file.name}")
        except Exception as e:
            logger.error(f"Failed to process {pdf_file.name}: {e}")
            print(f"❌ Error processing {pdf_file.name}: {e}")
        finally:
            logger.remove()

    input("✔️ Finished processing. Press Enter to exit...")

if __name__ == "__main__":
    main()
