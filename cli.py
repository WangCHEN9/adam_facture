import argparse
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
    "JESSY": JessyFactureReader,
    "DOLVIKA": DolvikaFactureReader,
    "MODE_CMD": ModFactureReader,
    "SARL_ZHC": SarlZhcFactureReader,
    "ZHC": ZhcFactureReader,
}


def detect_company_from_folder(path: Path) -> str:
    folder_name = path.name.upper()
    for company_name in func_mapping.keys():
        if company_name in folder_name:
            return company_name
    raise ValueError(f"Company name not detected in folder: {folder_name}, supported companies: {', '.join(func_mapping.keys())}")


def process_all_pdfs(company_folder: Path):
    company_name = detect_company_from_folder(company_folder)
    if not company_name:
        print(f"Cannot detect company name from folder: {company_folder.name}")
        print(f"Supported: {', '.join(func_mapping.keys())}")
        sys.exit(1)
    logger.success(f"Detected company: {company_name}")
    input_path = company_folder
    output_path = company_folder / "output"
    output_path.mkdir(parents=True, exist_ok=True)
    log_dir = output_path / "log"
    log_dir.mkdir(parents=True, exist_ok=True)

    article_info_excel = Path("data/DONNEES DOUANE PYTHON.xlsx")
    article_info = Article_Info(source_excel=article_info_excel)
    reader_class = func_mapping[company_name]

    for pdf_file in input_path.glob("*.pdf"):
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
            logger.remove()
        except Exception as e:
            logger.error(f"Failed to process {pdf_file.name}: {e}")
            logger.remove()


def main():
    parser = argparse.ArgumentParser(description="Batch process invoices by folder.")
    parser.add_argument("company_folder", type=Path, help="Folder containing input/output folders")

    args = parser.parse_args()
    logger.info(f"Processing company folder: {args.company_folder}")
    process_all_pdfs(args.company_folder)


if __name__ == "__main__":
    main()
