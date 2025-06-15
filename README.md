
## Python environment

```python
pip install -r requirements.txt
```


## What this repo do

1. Read pdf facture (each company have its own reader)
2. Combine with excel input data
3. Output required xml file


## Important note

1. The parser of pdf is per company, each company has different pdf format, so the parse method must be updated, and handle different edge cases

2. Failed parse page will be print out in the end


---

## How to create the `.exe`

```bash
pyinstaller --onefile cli.py --name=facture_parser
```

