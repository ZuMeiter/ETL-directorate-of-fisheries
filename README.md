# ETL – Directorate of Fisheries (Monthly Landings)

Small ETL project that converts raw landings statistics from the Norwegian Directorate of Fisheries into
clean monthly aggregates for:

- **Norwegian landings**
- **Foreign landings (utenland)**

The pipeline reads two CSV files with detailed landings per species and outputs monthly totals
both in CSV and Parquet formats.

---

## 1. Project structure

ETL-directorate-of-fisheries/
├── .venv/ # your local virtual machine (in .gitignore - not available on GitHub)
│ └── Lib/site-packages/... # installed libraries
│
├── data/
│ ├── raw/ # SIROVY data (input)
│ │ ├── landings_og_art.csv
│ │ └── landings_og_art_utenland.csv
│ └── processed/ # READY aggregate tables (output)
│ ├── .gitkeep # empty file so that the folder is added to the repo
│ ├── landings_no_monthly.csv
│ ├── landings_foreign_monthly.csv
│ ├── landings_foreign_monthly.parquet
│ └── monthly_2024.parquet
│
├──etl/
│ ├── __pycache__/ # .pyc - you don’t have to read it, the stink can be ignored
│ ├── common.py # common functions (read CSV, normalization, aggregation)
│ ├── transform_no.py # transformation for Norway
│ ├── transform_utenland.py # transformation for foreign data
│ └── run.py # head "runner": launches offending pipelines
│
├── .gitignore
├── requirements.txt # list of libraries (pandas, pyarrow, requests…)
├── monthly.py # old script that works using a different hook, it’s not needed now
└── test_1.py # very first sample
  * Note: the virtual environment (.venv/) and generated files (*.parquet, processed CSVs)
are deliberately excluded from Git via .gitignore.


  2. Requirements
-Python 3.11+
-Optionally: Git (to clone the repository)
-Python dependencies are listed in requirements.txt and installed with:
  pandas
  pyarrow


  3. Getting the project
-Option A - clone via Git (recommended)
git clone https://github.com/NAME-USER/ETL-directorate-of-fisheries.git
cd ETL-directorate-of-fisheries

-Option B - download ZIP
Open the repository on GitHub.
Click Code - Download ZIP.

-Unzip and open the folder in VS Code/terminal.


  4. Virtual environment & dependencies
From the project root:
  4.1 Create virtual environment
python -m venv .venv
  4.2 Activate it
Windows (PowerShell): .\.venv\Scripts\Activate.ps1

-You should see (.venv) at the beginning of the terminal prompt.

  4.3 Install dependencies
pip install -r requirements.txt


  5. Raw input data
Before running the ETL, make sure the raw CSV files are placed in: data/raw/
                                                                    landings_og_art.csv
                                                                    landings_og_art_utenland.csv
Expected:
landings_og_art.csv – Norwegian landings
landings_og_art_utenland.csv – foreign landings
The script is robust to different encodings and separators.
smtart_read_csv() in etl/common.py will try combinations like: utf-8-sig, utf-16
separators ; and tab \t


  6. Running the ETL
Activate the virtual environment first so you have (.venv) in your prompt, then:
-python -m etl.run

Prints a short summary in the console, e.g.:
[NO]  -> C:\...\data\raw\landings_og_art.csv
[FOREIGN] -> C:\...\data\raw\landings_og_art_utenland.csv
Finish: data/processed/*.csv|*.parquet


  7. Output format
Typical columns:
-year – year of landing
-month – month number (1–12)
-rundvekt_tonn – total landed quantity in tonnes
-rows – number of original rows from the raw file that were aggregated into this group


  8. How to re-run from scratch
If you want to regenerate all outputs:
-Delete files inside data/processed/ except .gitkeep
(or just keep the folder, delete only *.csv / *.parquet).
Make sure updated raw CSVs are in data/raw/.
-Run: python -m etl.run


9. Troubleshooting
UnicodeDecodeError while reading CSV
Handled automatically by smart_read_csv. If it still fails, check that the raw file is a valid CSV and not e.g. Excel.

ValueError: could not convert string to float
Means that a numeric column contains unexpected text. Check the relevant column in the raw CSV.