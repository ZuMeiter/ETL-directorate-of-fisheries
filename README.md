# CSV to Chart with pandas

This repository contains a small script `plot_csv.py` that reads a CSV file using pandas and creates a chart using matplotlib.

Files added:
- `plot_csv.py` - function `load_and_plot` and CLI to plot CSV columns.
- `data/sample.csv` - sample dataset (date,value) used for testing.
- `requirements.txt` - dependencies.

Quick start (PowerShell on Windows):

```powershell
# Create a virtualenv (optional but recommended)
python -m venv .venv; .\.venv\Scripts\Activate.ps1

# Install dependencies
pip install -r requirements.txt

# Run the example and save an image
python plot_csv.py data/sample.csv --x date --y value --type line --output out.png

# Open the resulting image (PowerShell)
ii out.png
```

Library usage (from Python):

```python
from plot_csv import load_and_plot

fig = load_and_plot('data/sample.csv', x_col='date', y_cols=['value'], output='out.png')
```

Notes:
- If `--y` is omitted, the script will attempt to plot all numeric columns.
- Supported chart types: line, bar, scatter.
