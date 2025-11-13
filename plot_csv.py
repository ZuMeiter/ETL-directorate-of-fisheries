import argparse
from typing import List, Optional

import pandas as pd
import matplotlib.pyplot as plt


def load_and_plot(
    csv_path: str,
    x_col: Optional[str] = None,
    y_cols: Optional[List[str]] = None,
    chart_type: str = "line",
    title: Optional[str] = None,
    figsize: tuple = (10, 6),
    output: Optional[str] = None,
    show: bool = False,
):
    """Load CSV with pandas and create a matplotlib chart.

    Args:
        csv_path: Path to input CSV file.
        x_col: Column name to use for the x-axis. If None, use the DataFrame index.
        y_cols: List of column names to plot. If None, plot all numeric columns except x_col.
        chart_type: 'line', 'bar', or 'scatter'.
        title: Optional chart title.
        figsize: Figure size tuple.
        output: If provided, save the chart to this path (e.g., 'out.png').
        show: If True, call plt.show() (useful in interactive sessions).

    Returns:
        matplotlib.figure.Figure: The created figure object.
    """
    df = pd.read_csv(csv_path)

    if x_col is not None and x_col in df.columns:
        x = df[x_col]
    else:
        x = None

    # If y_cols not provided, select numeric columns except x_col
    if y_cols is None:
        y_cols = [c for c in df.select_dtypes(include="number").columns if c != x_col]

    if not y_cols:
        raise ValueError("No columns to plot. Specify y_cols or provide numeric columns in the CSV.")

    fig, ax = plt.subplots(figsize=figsize)

    for y in y_cols:
        if chart_type == "line":
            if x is not None:
                ax.plot(x, df[y], label=y)
            else:
                ax.plot(df[y], label=y)
        elif chart_type == "bar":
            if x is not None:
                ax.bar(x, df[y], label=y)
            else:
                ax.bar(df.index, df[y], label=y)
        elif chart_type == "scatter":
            if x is not None:
                ax.scatter(x, df[y], label=y)
            else:
                ax.scatter(df.index, df[y], label=y)
        else:
            raise ValueError(f"Unsupported chart_type: {chart_type}")

    ax.set_title(title or f"{', '.join(y_cols)} vs {x_col or 'index'}")
    ax.set_xlabel(x_col or "index")
    ax.set_ylabel(", ".join(y_cols))
    ax.legend()
    plt.tight_layout()

    if output:
        fig.savefig(output)

    if show:
        plt.show()

    return fig


def _cli():
    parser = argparse.ArgumentParser(description="Plot CSV data using pandas and matplotlib")
    parser.add_argument("csv", help="Path to CSV file")
    parser.add_argument("--x", help="Column name to use for x-axis (optional)")
    parser.add_argument("--y", nargs="+", help="One or more column names to plot (optional)")
    parser.add_argument("--type", default="line", choices=["line", "bar", "scatter"], help="Chart type")
    parser.add_argument("--output", help="Output image path (e.g. out.png)")
    parser.add_argument("--title", help="Chart title")
    args = parser.parse_args()

    load_and_plot(
        args.csv, x_col=args.x, y_cols=args.y, chart_type=args.type, title=args.title, output=args.output
    )


if __name__ == "__main__":
    _cli()
