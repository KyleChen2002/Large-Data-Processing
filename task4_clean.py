import argparse
import pandas as pd


def clean_yearly_agg(
    input_path: str,
    output_path: str,
    year_min: int = 2008,
    year_max: int = 2024,
    min_count: int = 10,
) -> None:
    """
    Task 4: Data Cleaning and Sampling

    1. Limit years to [year_min, year_max], excluding artificial data after 2025.
    2. Remove rows missing year / tag / count.
    3. Convert numeric fields to numeric types.
    4. Filter noise tags: drop (year, tag) pairs with count < min_count.
    """

    print(f"Loading input from: {input_path}")
    df = pd.read_csv(input_path, sep="\t", header=None)

    df.columns = [
        "year",
        "tag",
        "count",
        "total_views",
        "total_score",
        "total_answers",
        "avg_views",
        "avg_score",
        "avg_answers",
    ]

    df = df.dropna(subset=["year", "tag", "count"])

    num_cols_int = ["year", "count", "total_views", "total_score", "total_answers"]
    num_cols_float = ["avg_views", "avg_score", "avg_answers"]

    for col in num_cols_int:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in num_cols_float:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["year", "count"])

    df = df[(df["year"] >= year_min) & (df["year"] <= year_max)]

    df = df[df["count"] >= min_count]

    df = df.reset_index(drop=True)

    print("After cleaning:")
    print(f"  rows = {len(df)}")
    print(f"  years = {sorted(df['year'].unique().tolist())[:5]} ...")

    df.to_csv(output_path, sep="\t", index=False)
    print(f"Saved cleaned data to: {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Task 4: Data Cleaning and Sampling")
    parser.add_argument(
        "--input",
        required=True,
        help="Path to yearly_agg.tsv (raw yearly aggregation output).",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Path to save cleaned yearly data (e.g., yearly_clean.tsv).",
    )
    parser.add_argument(
        "--year-min",
        type=int,
        default=2008,
        help="Minimum year to keep (inclusive). Default: 2008",
    )
    parser.add_argument(
        "--year-max",
        type=int,
        default=2024,
        help="Maximum year to keep (inclusive). Default: 2024",
    )
    parser.add_argument(
        "--min-count",
        type=int,
        default=10,
        help="Minimum count per (year, tag) to keep. Default: 10",
    )

    args = parser.parse_args()
    clean_yearly_agg(
        input_path=args.input,
        output_path=args.output,
        year_min=args.year_min,
        year_max=args.year_max,
        min_count=args.min_count,
    )


if __name__ == "__main__":
    main()
