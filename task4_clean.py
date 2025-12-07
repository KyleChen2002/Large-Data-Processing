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

    1. 限制年份在 [year_min, year_max]，順便排除 2025 之後的人工資料。
    2. 移除缺 year / tag / count 的 row。
    3. 將數值欄位轉成數字型別。
    4. 過濾 noise tags：count < min_count 的 (year, tag) drop。
    """

    print(f"Loading input from: {input_path}")
    df = pd.read_csv(input_path, sep="\t", header=None)

    # 原本 yearly_agg.tsv 的欄位順序（依你 AggReducer 的輸出）
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

    # 1️⃣ 基本欄位存在檢查：year / tag / count 缺的直接丟
    df = df.dropna(subset=["year", "tag", "count"])

    # 2️⃣ 型別轉換
    # 年份、count、各種總和轉成數字，錯的就變 NaN
    num_cols_int = ["year", "count", "total_views", "total_score", "total_answers"]
    num_cols_float = ["avg_views", "avg_score", "avg_answers"]

    for col in num_cols_int:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in num_cols_float:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # 再丟一次：year / count 如果變成 NaN (無法轉成數字) 的也 drop
    df = df.dropna(subset=["year", "count"])

    # 3️⃣ 限制年份區間，順便把 2025 那些異常資料排掉
    df = df[(df["year"] >= year_min) & (df["year"] <= year_max)]

    # 4️⃣ 過濾 noise tags：count < min_count 的 row 直接丟掉
    df = df[df["count"] >= min_count]

    # 可選：reset index 比較乾淨
    df = df.reset_index(drop=True)

    print("After cleaning:")
    print(f"  rows = {len(df)}")
    print(f"  years = {sorted(df['year'].unique().tolist())[:5]} ...")

    # 5️⃣ 存成乾淨版 TSV，包含 header，方便之後在 notebook 用欄名操作
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
