import pandas as pd

def process_aws_invoice(input_file, output_file):
    # CSVファイルを読み込み
    df = pd.read_csv(input_file)

    # 条件1: ProductName が Amazon Simple Storage Service
    condition1 = df['ProductName'] == 'Amazon Simple Storage Service'

    # 条件2: UsageType の末尾が指定されたパターンであるか
    usage_patterns = [
        'TimedStorage-ByteHrs',
        'TimedStorage-GDA-ByteHrs',
        'TimedStorage-GDA-Staging',
        'TimedStorage-GIR-ByteHrs',
        'TimedStorage-GIR-SmObjects',
        'TimedStorage-GlacierByteHrs',
        'TimedStorage-GlacierStaging',
        'TimedStorage-INT-FA-ByteHrs',
        'TimedStorage-INT-IA-ByteHrs',
        'TimedStorage-INT-AA-ByteHrs',
        'TimedStorage-INT-AIA-ByteHrs',
        'TimedStorage-INT-DAA-ByteHrs',
        'TimedStorage-RRS-ByteHrs',
        'TimedStorage-SIA-ByteHrs',
        'TimedStorage-SIA-SmObjects',
        'TimedStorage-XZ-ByteHrs',
        'TimedStorage-ZIA-ByteHrs',
        'TimedStorage-ZIA-SmObjects'
    ]

    # usage_patternsのどれかでUsageTypeが終わるかをチェック
    condition2 = df['UsageType'].apply(lambda x: isinstance(x, str) and any(x.endswith(pattern) for pattern in usage_patterns))

    # フィルタリングの実施
    filtered_df = df[condition1 & condition2].copy()  # .copy()でコピーを作成

    # 必要な列を抽出し、変換
    filtered_df.loc[:, 'region'] = filtered_df['UsageType'].str.split('-').str[0]
    filtered_df.loc[:, 'year_month'] = pd.to_datetime(filtered_df['UsageStartDate']).dt.strftime('%Y-%m')
    filtered_df.loc[:, 'storge_type'] = filtered_df['UsageType']
    filtered_df.loc[:, 'usage_quantity'] = filtered_df['UsageQuantity']

    # 結果を出力用のDataFrameに整形
    result_df = filtered_df[['region', 'year_month', 'storge_type', 'usage_quantity']]

    # 結果をCSVに書き込み
    result_df.to_csv(output_file, index=False)

# メインスクリプトの実行
if __name__ == "__main__":
    input_csv = 'ecsv_1_2025.csv'  # 入力ファイル名
    output_csv = 'result_1_2025.csv'  # 出力ファイル名

    process_aws_invoice(input_csv, output_csv)
    print(f"データが {output_csv} に保存されました。")
