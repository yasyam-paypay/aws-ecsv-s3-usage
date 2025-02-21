import pandas as pd

def process_aws_invoice(input_file, output_file):
    # CSVファイルを読み込み
    df = pd.read_csv(input_file)

    # 条件1: ProductName が Amazon Simple Storage Service
    condition1 = df['ProductName'] == 'Amazon Simple Storage Service'

    # 条件2: UsageType の末尾が指定されたパターンであるか
    usage_patterns = {
        'TimedStorage-ByteHrs': 'S3 Standard',
        'TimedStorage-GDA-ByteHrs': 'S3 Glacier Deep Archive',
        'TimedStorage-GDA-Staging': 'S3 Glacier Deep Archive Staging',
        'TimedStorage-GIR-ByteHrs': 'S3 Glacier Instant Retrieval',
        'TimedStorage-GIR-SmObjects': 'S3 Glacier Flexible Retrieval (Small Objects)',
        'TimedStorage-GlacierByteHrs': 'S3 Glacier Flexible Retrieval',
        'TimedStorage-GlacierStaging': 'S3 Glacier Flexible Retrieval Staging',
        'TimedStorage-INT-FA-ByteHrs': 'S3 Intelligent-Tiering (Frequent Access)',
        'TimedStorage-INT-IA-ByteHrs': 'S3 Intelligent-Tiering (Infrequent Access)',
        'TimedStorage-INT-AA-ByteHrs': 'S3 Intelligent-Tiering (Archive Access)',
        'TimedStorage-INT-AIA-ByteHrs': 'S3 Intelligent-Tiering (Archive Instant Access)',
        'TimedStorage-INT-DAA-ByteHrs': 'S3 Intelligent-Tiering (Deep Archive Access)',
        'TimedStorage-RRS-ByteHrs': 'Reduced Redundancy Storage (RRS)',
        'TimedStorage-SIA-ByteHrs': 'S3 Standard-IA',
        'TimedStorage-SIA-SmObjects': 'S3 Standard-IA (Small Objects)',
        'TimedStorage-XZ-ByteHrs': 'S3 Express One Zone',
        'TimedStorage-ZIA-ByteHrs': 'S3 One Zone-IA',
        'TimedStorage-ZIA-SmObjects': 'S3 One Zone-IA (Small Objects)'
    }

    # usage_patternsのどれかでUsageTypeが終わるかをチェック
    condition2 = df['UsageType'].apply(
        lambda x: isinstance(x, str) and any(x.endswith(pattern) for pattern in usage_patterns.keys())
    )

    # フィルタリングの実施
    filtered_df = df[condition1 & condition2].copy()

    # 必要な列を抽出し、変換
    filtered_df['storge_type_raw'] = filtered_df['UsageType']
    filtered_df['region'] = filtered_df['UsageType'].str.split('-').str[0]
    filtered_df['year_month'] = pd.to_datetime(filtered_df['UsageStartDate']).dt.strftime('%Y-%m')
    filtered_df['storge_type'] = filtered_df['UsageType'].apply(
        lambda x: next((usage_patterns[pattern] for pattern in usage_patterns if x.endswith(pattern)), 'Unknown')
    )
    filtered_df['usage_quantity'] = filtered_df['UsageQuantity']

    # 結果を出力用のDataFrameに整形
    result_df = filtered_df[['region', 'year_month', 'storge_type_raw', 'storge_type', 'usage_quantity']]

    # 結果をCSVに書き込み
    result_df.to_csv(output_file, index=False)

# メインスクリプトの実行
if __name__ == "__main__":
    input_csv = 'ecsv_1_2025.csv'  # 入力ファイル名
    output_csv = 'result_1_2025.csv'  # 出力ファイル名

    process_aws_invoice(input_csv, output_csv)
    print(f"データが {output_csv} に保存されました。")
