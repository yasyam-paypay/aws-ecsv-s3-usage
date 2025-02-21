import pandas as pd
import glob
import os

def process_aws_invoices(directory, output_file):
    # filesディレクトリのecsv_*.csvファイルをすべて取得
    files = glob.glob(os.path.join(directory, 'ecsv_*.csv'))

    # 種類ごとのストレージタイプを定義
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

    # 各ファイルを処理
    all_data = []
    for file in files:
        df = pd.read_csv(file)

        # 条件0: PayerLineItemId が存在 (Payer アカウントの情報だけを取得)
        condition0 = df['RecordType'] == 'PayerLineItem'
        
        # 条件1: ProductName が Amazon Simple Storage Service
        condition1 = df['ProductName'] == 'Amazon Simple Storage Service'
        
        # 条件2: UsageType の末尾が指定されたパターンであるか
        condition2 = df['UsageType'].apply(
            lambda x: isinstance(x, str) and any(x.endswith(pattern) for pattern in usage_patterns.keys())
        )

        # フィルタリングの実施
        filtered_df = df[condition0 & condition1 & condition2].copy()

        # 必要な列を抽出し、変換
        filtered_df['storge_type_raw'] = filtered_df['UsageType']
        filtered_df['region'] = filtered_df['UsageType'].str.split('-').str[0]
        filtered_df['year_month'] = pd.to_datetime(filtered_df['UsageStartDate']).dt.strftime('%Y-%m')
        filtered_df['storge_type'] = filtered_df['UsageType'].apply(
            lambda x: next((usage_patterns[pattern] for pattern in usage_patterns if x.endswith(pattern)), 'Unknown')
        )
        filtered_df['GB-Month'] = filtered_df['UsageQuantity']

        # 必要な列のDataFrame
        processed_df = filtered_df[['region', 'year_month', 'storge_type', 'storge_type_raw', 'GB-Month']]
        
        # 処理したDataFrameをリストに追加
        all_data.append(processed_df)
        print(f"{file} を処理しました。")

    # 全てのデータフレームを1つに結合
    combined_df = pd.concat(all_data, ignore_index=True)

    # year_month, storge_type_raw, region, storge_type で groupby し、GB-Month を合計
    aggregated_df = combined_df.groupby(
        ['region', 'year_month', 'storge_type', 'storge_type_raw'], as_index=False
    )['GB-Month'].sum()

    # 結果をCSVに書き込み
    aggregated_df.to_csv(output_file, index=False)
    print(f"データが {output_file} に保存されました。")

# メインスクリプトの実行
if __name__ == "__main__":
    input_directory = './files'  # インプットファイルが保管されているディレクトリ
    output_csv = 'result.csv'  # 出力ファイル名

    process_aws_invoices(input_directory, output_csv)
