"""
load_to_bigquery.py

Load Parquet files from GCS to BigQuery.

Mục đích:
- Load output Parquet của ALS hoặc MiniLM từ GCS vào BigQuery.
- Tự động dùng source_format=PARQUET.
- Có thể overwrite hoặc append.

Ví dụ load ALS:

python load_to_bigquery.py \
  --project_id project-79499e5c-69d7-42b8-864 \
  --dataset steam_gold \
  --table gold_recommendations_top30_10000users_demo \
  --source_uri gs://truong_bigdata_24032026_init/gold/gold_recommendations_top30_10000users_demo/*.parquet \
  --write_disposition WRITE_TRUNCATE

Ví dụ load MiniLM game text embedding:

python load_to_bigquery.py \
  --project_id project-79499e5c-69d7-42b8-864 \
  --dataset steam_gold \
  --table gold_minilm_game_text_embeddings \
  --source_uri gs://truong_bigdata_24032026_init/embeddings/minilm_game_text_features/*.parquet \
  --write_disposition WRITE_TRUNCATE

Ví dụ load MiniLM review chunk embedding:

python load_to_bigquery.py \
  --project_id project-79499e5c-69d7-42b8-864 \
  --dataset steam_gold \
  --table gold_minilm_review_chunk_embeddings \
  --source_uri gs://truong_bigdata_24032026_init/embeddings/minilm_app_language_review_chunks/*.parquet \
  --write_disposition WRITE_TRUNCATE
"""

import argparse
from google.cloud import bigquery


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--project_id", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--source_uri", required=True)

    parser.add_argument(
        "--write_disposition",
        default="WRITE_TRUNCATE",
        choices=["WRITE_TRUNCATE", "WRITE_APPEND", "WRITE_EMPTY"]
    )

    parser.add_argument(
        "--create_disposition",
        default="CREATE_IF_NEEDED",
        choices=["CREATE_IF_NEEDED", "CREATE_NEVER"]
    )

    return parser.parse_args()


def main():
    args = parse_args()

    client = bigquery.Client(project=args.project_id)

    table_id = f"{args.project_id}.{args.dataset}.{args.table}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=args.write_disposition,
        create_disposition=args.create_disposition
    )

    print("Loading GCS to BigQuery")
    print("Source URI:", args.source_uri)
    print("Target table:", table_id)
    print("Write disposition:", args.write_disposition)

    load_job = client.load_table_from_uri(
        args.source_uri,
        table_id,
        job_config=job_config
    )

    load_job.result()

    table = client.get_table(table_id)

    print("Load completed.")
    print("Rows:", table.num_rows)
    print("Schema:")
    for field in table.schema:
        print(f"- {field.name}: {field.field_type} ({field.mode})")


if __name__ == "__main__":
    main()
