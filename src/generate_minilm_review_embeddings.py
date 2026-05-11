"""
generate_minilm_review_embeddings.py

Generate MiniLM embeddings for Steam review chunks.

Mục đích:
- Đọc bảng BigQuery steam_gold.gold_app_language_review_chunks.
- Lấy app_id, language, chunk_id, merged_review_text.
- Dùng pretrained multilingual MiniLM để encode review chunks.
- Ghi embedding ra GCS dạng Parquet.
- Output dùng để load vào BigQuery:
  steam_gold.gold_minilm_review_chunk_embeddings

Model:
sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2

Cách chạy mẫu:

python generate_minilm_review_embeddings.py \
  --project_id project-79499e5c-69d7-42b8-864 \
  --dataset steam_gold \
  --source_table gold_app_language_review_chunks \
  --output_path gs://truong_bigdata_24032026_init/embeddings/minilm_app_language_review_chunks \
  --batch_size 128
"""

import argparse
import os
import tempfile
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from google.cloud import bigquery, storage
from sentence_transformers import SentenceTransformer


MODEL_NAME = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
EMBEDDING_DIM = 384


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--project_id", required=True)
    parser.add_argument("--dataset", default="steam_gold")
    parser.add_argument("--source_table", default="gold_app_language_review_chunks")
    parser.add_argument("--output_path", required=True)

    parser.add_argument("--model_name", default=MODEL_NAME)
    parser.add_argument("--batch_size", type=int, default=128)
    parser.add_argument("--chunk_rows", type=int, default=50000)

    return parser.parse_args()


def parse_gcs_uri(uri: str):
    if not uri.startswith("gs://"):
        raise ValueError("GCS path must start with gs://")
    no_scheme = uri[5:]
    bucket, _, prefix = no_scheme.partition("/")
    return bucket, prefix.rstrip("/")


def upload_file_to_gcs(local_path: str, gcs_uri: str):
    bucket_name, prefix = parse_gcs_uri(gcs_uri)
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(prefix)
    blob.upload_from_filename(local_path)


def dataframe_to_parquet_and_upload(df: pd.DataFrame, output_path: str, part_idx: int):
    bucket_name, prefix = parse_gcs_uri(output_path)

    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = os.path.join(tmpdir, f"part-{part_idx:05d}.parquet")
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, local_path, compression="snappy")

        gcs_file_uri = f"gs://{bucket_name}/{prefix}/part-{part_idx:05d}.parquet"
        upload_file_to_gcs(local_path, gcs_file_uri)
        print(f"Uploaded {gcs_file_uri}")


def main():
    args = parse_args()

    bq_client = bigquery.Client(project=args.project_id)

    source = f"`{args.project_id}.{args.dataset}.{args.source_table}`"

    query = f"""
    SELECT
      app_id,
      language,
      chunk_id,
      review_count,
      user_count,
      positive_count,
      negative_count,
      avg_rating,
      avg_hours,
      total_helpful_score,
      total_funny_score,
      avg_review_text_length,
      merged_text_length,
      merged_review_text
    FROM {source}
    WHERE app_id IS NOT NULL
      AND language IS NOT NULL
      AND chunk_id IS NOT NULL
      AND merged_review_text IS NOT NULL
      AND TRIM(merged_review_text) != ''
    ORDER BY app_id, language, chunk_id
    """

    print("Loading MiniLM model:", args.model_name)
    model = SentenceTransformer(args.model_name)

    print("Device:", model.device)
    print("Reading BigQuery data...")

    job = bq_client.query(query)
    iterator = job.result(page_size=args.chunk_rows).to_dataframe_iterable()

    part_idx = 0
    total_rows = 0

    for chunk in iterator:
        if chunk.empty:
            continue

        texts = chunk["merged_review_text"].fillna("").astype(str).tolist()

        print(f"Encoding part {part_idx}, rows={len(texts)}")

        embeddings = model.encode(
            texts,
            batch_size=args.batch_size,
            show_progress_bar=True,
            normalize_embeddings=True
        )

        embeddings = np.asarray(embeddings, dtype=np.float32)

        if embeddings.shape[1] != EMBEDDING_DIM:
            raise ValueError(f"Unexpected embedding dim: {embeddings.shape[1]}")

        out_df = pd.DataFrame({
            "app_id": chunk["app_id"].astype("int64").tolist(),
            "language": chunk["language"].fillna("").astype(str).tolist(),
            "chunk_id": chunk["chunk_id"].astype("int64").tolist(),

            "embedding_model": args.model_name,
            "embedding_dim": EMBEDDING_DIM,
            "embedding": [emb.astype(float).tolist() for emb in embeddings],

            "review_count": chunk["review_count"].fillna(0).astype("int64").tolist(),
            "user_count": chunk["user_count"].fillna(0).astype("int64").tolist(),
            "positive_count": chunk["positive_count"].fillna(0).astype("int64").tolist(),
            "negative_count": chunk["negative_count"].fillna(0).astype("int64").tolist(),
            "avg_rating": chunk["avg_rating"].fillna(0).astype(float).tolist(),
            "avg_hours": chunk["avg_hours"].fillna(0).astype(float).tolist(),
            "total_helpful_score": chunk["total_helpful_score"].fillna(0).astype(float).tolist(),
            "total_funny_score": chunk["total_funny_score"].fillna(0).astype(float).tolist(),
            "avg_review_text_length": chunk["avg_review_text_length"].fillna(0).astype(float).tolist(),
            "merged_text_length": chunk["merged_text_length"].fillna(0).astype("int64").tolist(),

            "embedded_at": datetime.now(timezone.utc)
        })

        dataframe_to_parquet_and_upload(out_df, args.output_path, part_idx)

        total_rows += len(out_df)
        part_idx += 1

    print(f"Done. Total embedded review chunks: {total_rows}")
    print(f"Output path: {args.output_path}")


if __name__ == "__main__":
    main()
