import argparse
import math
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
    parser.add_argument("--source_table", default="gold_game_text_features")
    parser.add_argument("--output_path", required=True)

    parser.add_argument("--model_name", default=MODEL_NAME)
    parser.add_argument("--batch_size", type=int, default=128)
    parser.add_argument("--chunk_rows", type=int, default=50000)

    return parser.parse_args()


def parse_gcs_uri(uri: str):
    if not uri.startswith("gs://"):
        raise ValueError("output_path must start with gs://")
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
      game_text_document,
      game_text_document_len,
      text_completeness_score,
      text_quality_group
    FROM {source}
    WHERE app_id IS NOT NULL
      AND game_text_document IS NOT NULL
      AND TRIM(game_text_document) != ''
    ORDER BY app_id
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

        texts = chunk["game_text_document"].fillna("").astype(str).tolist()
        app_ids = chunk["app_id"].astype("int64").tolist()

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
            "app_id": app_ids,
            "embedding_model": args.model_name,
            "embedding_dim": EMBEDDING_DIM,
            "embedding": [emb.astype(float).tolist() for emb in embeddings],
            "game_text_document_len": chunk["game_text_document_len"].fillna(0).astype("int64").tolist(),
            "text_completeness_score": chunk["text_completeness_score"].fillna(0).astype(float).tolist(),
            "text_quality_group": chunk["text_quality_group"].fillna("").astype(str).tolist(),
            "embedded_at": datetime.now(timezone.utc)
        })

        dataframe_to_parquet_and_upload(out_df, args.output_path, part_idx)

        total_rows += len(out_df)
        part_idx += 1

    print(f"Done. Total embedded rows: {total_rows}")
    print(f"Output path: {args.output_path}")


if __name__ == "__main__":
    main()
