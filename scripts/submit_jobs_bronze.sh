REGION="YOUR_REGION"
BUCKET="YOUR_BUCKET"
CODE_PATH="$BUCKET/code/steam_bigdata"
PROJECT_ID = "YOUR_PROJECT_ID"

## Submit jobs bronze
gcloud dataproc batches submit pyspark \
  $BUCKET/26_init/code/jobs/01_profile_bronze.py \
  --project=$PROJECT_ID \
  --region=$REGION \
  --batch=bronze-profile-job \
  --deps-bucket=$BUCKET/deps \
  --py-files=$BUCKET/code/steam_bigdata.zip