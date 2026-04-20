import os
import json
import logging
import tempfile
import boto3
from google.cloud import storage
from tenacity import retry, wait_exponential, stop_after_attempt


class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "level": record.levelname,
            "message": record.getMessage(),
            "trace_id": os.environ.get("CORRELATION_ID", "unknown"),
        }
        return json.dumps(log_record)


logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())
logger.addHandler(handler)
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
gcs = storage.Client()


@retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(3))
def process_transfer(src_bucket, src_key, dst_bucket, dst_key):
    logger.info(f"Initiating transfer: s3://{src_bucket}/{src_key} -> gs://{dst_bucket}/{dst_key}")

    fd, local_tmp = tempfile.mkstemp(prefix="worker_", suffix=f"_{os.path.basename(src_key)}")

    try:
        with os.fdopen(fd, "wb") as _:
            pass
        s3.download_file(src_bucket, src_key, local_tmp)

        bucket = gcs.bucket(dst_bucket)
        blob = bucket.blob(dst_key)
        blob.upload_from_filename(local_tmp)

        logger.info("Transfer completed successfully")

    except Exception as e:
        logger.error(f"Transfer failed: {str(e)}")
        raise
    finally:
        if os.path.exists(local_tmp):
            os.remove(local_tmp)


if __name__ == "__main__":
    source_bucket = os.environ.get("SOURCE_BUCKET")
    source_key = os.environ.get("SOURCE_KEY")
    dest_bucket = os.environ.get("DEST_BUCKET")
    dest_key = os.environ.get("DEST_KEY")

    env_vars = {
        "SOURCE_BUCKET": source_bucket,
        "SOURCE_KEY": source_key,
        "DEST_BUCKET": dest_bucket,
        "DEST_KEY": dest_key,
    }
    missing = [k for k, v in env_vars.items() if not v]
    if missing:
        logger.error(f"Missing required environment variables: {', '.join(missing)}")
        exit(1)

    process_transfer(source_bucket, source_key, dest_bucket, dest_key)
