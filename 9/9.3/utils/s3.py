import logging

from pathlib import Path
from contextlib import asynccontextmanager
from aiobotocore.session import get_session
from botocore.exceptions import ClientError


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class AsyncObjectStorage:
    def __init__(self, *, key_id: str, secret: str, endpoint: str, bucket: str):
        self._auth = {
            "aws_access_key_id": key_id,
            "aws_secret_access_key": secret,
            "endpoint_url": endpoint,
        }
        self._bucket = bucket
        self._session = get_session()

    @asynccontextmanager
    async def _connect(self):
        async with self._session.create_client("s3", **self._auth) as connection:
            yield connection

    async def send_file(self, local_source: str, remote_path: str = None):
        file_ref = Path(local_source)
        target_name = remote_path if remote_path else file_ref.name
        async with self._connect() as remote:
            with file_ref.open("rb") as binary_data:
                await remote.put_object(
                    Bucket=self._bucket, Key=target_name, Body=binary_data
                )

    async def fetch_file(self, remote_name: str, local_target: str):
        async with self._connect() as remote:
            try:
                response = await remote.get_object(Bucket=self._bucket, Key=remote_name)
                body = await response["Body"].read()
                with open(local_target, "wb") as out:
                    out.write(body)
            except ClientError as e:
                logger.error(f"Failed to fetch file {remote_name}: {str(e)}")
                raise

    async def remove_file(self, remote_name: str):
        async with self._connect() as remote:
            await remote.delete_object(Bucket=self._bucket, Key=remote_name)

    async def list_files(self):
        async with self._connect() as remote:
            response = await remote.list_objects_v2(Bucket=self._bucket)
            if "Contents" in response:
                return [obj["Key"] for obj in response["Contents"]]
            return []

    async def file_exists(self, remote_name: str):
        async with self._connect() as remote:
            try:
                await remote.head_object(Bucket=self._bucket, Key=remote_name)
                return True
            except ClientError:
                return False
