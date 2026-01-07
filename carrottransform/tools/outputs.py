"""
this file contains several "output target" classes. each class is used to write carrot-transform output data in a different way. all classes are operated the same way - so - which output is in use can be selected by the CLICK argument type - also defined in this file.
"""

import io
import logging
import re
from enum import IntEnum
from pathlib import Path

import boto3
import click
import sqlalchemy
from sqlalchemy import Column, MetaData, Table, Text, insert

from carrottransform import require

logger = logging.getLogger(__name__)


class RateLimits(IntEnum):
    S3_LIMIT = 100 * 1024 * 1024  # 100 MB


class OutputTarget:
    """the OutputTarget classes provide a common abstraction for writing tables of data out of the program. each implementation offers an identical interface to some underlying storage mechanism"""

    def __init__(self, start, write, close):
        self._start = start
        self._write = write
        self._close = close
        self._active = {}

    class Handle:
        """
        a handle is a streaming connection to an individual table or file for a given output-target implementation
        """

        def __init__(self, host, name, item, shorten: bool, length: int):
            self._host = host
            self._name = name
            self._item = item
            self._shorten = shorten
            self._length = length

        def write(self, record: list[str]) -> None:
            require(self._length == len(record), f"{self._length=}, {len(record)=}")
            if self._shorten:
                record = record[:-1]
            self._host._write(self._item, record)

        def close(self) -> None:
            """close a single stream"""

            # perform the actual close operation
            self._host._close(self._item)

            # remove the handle fromt he list of handles
            del self._host._active[self._name]

    def start(self, name: str, header: list[str]) -> Handle:
        """
        opens a single handle to a single table or file with the given column names.
        """

        require(name not in self._active)

        length = len(header)
        shorten = header[-1] == ""

        if shorten:
            header = header[:-1]

        handle = self.Handle(
            host=self,
            name=name,
            item=self._start(name, header),
            shorten=shorten,
            length=length,
        )
        self._active[name] = handle
        return handle

    def close(self):
        """closes all active streams but doesn't prevent new ones from being opened"""

        # we need to loop like this to allow removing the entries from the dict in the loop body
        while 0 != len(self._active):
            # get the key for the first item
            name = next(iter(self._active))
            # close the first item
            self._active[name].close()


def csv_output_target(into: Path) -> OutputTarget:
    """creates an instance of the OutputTarget that points at a folder of csv files"""

    def start(name: str, header: list[str]):
        path = (into / name).with_suffix(".tsv")
        path.parent.mkdir(parents=True, exist_ok=True)
        file = path.open("w")
        file.write("\t".join(header) + "\n")
        return file

    def write(item, record):
        require(not isinstance(record, str))
        item.write("\t".join(record) + "\n")

    return OutputTarget(
        start,
        lambda item, record: write(item, record),
        lambda item: item.close(),
    )


def sql_output_target(connection: sqlalchemy.engine.Engine) -> OutputTarget:
    """creates an instance of the OutputTarget using the given SQLAlchemy connection"""

    def start(name: str, header: list[str]):
        # if you're adapting this to a non-dumb database; probably best to read the DDL or something and check/match the column types
        columns = [Column(name, Text()) for name in header]

        # create the table
        metadata = MetaData()
        table = Table(
            name,
            metadata,
            *(columns),
        )
        metadata.create_all(connection, tables=[table])

        # use this lambda to write a single record
        # ... rewriting the class "smells bad" when it already works and i'm struggling with s3
        def upload(record):
            with connection.begin() as conn:
                conn.execute(insert(table), dict(zip(header, record)))

        return upload

    return OutputTarget(start, lambda upload, record: upload(record), lambda item: 0)


class S3Tool:
    """this class simplifies s3 connections"""

    class S3UploadStream:
        """this class tracks a single upload stream. there's no download stream sibling; downloading is not streamed"""

        def __init__(self, tool, name: str):
            self._tool = tool
            self._name = self._tool.key_name(name)
            self._mpu = self._tool._s3.create_multipart_upload(
                Bucket=self._tool._bucket_name, Key=(self._name)
            )
            self._upload_id = self._mpu["UploadId"]
            self._buffer = io.BytesIO()
            self._parts: list[dict[str, int | object]] = []
            self._part_number = 1

    def __init__(self, s3, bucket_name: str, bucket_path: str):
        self._bucket_name = bucket_name
        self._bucket_path = bucket_path
        self._s3 = s3
        self._streams: dict[str, S3Tool.S3UploadStream] = {}

    def key_name(self, name):
        return self._bucket_path + name

    def scan(self) -> list[str]:
        seen = []
        response = self._s3.list_objects_v2(Bucket=self._bucket_name)

        if "Contents" in response:
            for obj in response["Contents"]:
                name = obj["Key"]
                require(name not in seen)
                seen.append(name)

        return seen

    def read(self, name: str):
        response = self._s3.get_object(
            Bucket=self._bucket_name, Key=(self.key_name(name))
        )
        return response["Body"].read().decode("utf-8")

    def delete(self, name: str):
        self._s3.delete_object(Bucket=self._bucket_name, Key=self.key_name(name))

    def new_stream(self, name: str):
        """start a stream for data we're going to upload"""
        require(name not in self._streams)
        self._streams[name] = S3Tool.S3UploadStream(self, name)

    def send_chunk(self, name: str, data):
        require(name in self._streams)

        stream = self._streams[name]

        stream._buffer.write(data)

        if stream._buffer.tell() >= RateLimits.S3_LIMIT:
            self.flush(stream)

    def complete_all(self):
        for name in self._streams:
            self.complete(name)
        self._streams = {}

    def complete(self, name):
        stream = self._streams[name]
        self.flush(stream)
        self._s3.complete_multipart_upload(
            Bucket=self._bucket_name,
            Key=stream._name,
            UploadId=stream._upload_id,
            MultipartUpload={"Parts": stream._parts},
        )

    def flush(self, stream):
        # upload the part
        stream._buffer.seek(0)
        resp = self._s3.upload_part(
            Bucket=self._bucket_name,
            Key=stream._name,
            PartNumber=stream._part_number,
            UploadId=stream._upload_id,
            Body=stream._buffer.read(),
        )

        # reset the buffer and parts
        stream._parts.append({"PartNumber": stream._part_number, "ETag": resp["ETag"]})
        stream._part_number += 1
        stream._buffer = io.BytesIO()


# Pattern to extract all components
MINIO_URL_PATTERN = r"^minio:([^:]+):([^@]+)@(https?)://([^:/]+):(\d+)/([^/]+)/?(.*)$"


class MinioURL:
    """parses/breaks a MinioURL up into the intended components"""

    def __init__(self, text: str):
        match = re.match(MINIO_URL_PATTERN, text)

        if not match:
            raise Exception(f"malformed minio URL {text=}")

        self._user = match.group(1)
        self._pass = match.group(2)
        self._protocol = match.group(3)
        self._host = match.group(4)
        self._port = match.group(5)
        self._bucket = match.group(6)
        self._folder = match.group(7)


def minio_output_target(coordinate: str) -> OutputTarget:
    """create an output target for a folder in an minio bucket"""

    bucket = MinioURL(coordinate)

    s3_client = boto3.client(
        "s3",
        endpoint_url=f"{bucket._protocol}://{bucket._host}:{bucket._port}",
        aws_access_key_id=bucket._user,
        aws_secret_access_key=bucket._pass,
    )

    s3_tool = S3Tool(s3_client, bucket._bucket, bucket._folder)

    def start(name: str, header: list[str]):
        s3_tool.new_stream(name)
        s3_tool.send_chunk(name, ("\t".join(header) + "\n").encode("utf-8"))
        return name

    return OutputTarget(
        start,
        lambda name, record: s3_tool.send_chunk(
            name, ("\t".join(record) + "\n").encode("utf-8")
        ),
        lambda name: s3_tool.complete(name),
    )


def s3_bucket_folder(coordinate: str):
    """splits the uri-like coordinate strings for S3 into [bucket, subfolder] data"""

    require(coordinate.startswith("s3:"))
    require(
        "/" in coordinate,
        f"need the format <s3>:<bucket>/<folder> but was {coordinate=}",
    )

    bucket = coordinate.split("/")[0]
    folder = coordinate[len(bucket) + 1 :]

    if not folder.endswith("/"):
        folder += "/"
    return [bucket[3:], folder]


class OutputTargetArgumentType(click.ParamType):
    """creates an output target for a command line string parameter"""

    name = "a connection to the/a target (whatever that may be)"

    def convert(self, value: str, param, ctx):
        value = str(value)
        if value.startswith("minio:"):
            return minio_output_target(value)

        try:
            return sql_output_target(sqlalchemy.create_engine(value))
        except sqlalchemy.exc.ArgumentError as argumentError:
            require(
                "Could not parse SQLAlchemy URL from given URL string"
                == str(argumentError)
            )

        return csv_output_target(Path(value))


# create a singleton for the Click settings
TargetArgument = OutputTargetArgumentType()
