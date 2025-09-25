import csv
import io
import logging
from pathlib import Path

import boto3
import click
import sqlalchemy
from sqlalchemy import MetaData, select

logger = logging.getLogger(__name__)


class OutputTarget:
    def __init__(self, start, write, close):
        self._start = start
        self._write = write
        self._close = close
        self._active = {}

    class Handle:
        def __init__(self, host, name, item):
            self._host = host
            self._name = name
            self._item = item

        def write(self, record):
            self._host._write(self._item, record)

        def close(self):
            """close a single stream"""

            # perform the actual close operation
            self._host._close(self._item)

            # remove the handle fromt he list of handles
            del self._host._active[self._name]

    def start(self, name: str, header: list[str]):
        assert name not in self._active

        handle = self.Handle(self, name, self._start(name, header))
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


def csvOutputTarget(into: Path) -> OutputTarget:
    """creates an instance of the OutputTarget that points at simple .csv files"""

    def start(name: str, header: list[str]):
        path = (into / name).with_suffix(".tsv")
        path.parent.mkdir(parents=True, exist_ok=True)
        file = path.open("w")
        file.write("\t".join(header) + "\n")
        return file

    return OutputTarget(
        start,
        lambda item, record: item.write("\t".join(record) + "\n"),
        lambda item: item.close(),
    )


def sqlOutputTarget(connection: sqlalchemy.engine.Engine) -> OutputTarget:
    """creates an instance of the OutputTarget that points at simple .csv files"""

    def start(name: str, header: list[str]):
        from sqlalchemy import Column, MetaData, Table, Text, insert

        # if you're adapting this to a non-dumb database; probably best to read teh DDL or something and check/match the column types
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
    class S3UploadStream:
        def __init__(self, tool, name):
            self._name = name
            self._tool = tool
            self._mpu = self._tool._s3.create_multipart_upload(
                Bucket=self._tool._bucket, Key=self._name
            )
            self._upload_id = self._mpu["UploadId"]
            self._buffer = io.BytesIO()
            self._parts = []
            self._part_number = 1

    def __init__(self, s3, bucket: str, limit: int = 100 * 1024 * 1024):
        self._s3 = s3
        self._bucket = bucket
        self._limit = limit
        self._streams: dict[str, S3Tool.S3UploadStream] = {}

    def scan(self) -> list[str]:
        seen = []
        response = self._s3.list_objects_v2(Bucket=self._bucket)

        if "Contents" in response:
            for obj in response["Contents"]:
                name = obj["Key"]
                assert name not in seen
                seen.append(name)

        return seen

    def read(self, name: str):
        response = self._s3.get_object(Bucket=self._bucket, Key=name)
        return response["Body"].read().decode("utf-8")

    def delete(self, name: str):
        self._s3.delete_object(Bucket=self._bucket, Key=name)

    def new_stream(self, name: str):
        """start a stre for date we're going to upload"""
        assert name not in self._streams
        self._streams[name] = S3Tool.S3UploadStream(self, name)

    def send_chunk(self, name: str, data):
        assert name in self._streams

        stream = self._streams[name]

        stream._buffer.write(data)

        if stream._buffer.tell() >= self._limit:
            self.flush(stream)

    def complete_all(self):
        for name in self._streams:
            self.complete(name)
        self._streams = {}

    def complete(self, name):
        stream = self._streams[name]
        self.flush(stream)
        self._s3.complete_multipart_upload(
            Bucket=self._bucket,
            Key=stream._name,
            UploadId=stream._upload_id,
            MultipartUpload={"Parts": stream._parts},
        )

    def flush(self, stream):
        # upload the part
        stream._buffer.seek(0)
        resp = self._s3.upload_part(
            Bucket=self._bucket,
            Key=stream._name,
            PartNumber=stream._part_number,
            UploadId=stream._upload_id,
            Body=stream._buffer.read(),
        )

        # reset the buffer and parts
        stream._parts.append({"PartNumber": stream._part_number, "ETag": resp["ETag"]})
        stream._part_number += 1
        stream._buffer = io.BytesIO()


def s3OutputTarget(s3tool: S3Tool) -> OutputTarget:
    def start(name: str, header: list[str]):
        s3tool.new_stream(name)
        s3tool.send_chunk(name, ("\t".join(header) + "\n").encode("utf-8"))
        return name

    return OutputTarget(
        start,
        lambda name, record: s3tool.send_chunk(
            name, ("\t".join(record) + "\n").encode("utf-8")
        ),
        lambda name: s3tool.complete(name),
    )


class OutputTargetArgumentType(click.ParamType):
    """"""

    name = "a connection to the/a target (whatever that may be)"

    def convert(self, value, param, ctx):
        if value.startswith("s3:"):
            raise Exception("flork the write-to-s3")
        else:
            return csvOutputTarget(Path(value))


# create a singleton for the Click settings
TargetArgument = OutputTargetArgumentType()
