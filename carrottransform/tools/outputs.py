"""
this file contains several "output target" classes. each class is used to write carrot-transform output data in a different way. all classes are operated the same way - so - which output is in use can be selected by the CLICK argument type - also defined in this file.
"""

import io
import logging
from pathlib import Path

import boto3
import click
import sqlalchemy
from sqlalchemy import MetaData

logger = logging.getLogger(__name__)


def require(con: bool, msg: str = ""):
    if "" != msg:
        msg = "\n\t" + msg
    import inspect

    if con:
        return
    # Get the calling frame and its code context
    currentframe = inspect.currentframe()
    frame = currentframe.f_back if currentframe is not None else None
    frame_info = inspect.getframeinfo(frame) if frame is not None else None

    context = frame_info.code_context if frame_info is not None else None
    if context:
        call_line = context[0].strip()
        raise AssertionError(
            f"failed {frame_info.filename}:{frame_info.lineno}: {call_line}{msg}"
        )
    if frame_info is not None:
        raise AssertionError(f"failed {frame_info.filename}:{frame_info.lineno}{msg}")

    raise AssertionError(f"failed requirement{msg}")


class OutputTarget:
    def __init__(self, start, write, close):
        self._start = start
        self._write = write
        self._close = close
        self._active = {}

    class Handle:
        def __init__(self, host, name, item, shorten: bool, length: int):
            self._host = host
            self._name = name
            self._item = item
            self._shorten = shorten
            self._length = length

        def write(self, record):
            require(self._length == len(record), f"{self._length=}, {len(record)=}")
            if self._shorten:
                record = record[:-1]
            self._host._write(self._item, record)

        def close(self):
            """close a single stream"""

            # perform the actual close operation
            self._host._close(self._item)

            # remove the handle fromt he list of handles
            del self._host._active[self._name]

    def start(self, name: str, header: list[str]):
        assert name not in self._active

        length = len(header)
        shorten = "" == header[-1]

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
        from sqlalchemy import Column, Table, Text, insert

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
    class S3UploadStream:
        def __init__(self, tool, name):
            self._tool = tool
            self._name = self._tool.key_name(name)
            self._mpu = self._tool._s3.create_multipart_upload(
                Bucket=self._tool._bucket_name, Key=(self._name)
            )
            self._upload_id = self._mpu["UploadId"]
            self._buffer = io.BytesIO()
            self._parts = []
            self._part_number = 1

    def __init__(self, s3, coordinate: str, limit: int = 100 * 1024 * 1024):
        if "/" in coordinate:
            [b, f] = s3BucketFolder(coordinate)
            self._bucket_name = b
            self._bucket_path = f
        else:
            self._bucket_name = coordinate[3:]
            self._bucket_path = ""

        self._s3 = s3
        self._limit = limit
        self._streams: dict[str, S3Tool.S3UploadStream] = {}

    def key_name(self, name):
        return self._bucket_path + name

    def scan(self) -> list[str]:
        seen = []
        response = self._s3.list_objects_v2(Bucket=self._bucket_name)

        if "Contents" in response:
            for obj in response["Contents"]:
                name = obj["Key"]
                assert name not in seen
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


def s3BucketFolder(coordinate: str):
    assert "/" in coordinate, (
        f"need <s3>:<bucket>/<folder> at the lease but was {coordinate=}"
    )
    assert coordinate.startswith("s3:")

    bucket = coordinate.split("/")[0]
    folder = coordinate[len(bucket) + 1 :]

    if not folder.endswith("/"):
        folder += "/"
    return [bucket[3:], folder]


def s3OutputTarget(coordinate: str) -> OutputTarget:
    s3tool = S3Tool(boto3.client("s3"), coordinate)

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

    def convert(self, value: str, param, ctx):
        value = str(value)
        if value.startswith("s3:"):
            return s3OutputTarget(value)

        try:
            return sqlOutputTarget(sqlalchemy.create_engine(value))
        except sqlalchemy.exc.ArgumentError as argumentError:
            require(
                "Could not parse SQLAlchemy URL from given URL string"
                == str(argumentError)
            )

        return csvOutputTarget(Path(value))


# create a singleton for the Click settings
TargetArgument = OutputTargetArgumentType()
