from __future__ import absolute_import
import __builtin__
import warc as ia_warc
from urllib3 import HTTPResponse
from urllib3._collections import HTTPHeaderDict
import gzip


class WARCResponseRecord(ia_warc.WARCRecord):
    """
    Subclass of WARCRecord that exposes an HttpResponse for a response record payload.

    The HttpResponse provides a usable interface to the response and handles chunking and
    decoding.
    """
    def __init__(self, warc_header, http_response):
        ia_warc.WARCRecord.__init__(self, warc_header, defaults=False)
        self.http_response = http_response


class WARCResponseReader:
    """
    A reader of WARC response records that returns WARCResponseRecords.
    """
    def __init__(self, fileobj):
        self._warc_reader = ia_warc.WARCReader(fileobj)

    def __iter__(self):
        for record in self._warc_reader:
            # Only handle responses.
            if record.type == "response":
                payload = PayloadFilePart(record.payload.fileobj, record.payload.length)
                self._warc_reader.current_payload = payload

                # Read http start line and headers
                start_line, http_headers = self.read_http_start_line_and_headers(payload)
                if start_line.startswith("HTTP/1.1 200 OK"):

                    http_response = HTTPResponse(body=payload, headers=http_headers, status=200, version=1.1,
                                                 reason="OK", preload_content=False)
                    yield WARCResponseRecord(record.header, http_response)

    @staticmethod
    def read_http_start_line_and_headers(payload):
        headers = HTTPHeaderDict()
        start_line = None
        for count, line in enumerate(payload):
            if count == 0:
                start_line = line
            elif line == "\r\n":
                break
            else:
                pos = line.index(":")
                assert pos != -1
                headers.add(line[:pos], line[pos+1:].strip())
        return start_line, headers


class WARCResponseFile(ia_warc.WARCFile):
    def __init__(self, filename):
        # Creating own fileobj to bypass IA WARC's own gzip2.
        if filename.endswith(".gz"):
            fileobj = gzip.open(filename, "rb")
        else:
            fileobj = __builtin__.open(filename, "rb")
        ia_warc.WARCFile.__init__(self, fileobj=fileobj)

    @property
    def reader(self):
        if self._reader is None:
            self._reader = WARCResponseReader(self.fileobj)
        return self._reader


class PayloadFilePart:
    """
    File interface for WARC record payloads.
    """
    def __init__(self, fileobj, length):
        self.offset = 0

        self.fileobj = fileobj
        self.start_position = fileobj.tell()
        self.length = length
        self.end_offset = length - 1

    def read(self, size=-1):
        if size == -1:
            size = self.length
        ending_offset = min(self.offset + size, self.length)
        buf = ""
        while self.offset != ending_offset:
            file_section_offset = self.offset + self.start_position
            remaining_size = self.length - file_section_offset + self.start_position
            read_size = min(size - len(buf), remaining_size)
            self.fileobj.seek(file_section_offset)
            buf += self.fileobj.read(read_size)
            self.offset += read_size
            assert self.offset <= ending_offset
        return buf

    def tell(self):
        return self.offset

    def seek(self, offset, whence=0):
        if whence == 0:
            # Seek from start
            self.offset = offset
        elif whence == 1:
            # Relative to current
            self.offset += offset
        elif whence == 2:
            # Relative to end
            self.offset = self.length - offset

    def readline(self):
        chunks = []
        chunk = self.read(1024)
        while chunk and "\n" not in chunk:
            chunks.append(chunk)
            chunk = self.read(1024)
        if "\n" in chunk:
            chunk_len = len(chunk)
            index = chunk.index("\n")
            chunk = chunk[:index+1]
            self.seek((chunk_len-index-1) * -1, 1)
        chunks.append(chunk)
        return "".join(chunks)

    def __iter__(self):
        line = self.readline()
        while line:
            yield line
            line = self.readline()

    @property
    def closed(self):
        return self.offset >= self.length

    @property
    def fp(self):
        return self

    def _safe_read(self, size=-1):
        return self.read(size)

    def close(self):
        self.offset = self.length
