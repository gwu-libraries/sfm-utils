from __future__ import absolute_import
import __builtin__
import warc as ia_warc
from warc.utils import FilePart as ia_FilePart
from urllib3 import HTTPResponse
from urllib3._collections import HTTPHeaderDict
import gzip
from collections import namedtuple

"""
These classes extend Internet Archive's WARC library (https://github.com/internetarchive/warc)
to support record segmentation.
"""


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
                payload = CompositeFilePart(record.payload.fileobj, record.payload.length)
                save_payload = self._save_current_payload()
                # If start of a continuation, look for rest of records and build up a
                # ComposeFilePart.
                self._find_continuations(record, payload)

                # Read http start line and headers
                start_line, http_headers = self.read_http_start_line_and_headers(payload)
                if start_line.startswith("HTTP/1.1 200 OK"):

                    http_response = HTTPResponse(body=payload, headers=http_headers, status=200, version=1.1,
                                                 reason="OK", preload_content=False)
                    yield WARCResponseRecord(record.header, http_response)
                    # Setting this so _warc_reader can read to end of record.
                    self._restore_current_payload(save_payload)

    def _find_continuations(self, record, payload):
        # TODO: Also look in subsequent WARCs for continuations.
        if record.header.get("WARC-Segment-Number") == "1":
            segment = 1
            save_payload = self._save_current_payload()
            for peek_record in self._warc_reader:
                # If it's a continuation record and it is related to the original
                # response record.
                if peek_record.type == "continuation" and \
                        peek_record.header.get("WARC-Segment-Origin-ID") == record["WARC-Record-ID"]:
                    # Make sure this is the next segment.
                    segment += 1
                    assert peek_record.header.get("WARC-Segment-Number") == str(segment)
                    payload.add_file_section(peek_record.payload.fileobj, peek_record.payload.fileobj.tell(),
                                             peek_record.payload.length)
                    # Stop when hit last
                    if "WARC-Segment-Total-Length" in peek_record.header:
                        break
                self._warc_reader.finish_reading_current_record()
            self._restore_current_payload(save_payload)

    def _save_current_payload(self):
        save_payload = ia_FilePart(self._warc_reader.current_payload.fileobj,
                                   self._warc_reader.current_payload.length)
        save_payload.buf = self._warc_reader.current_payload.buf
        save_payload.offset = self._warc_reader.current_payload.offset
        save_payload.fileobj_position = self._warc_reader.current_payload.fileobj.tell()
        return save_payload

    def _restore_current_payload(self, save_payload):
        self._warc_reader.current_payload = save_payload
        self._warc_reader.current_payload.fileobj.seek(save_payload.fileobj_position)

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


FileSection = namedtuple("FileSection", ["fileobj", "start_position", "length", "start_offset", "end_offset"])


class CompositeFilePart:
    """
    File interface to multiple WARC record payloads.
    """
    def __init__(self, fileobj, length):
        self.file_sections = []
        self.file_sections.append(FileSection(fileobj, fileobj.tell(), length, 0, length-1))
        self.total_length = length
        self.offset = 0

    def add_file_section(self, fileobj, start_position, length):
        self.file_sections.append(FileSection(fileobj, start_position, length, self.total_length,
                                              self.total_length + length - 1))
        self.total_length += length

    def _file_section_offset(self, offset):
        if offset > self.total_length:
            offset = self.total_length
        for file_section in self.file_sections:
            if file_section.start_offset <= offset <= file_section.end_offset:
                return file_section, offset - file_section.start_offset + file_section.start_position
        return None, None

    def read(self, size=-1):
        if size == -1:
            size = self.total_length
        ending_offset = min(self.offset + size, self.total_length)
        buf = ""
        while self.offset != ending_offset:
            file_section, file_section_offset = self._file_section_offset(self.offset)
            assert file_section and file_section_offset
            remaining_size = file_section.length - file_section_offset + file_section.start_position
            read_size = min(size - len(buf), remaining_size)
            file_section.fileobj.seek(file_section_offset)
            buf += file_section.fileobj.read(read_size)
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
            self.offset = self.total_length - offset

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
        return self.offset >= self.total_length

    @property
    def fp(self):
        return self

    def _safe_read(self, size=-1):
        return self.read(size)

    def close(self):
        self.offset = self.total_length
