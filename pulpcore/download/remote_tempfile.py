import os
import logging

import asyncio
import aiofiles

from .base import BaseDownloader, DownloadResult
from pulpcore.app.models import RemoteDownload


log = logging.getLogger(__name__)


class TempFileDownloader(BaseDownloader):
    """
    A downloader for hooking onto a download in progress

    This downloader has all of the attributes of
    [pulpcore.plugin.download.BaseDownloader][]
    """

    def __init__(self, url, *args, **kwargs):
        """
        Download files from a url that starts with `tmp://`

        Args:
            url (str): The url to the file. This is expected to begin with `tmp://`
            kwargs (dict): This accepts the parameters of
                [pulpcore.plugin.download.BaseDownloader][].

        Raises:
            AssertionError: When expected_size is 0
        """
        super().__init__(url, *args, **kwargs)
        assert self.expected_size > 0
        self.download_id = url[6:]

    async def _run(self, extra_data=None):
        """
        Read, validate, and compute digests on the tempfile. This is a coroutine.

        This method provides the same return object type and documented in
        :meth:`~pulpcore.plugin.download.BaseDownloader._run`.

        Args:
            extra_data (dict): Extra data passed to the downloader.
        """

        timeout = 30
        try:
            async with asyncio.timeout(timeout) as to_cm:
                while True:
                    res = await RemoteDownload.objects.aget(download_id=self.download_id)
                    if res.temp_path and os.path.exists(res.temp_path):
                        self._path = res.temp_path
                        break
                    await asyncio.sleep(1)
                to_cm.reschedule(asyncio.get_running_loop().time()+timeout)
                async with aiofiles.open(self._path, "rb") as f_handle:
                    while True:
                        chunk = await f_handle.read(1048576)  # 1 megabyte
                        if not chunk:
                            log.warning(f"tmpfile_dingens read {self._size}/{self.expected_size} bytes")
                            if self._size >= self.expected_size:
                                await self.finalize()
                                break
                            await asyncio.sleep(0.5)
                            continue
                        to_cm.reschedule(asyncio.get_running_loop().time()+timeout)
                        await self.handle_data(chunk)
        except TimeoutError:
            log.warning(f"{self._path} did not receive data for {timeout} seconds, cancelled.")
            return None

        return DownloadResult(
            path=self.path,
            artifact_attributes=self.artifact_attributes,
            url=self.url,
            headers=None,
        )

    async def handle_data(self, data):
        """
        A coroutine for streaming the content and calculating size and digest

        All subclassed downloaders are expected to pass all data downloaded to this method. Similar
        to the hashlib docstring, repeated calls are equivalent to a single call with
        the concatenation of all the arguments: m.handle_data(a); m.handle_data(b) is equivalent to
        m.handle_data(a+b).

        Args:
            data (bytes): The data to be handled by the downloader.
        """
        self._record_size_and_digests_for_data(data)
