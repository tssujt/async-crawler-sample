import asyncio
import logging

import aiohttp
import async_timeout
import uvloop
from pyquery import PyQuery as pq

loop = uvloop.new_event_loop()
asyncio.set_event_loop(loop)

logger = logging.getLogger('Crawler')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

ROOT_URL = 'http://readfree.me?page=1'

HEADERS = {
    'Cookie': 'csrftoken=16tJeMh8yI5AZcLr7LnJcpq7S1UPwaUmSzjiBCqm68epqcApkRbUqRKyqC3p26cn; '
              'sessionid=htt1dqolvkuz6fi464xwayogqsocygxa',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:57.0) Gecko/20100101 '
                  'Firefox/57.0',
    'Host': 'readfree.me',
    'Referer': 'http://readfree.me',
}


class ReadfreeCrawler:
    def __init__(self, loop=None, max_tasks=100):
        self.loop = loop
        self.headers = HEADERS
        self.logger = logger
        self.max_tasks = max_tasks

        self.conn = aiohttp.TCPConnector(verify_ssl=False)
        self.session = aiohttp.ClientSession(loop=loop,
                                             connector=self.conn,
                                             headers=self.headers)
        self.queue = asyncio.Queue()

    async def _extract(self, url):
        self.logger.debug('Start extracting URL: {}'.format(url))
        async with self.session.get(url) as resp:
            content = await resp.text()
        await asyncio.sleep(4)
        self.logger.debug('Finish extracting URL: {}'.format(url))

        urls = []
        if url.startswith('http://readfree.me?page='):
            urls.append(
                'http://readfree.me?page={}'.format(int(url.replace('http://readfree.me?page=', '')) + 1))
        num = 0
        for node in pq(content).items('.book-item'):
            urls.append('http://readfree.me{}'.format(node('.pjax').attr.href))

        return urls

    async def _worker(self):
        while True:
            _url = await self.queue.get()

            new_urls = await self._extract(_url)
            for _u in new_urls:
                self.queue.put_nowait(_u)

            self.queue.task_done()

    def run(self):
        self.queue.put_nowait(ROOT_URL)

        async def _runner():
            workers = [asyncio.Task(self._worker(), loop=self.loop)
                       for _ in range(self.max_tasks)]
            await self.queue.join()
            for _w in workers:
                _w.cancel()

        self.loop.run_until_complete(_runner())

    def close(self):
        self.session.close()


if __name__ == '__main__':
    crawler = ReadfreeCrawler(loop=loop)
    crawler.run()
