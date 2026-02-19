import argparse
import asyncio
import os
import aiohttp
import aiofiles


class ZipDataFetcher:
    def __init__(self, url_file: str, max_concurrency: int = 10):
        self.url_file = url_file
        self.base_url = "https://data.binance.vision/"
        self.session = aiohttp.ClientSession()
        self.retry_limit = 3
        self.failed_file = f"{url_file}.failed"
        self.sem = asyncio.Semaphore(max_concurrency)

    async def process_one_url(self, url: str):
        if self.has_downloaded(url):
            return
        async with self.sem:
            await self.download_file(url)

    def get_out_path(self, url: str) -> str:
        return url.replace("https://", "")

    def has_downloaded(self, url: str):
        file_path = self.get_out_path(url)
        done_file = f"{file_path}.done"
        return os.path.exists(file_path) or os.path.exists(done_file)

    async def fetch_content(self, url: str):
        for attempt in range(self.retry_limit):
            try:
                async with self.session.get(url) as response:
                    response.raise_for_status()
                    return await response.read()
            except Exception as e:
                print(f"Attempt {attempt + 1} failed for {url}: {e}")
                await asyncio.sleep(2**attempt)
        await self.log_failed(url)
        print(f"Failed to fetch {url} after {self.retry_limit} attempts")

    async def log_failed(self, url: str):
        async with aiofiles.open(self.failed_file, "a", encoding="utf-8") as f:
            await f.write(f"{url}\n")

    async def download_file(self, url: str):
        content = await self.fetch_content(url)
        if content is None:
            return
        file_path = self.get_out_path(url)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        async with aiofiles.open(file_path, "wb") as f:
            await f.write(content)

    async def run(self):
        async with aiofiles.open(self.url_file, "r", encoding="utf-8") as f:
            urls = [line.strip() for line in await f.readlines() if line.strip()]
        print(f"Total URLs to process: {len(urls)}")
        tasks = [self.process_one_url(url) for url in urls]
        await asyncio.gather(*tasks)
        await self.session.close()
        if not os.path.exists(self.failed_file):
            os.remove(self.url_file)
            print(
                f"All files downloaded successfully. url file {self.url_file} removed."
            )


async def main(args):
    fetcher = ZipDataFetcher(args.url_file, max_concurrency=args.max_concurrency)
    await fetcher.run()
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url-file", default="test_url.txt")
    parser.add_argument("--max-concurrency", type=int, default=10)
    args = parser.parse_args()
    print(
        f"Fetching URLs from file: {args.url_file}, max concurrency: {args.max_concurrency}"
    )
    asyncio.run(main(args))
