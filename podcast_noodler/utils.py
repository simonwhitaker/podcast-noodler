import os

import aiohttp


async def download_file(
    session: aiohttp.ClientSession, url: str, local_path: str
) -> None:
    async with session.get(url) as resp:
        if resp.status == 200:
            # Check to see if we already have a download of the same size. Skip
            # this download if we do.
            try:
                stat = os.stat(local_path)
                file_size = stat.st_size
                download_size = int(resp.headers["Content-Length"])
                if file_size == download_size:
                    print(f"Skipping {url}, already downloaded at {local_path}")
                    resp.close()
                    return
            except FileNotFoundError:
                pass
            print(f"Downloading {url} to {local_path}")
            with open(local_path, "wb") as f:
                async for chunk in resp.content.iter_chunked(1024):
                    f.write(chunk)
        else:
            print(f"[{resp.status}] {url}")
