import pandas as pd
import aiohttp
import asyncio
import aiofiles
import re
import time

def sanitize_filename(filename):
    # Erstatter ulovlige symboler i filnavne med en understreg.
    return re.sub(r'[<>:"/\\|?*]', '_', filename)

async def file_reader(session, link_1, link_2, br, title, error_queue, status_df):
    save_path = "Opgaver - Uge 5/pdf-filer/"
    download_status = "Failed"
    filename = sanitize_filename(title) + ".pdf"
    
    print(f"Starting download: {title}")

    def is_valid_link(link):
        # Checker om et link som minimum opfylder at være en string der ikke kun består af mellemrum.
        return isinstance(link, str) and link.strip() != ""

    async def try_download(link):
        # Prøver at downloade en fil givet et link. Den giver 10 sekunder til at oprette forbindelse og 5 minutter til at downloade filen. Hvis der sker nogle fejl bliver de gemt.
        if not is_valid_link(link):
            return None
        
        try:
            timeout = aiohttp.ClientTimeout(connect=10, sock_read=300)
            async with session.get(link, ssl=False, timeout=timeout) as response:
                if response.status == 200 and response.headers.get("Content-Type") == "application/pdf":
                    content = await response.read()
                    return content
                else:
                    await error_queue.put(f"Download failed for {link}: Status {response.status}, Content-Type {response.headers.get('Content-Type')}")
        except aiohttp.ClientError as e:
            await error_queue.put(f"Client error downloading from {link}: {e}")
        except asyncio.TimeoutError:
            await error_queue.put(f"Timeout error downloading from {link}")
        except Exception as e:
            await error_queue.put(f"Unexpected error downloading from {link}: {e}")
        
        return None

    # Prøver at downloade første link først, og hvis det ikke virker det andet. Til sidst gemmes hvorvidt om filen blev downloadet i en pandas dataframe.
    try:
        content = await try_download(link_1)
        if not content:
            content = await try_download(link_2)            
        if content:
            async with aiofiles.open(save_path + filename, 'wb') as f:
                await f.write(content)
            download_status = "Succeeded"
            
    except Exception as e:
        await error_queue.put(f"Unexpected error with {title}: {e}")

    finally:
        status_df.loc[status_df['BRnum'] == br, 'Download status'] = download_status
        print(f"Finished download: {title} - Status: {download_status}")

async def error_logger(error_queue):
    # Gemmer fejl i file_reader i "errors.txt"
    async with aiofiles.open("Opgaver - Uge 5/errors.txt", "a", encoding="utf-8") as error_file:
        while True:
            message = await error_queue.get()
            if message is None:
                break
            await error_file.write(message + "\n")
            error_queue.task_done()

path = "Opgaver - Uge 5/"
excel_file = "GRI_2017_2020.xlsx"
csv_file = "downloads.csv"

files = pd.read_excel(path + excel_file, header=0)
status_df = files[['BRnum']].copy()
status_df['Download status'] = ""

entries = [(files.iloc[i]["Pdf_URL"], files.iloc[i]["Report Html Address"], files.iloc[i]["BRnum"], files.iloc[i]["Title"]) for i in range(files.shape[0])]

a = time.time()

async def main():
    connector = aiohttp.TCPConnector(limit=20)
    error_queue = asyncio.Queue()
    error_logger_task = asyncio.create_task(error_logger(error_queue))

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [file_reader(session, *entry, error_queue, status_df) for entry in entries]
        await asyncio.gather(*tasks)

    await error_queue.put(None)
    await error_logger_task

    status_df.to_csv("Opgaver - Uge 5/" + csv_file, index=False, encoding="utf-8")

asyncio.run(main())

b = time.time()
print(b - a)
