import requests
import zipfile
import os
import re
from datetime import datetime
from dateutil.relativedelta import relativedelta
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from rich.progress import Progress, TaskID
from rich.console import Console
from typing import List, Optional
from natsort import natsorted

def download_binance_data(
    asset_type: str,
    time_period: str,
    data_type: str,
    data_frequency: str,
    destination_dir: str = "./",
    max_workers: int = 100,
    symbol_suffix: Optional[List[str]] = None,
    batch_size: int = 20,
    max_extract_workers: int = 5,
    retries: int = 3,
    batch_number: int = 1,
    total_batches: int = 3
):
    """
    Downloads and extracts Binance data with parallel downloading and extraction.
    """
    # Validate parameters
    valid_asset_types = ["spot", "um", "cm"]
    if asset_type not in valid_asset_types:
        raise ValueError(f"Invalid asset_type: {asset_type}. Must be one of {valid_asset_types}")

    valid_time_periods = ["daily", "monthly"]
    if time_period not in valid_time_periods:
        raise ValueError(f"Invalid time_period: {time_period}. Must be one of {valid_time_periods}")

    s3_base_url = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
    download_base_url = "https://data.binance.vision"
    console = Console()

    def get_all_symbols(asset_type: str, symbol_suffix: Optional[List[str]] = None) -> List[str]:
        """Get all symbols for the given asset type with optional suffix filtering."""
        console.print(f"[bold blue]Fetching symbols for {asset_type}...[/]")

        if asset_type == "spot":
            prefix = f"data/spot/{time_period}/{data_type}/"
        else:
            prefix = f"data/futures/{asset_type}/{time_period}/{data_type}/"

        delimiter = "/"
        marker = None
        all_symbols = []

        while True:
            params = {"prefix": prefix, "delimiter": delimiter}
            if marker:
                params["marker"] = marker

            try:
                response = requests.get(s3_base_url, params=params)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                console.print(f"[bold red]Error fetching symbol list: {e}[/]")
                return []

            try:
                from xml.etree import ElementTree
                tree = ElementTree.fromstring(response.content)
            except Exception as e:
                console.print(f"[bold red]Error parsing XML: {e}[/]")
                return []

            namespace = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
            common_prefixes = tree.findall(".//s3:CommonPrefixes/s3:Prefix", namespaces=namespace)
            if not common_prefixes:
                common_prefixes = tree.findall(".//CommonPrefixes/Prefix")

            for common_prefix in common_prefixes:
                symbol_path = common_prefix.text
                if asset_type == "spot":
                    symbol = symbol_path.replace(prefix, "").strip("/")
                else:
                    symbol = symbol_path.replace(prefix, "").split('/')[0]
                if symbol and symbol not in all_symbols:
                    all_symbols.append(symbol)

            marker_element = tree.find(".//s3:NextMarker", namespaces=namespace)
            if marker_element is None:
                marker_element = tree.find(".//NextMarker")

            if marker_element is not None and marker_element.text:
                marker = marker_element.text
            else:
                break

        # Filter unwanted symbols
        exclude_patterns = ["UPUSDT", "DOWNUSDT", "BEARUSDT", "BULLUSDT"]
        all_symbols = [s for s in all_symbols if not any(pattern in s for pattern in exclude_patterns)]

        if symbol_suffix:
            filtered_symbols = []
            for symbol in all_symbols:
                for suffix in symbol_suffix:
                    if symbol.endswith(suffix):
                        filtered_symbols.append(symbol)
                        break
            all_symbols = filtered_symbols

        return natsorted(all_symbols)

    def _fetch_urls_for_prefix(prefix: str) -> List[str]:
        """Fetch download URLs for a single prefix with retries."""
        download_urls = []
        marker = None
        while True:
            params = {"prefix": prefix, "max-keys": 1000}
            if marker:
                params["marker"] = marker

            for attempt in range(retries + 1):
                try:
                    response = requests.get(s3_base_url, params=params)
                    response.raise_for_status()
                    break
                except requests.exceptions.RequestException as e:
                    if attempt < retries:
                        continue
                    else:
                        console.print(f"[bold red]Error fetching URLs for {prefix}: {e}[/]")
                        return download_urls

            try:
                from xml.etree import ElementTree
                tree = ElementTree.fromstring(response.content)
            except Exception as e:
                console.print(f"[bold red]Error parsing XML for {prefix}: {e}[/]")
                return download_urls

            namespace = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
            contents = tree.findall(".//s3:Contents", namespaces=namespace)
            if not contents:
                contents = tree.findall(".//Contents")

            for content in contents:
                key_element = content.find("./s3:Key", namespaces=namespace)
                if key_element is None:
                    key_element = content.find("./Key")
                if key_element is not None and key_element.text.endswith(".zip"):
                    download_urls.append(f"{download_base_url}/{key_element.text}")

            marker_element = tree.find(".//s3:NextMarker", namespaces=namespace)
            if marker_element is None:
                marker_element = tree.find(".//NextMarker")

            if marker_element is not None and marker_element.text:
                marker = marker_element.text
            else:
                break

        return download_urls

    def get_download_urls_batched(symbols: List[str]) -> List[str]:
        """Fetch download URLs in batches."""
        console.print(f"[blue]Fetching URLs for {len(symbols)} symbols...[/]")
        download_urls = []

        if asset_type == "spot":
            base_prefix = f"data/spot/{time_period}/{data_type}/"
        else:
            base_prefix = f"data/futures/{asset_type}/{time_period}/{data_type}/"

        with Progress() as progress:
            task = progress.add_task("[cyan]Fetching URLs...", total=len(symbols))
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(_fetch_urls_for_prefix, f"{base_prefix}{symbol}/{data_frequency}/")
                          for symbol in symbols]

                for future in as_completed(futures):
                    download_urls.extend(future.result())
                    progress.advance(task)

        return download_urls

    def extract_file(zip_content: bytes, dest_path: str) -> int:
        """Extract CSV files from zip content."""
        extracted_count = 0
        try:
            with zipfile.ZipFile(BytesIO(zip_content)) as zip_file:
                for member in zip_file.namelist():
                    filename = os.path.basename(member)
                    if not filename.endswith(".csv"):
                        continue

                    extracted_path = os.path.join(dest_path, filename)
                    if not os.path.exists(extracted_path):
                        with zip_file.open(member) as source, open(extracted_path, "wb") as target:
                            target.write(source.read())
                        extracted_count += 1
        except Exception as e:
            console.print(f"[bold red]Error extracting: {e}[/]")
        return extracted_count

    def download_file(url: str, dest_path: str, extract_executor: ThreadPoolExecutor, extraction_progress: TaskID):
        """Download and submit for extraction."""
        for attempt in range(retries + 1):
            try:
                response = requests.get(url)
                response.raise_for_status()

                # Extract symbol from URL
                parts = url.split('/')
                if asset_type == "spot":
                    symbol = parts[7]
                else:
                    symbol = parts[8]

                final_path = os.path.join(destination_dir, asset_type, symbol, data_frequency)
                os.makedirs(final_path, exist_ok=True)

                extract_executor.submit(extract_file, response.content, final_path).add_done_callback(
                    lambda _: progress.advance(extraction_progress)
                )
                break
            except requests.exceptions.RequestException as e:
                if attempt == retries:
                    console.print(f"[bold red]Failed to download {url}: {e}[/]")

    def verify_url_completeness(download_urls: List[str]):
        """Verify all CSV files exist."""
        console.print("\n[bold blue]Checking CSV completeness...[/]")
        missing = 0

        for url in download_urls:
            parts = url.split('/')
            csv_name = parts[-1].replace('.zip', '.csv')
            symbol = parts[7] if asset_type == "spot" else parts[8]
            csv_path = os.path.join(destination_dir, asset_type, symbol, data_frequency, csv_name)

            if not os.path.exists(csv_path):
                missing += 1
                console.print(f"[red]Missing {csv_name}[/]")

        console.print(f"[green]{len(download_urls)-missing}/{len(download_urls)} files verified[/]")
        if missing > 0:
            console.print(f"[bold red]{missing} files missing[/]")

    def verify_download_completeness(symbols: List[str]):
        """Check date continuity in downloaded CSVs."""
        console.print("\n[bold blue]Verifying date continuity...[/]")
        date_format = "%Y-%m-%d" if time_period == "daily" else "%Y-%m"
        pattern = re.compile(r'(\d{4}-\d{2}(?:-\d{2})?)\.csv$')

        for symbol in symbols:
            symbol_dir = os.path.join(destination_dir, asset_type, symbol, data_frequency)
            if not os.path.exists(symbol_dir):
                console.print(f"[red]Missing directory for {symbol}[/]")
                continue

            csv_files = [f for f in os.listdir(symbol_dir) if f.endswith(".csv")]
            dates = []
            for f in csv_files:
                match = pattern.search(f)
                if match:
                    try:
                        dates.append(datetime.strptime(match.group(1), date_format))
                    except ValueError:
                        continue

            if not dates:
                console.print(f"[yellow]No valid dates for {symbol}[/]")
                continue

            dates.sort()
            min_date, max_date = dates[0], dates[-1]
            expected = []
            current = min_date
            delta = relativedelta(days=1) if time_period == "daily" else relativedelta(months=1)

            while current <= max_date:
                expected.append(current)
                current += delta

            missing = [d.strftime(date_format) for d in expected if d not in dates]
            if missing:
                console.print(f"\n[bold red]{symbol} missing {len(missing)} dates[/]")
                console.print(f"First: {min_date.strftime(date_format)}")
                console.print(f"Last: {max_date.strftime(date_format)}")
                console.print("Last 5 missing:")
                for d in missing[-5:]:
                    console.print(f"  {d}")
            else:
                console.print(f"[green]{symbol}: Complete ({len(dates)} files)[/]")

    # Main execution
    try:
        os.makedirs(destination_dir, exist_ok=True)
    except Exception as e:
        console.print(f"[bold red]Directory error: {e}[/]")
        return

    symbols = get_all_symbols(asset_type, symbol_suffix)
    if not symbols:
        console.print("[bold red]No symbols found[/]")
        return

    # Split into batches
    symbols = natsorted(symbols)
    batch_size_total = len(symbols) // total_batches
    remainder = len(symbols) % total_batches
    batches = []
    start = 0

    for i in range(total_batches):
        end = start + batch_size_total + (1 if i < remainder else 0)
        batches.append(symbols[start:end])
        start = end

    current_batch = batches[batch_number-1]
    console.print(f"\n[bold green]Processing batch {batch_number}/{total_batches} ({len(current_batch)} symbols)[/]")

    # Get URLs and download
    download_urls = get_download_urls_batched(current_batch)

    with Progress() as progress:
        dl_task = progress.add_task("[cyan]Downloading...", total=len(download_urls))
        ex_task = progress.add_task("[green]Extracting...", total=len(download_urls))

        with ThreadPoolExecutor(max_workers=max_workers) as dl_executor, \
             ThreadPoolExecutor(max_workers=max_extract_workers) as ex_executor:

            futures = []
            for url in download_urls:
                futures.append(dl_executor.submit(
                    download_file, url, destination_dir, ex_executor, ex_task
                ))

            for _ in as_completed(futures):
                progress.advance(dl_task)

    # Run verification
    verify_url_completeness(download_urls)
    verify_download_completeness(current_batch)
    console.print("[bold green]\nProcess completed[/]")

if __name__ == "__main__":
    download_binance_data(
        asset_type="spot",
        time_period="monthly",
        data_type="klines",
        data_frequency="1h",
        destination_dir="./binance_data",
        symbol_suffix=["USDT"],
        batch_number=1,
        total_batches=3,
        max_workers=50,
        max_extract_workers=10
    )