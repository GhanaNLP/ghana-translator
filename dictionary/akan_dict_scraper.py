import requests
from bs4 import BeautifulSoup
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import os


class AkanDict:
    """Integrated Scraper for the Akan Dictionary."""

    BASE_URL = "https://www.akandictionary.com"
    # Using the specific sitemaps found in your sitemap.xml
    SITEMAPS = [
        f"{BASE_URL}/post-sitemap1.xml",
        f"{BASE_URL}/post-sitemap2.xml",
        f"{BASE_URL}/post-sitemap3.xml",
    ]

    def __init__(self, headers):
        self.headers = headers
        self.session = requests.Session()
        self.session.headers.update(headers)
        self.failed_urls = []

    def get_content(self, url):
        """Fetches URL content safely."""
        try:
            response = self.session.get(url, timeout=15)
            if response.status_code == 200:
                return response.content
            return None
        except Exception:
            return None

    def get_all_word_urls(self):
        """Extracts word URLs using the lxml parser."""
        all_links = []
        for sitemap_url in self.SITEMAPS:
            print(f"Processing sitemap: {sitemap_url}...")
            content = self.get_content(sitemap_url)
            if not content:
                continue

            # Using 'xml' with the lxml builder you successfully installed
            soup = BeautifulSoup(content, "xml")
            links = [loc.get_text() for loc in soup.find_all("loc")]
            all_links.extend(links)

        # Filter for actual word links
        word_links = [l for l in all_links if "akandictionary.com" in l and len(l) > 35]
        print(f"Total unique links found: {len(list(set(word_links)))}")
        return list(set(word_links))

    def _parse_word_page(self, word_url):
        """Extracts Twi, English, and Part of Speech."""
        html = self.get_content(word_url)
        if not html:
            return None

        soup = BeautifulSoup(html, "html.parser")
        # Fixed dictionary keys to match sorting logic
        data = {
            "english_word": "",
            "twi_word": "",
            "part_of_speech": "",
            "url": word_url,
        }

        try:
            # Get Twi word from title
            title = soup.find("h1", class_="wp-block-post-title")
            if title:
                data["twi_word"] = title.get_text(strip=True)

            # Get translation and POS from entry content
            content = soup.find("div", class_="entry-content")
            if content:
                for p in content.find_all("p"):
                    text = p.get_text(strip=True)
                    text_low = text.lower()

                    if "part of speech:" in text_low:
                        data["part_of_speech"] = text.split(":", 1)[1].strip()
                    elif "english" in text_low:
                        delimiter = ":" if ":" in text else "-"
                        if delimiter in text:
                            data["english_word"] = text.split(delimiter, 1)[1].strip()
            return data
        except Exception:
            return None

    def make_dictionary(self):
        """Orchestrates scraping and fixed CSV saving."""
        urls = self.get_all_word_urls()
        if not urls:
            return

        print(f"Scraping {len(urls)} word pages...")
        with ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(self._parse_word_page, urls))

        # Filter out empty entries
        final_data = [r for r in results if r and (r["twi_word"] or r["english_word"])]

        df = pd.DataFrame(final_data)
        if not df.empty:
            # FIXED: Column name must match the sorting key exactly to avoid KeyError
            df = df.sort_values("english_word").reset_index(drop=True)
            desktop_path = os.path.join(
                os.path.join(os.environ["USERPROFILE"]), "Desktop"
            )
            file_name = "eng_akan_dict_final.csv"
            full_path = os.path.join(desktop_path, file_name)
            df.to_csv(full_path, index=False)
            print(f"Success! Saved {len(df)} words to {full_path}")


if __name__ == "__main__":
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0"
    }
    scraper = AkanDict(headers)
    scraper.make_dictionary()
