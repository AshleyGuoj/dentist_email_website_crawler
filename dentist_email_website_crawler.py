# Install required packages
# !pip install requests beautifulsoup4 tldextract

import pandas as pd
import requests
from bs4 import BeautifulSoup
import re, time, json
from datetime import datetime
import tldextract

# === Step 1: Load and Trim Data ===
df = pd.read_excel("Dentist_Locations_Filled.xlsx")
df_cleaned = df.iloc[180000:]
df_cleaned.to_csv("dentist_locations_cleaned.csv", index=False)
print(f"✅ Saved {len(df_cleaned)} rows to 'dentist_locations_cleaned.csv'")

# === Step 2: Email Extractor Class ===
class DentistEmailExtractor:
    def __init__(self, api_key, delay=3):
        self.api_key = api_key
        self.session = requests.Session()
        self.delay = delay

    def guess_website(self, company, city, state):
        query = f"{company or 'Dental'} {city} {state} dental site"
        try:
            res = requests.post(
                "https://google.serper.dev/search",
                headers={"X-API-KEY": self.api_key, "Content-Type": "application/json"},
                json={"q": query}
            )
            results = res.json().get("organic", [])
            for r in results:
                link = r.get("link")
                domain = tldextract.extract(link).domain
                if domain not in {"yelp", "facebook", "zocdoc", "healthgrades", "linkedin", "mapquest", "opencorporates", "bbb", "dnb"}:
                    print(f"🌐 Found website: {link}")
                    return link
        except Exception as e:
            print(f"❌ Website guess failed: {e}")
        return None

    def extract_email_from_site(self, url):
        if not url:
            return None
        html = self.fetch_page(url)
        if html:
            emails = self.find_emails(html)
            if emails:
                return emails[0]
        for page in ["contact", "contact-us", "about", "about-us"]:
            sub_url = f"{url.rstrip('/')}/{page}"
            html = self.fetch_page(sub_url)
            if html:
                emails = self.find_emails(html)
                if emails:
                    return emails[0]
        return None

    def fetch_page(self, url):
        try:
            res = self.session.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
            res.raise_for_status()
            return res.text
        except:
            return None

    def find_emails(self, html):
        soup = BeautifulSoup(html, "html.parser")
        mailtos = [a['href'].split(":")[1].split("?")[0] for a in soup.find_all('a', href=True) if 'mailto:' in a['href']]
        text_emails = re.findall(r"\b[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+\b", html)
        emails = set(mailtos + text_emails)
        return [e for e in emails if all(x not in e.lower() for x in ['.png', '.jpg', '.jpeg', '.webp', '@sentry']) and len(e) > 5 and not re.search(r'@[0-9.]', e)]

    def process(self, file, start=0, end=100):
        df = pd.read_csv(file).iloc[start:end].copy()
        results = []
        for i, row in df.iterrows():
            print(f"\n🔍 {i}: {row['Company']}")
            website = self.guess_website(row['Company'], row['City'], row['States'])
            email = self.extract_email_from_site(website)
            print(f"📧 Email: {email if email else 'Not found'}")

            results.append({
                'Company': row['Company'],
                'Phone': row.get('Phone', ''),
                'Email': email,
                'Website': website,
                'Address': f"{row.get('Address', '')}, {row['City']}, {row['States']}"
            })

            time.sleep(self.delay)
            if (i - start + 1) % 50 == 0:
                self.save(results, start, end)
        self.save(results, start, end)
        return results

    def save(self, results, start, end):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        df = pd.DataFrame(results)
        name = f"results_{start}_{end}_{timestamp}"
        df.to_csv(f"{name}.csv", index=False)
        df.to_excel(f"{name}.xlsx", index=False)
        json.dump({
            'processed': len(results),
            'emails_found': sum(bool(r['Email']) for r in results),
            'websites_found': sum(bool(r['Website']) for r in results),
            'timestamp': timestamp
        }, open(f"{name}_stats.json", "w"), indent=2)
        print(f"💾 Saved to: {name}.csv/.xlsx/.json")

# === Step 3: Run Extractor ===
if __name__ == "__main__":
    extractor = DentistEmailExtractor(api_key="YOUR_API_KEY_HERE")
    extractor.process("dentist_locations_cleaned.csv", start=0, end=100)
