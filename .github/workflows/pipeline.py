"""Lis Pendens daily pipeline (single file). Edit COUNTIES below, then run."""
import asyncio, datetime, json, os, re, smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pandas as pd
from anthropic import Anthropic
from playwright.async_api import async_playwright

# ---- EDIT THIS: one entry per county --------------------------------------
COUNTIES = {
    "example_county": {
        "url": "https://REPLACE-WITH-COUNTY-RECORDS-SEARCH-URL",
        "selectors": {
            "doc_type_dropdown": "select#DocType",
            "doc_type_value": "LIS PENDENS",
            "start_date": "input#StartDate",
            "end_date": "input#EndDate",
            "search_btn": "button#Search",
            "table_rows": "table#results tbody tr"
        }
    }
}
# ---------------------------------------------------------------------------
SUBS = json.loads(os.environ.get("COUNTY_SUBSCRIBERS", "{}"))
for _cid, _c in COUNTIES.items():
    _c["subscribers"] = SUBS.get(_cid, [])

# Filings are usually indexed with a delay, so by default pull yesterday's
# through today's records rather than "today" only (empty at 6:30 AM).
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "1"))
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")


async def _scrape_once(target_url, selectors):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            context = await browser.new_context(
                user_agent=UA, viewport={"width": 1920, "height": 1080})
            page = await context.new_page()
            await page.goto(target_url, wait_until="networkidle", timeout=60000)

            await page.select_option(selectors["doc_type_dropdown"],
                                     value=selectors["doc_type_value"])
            end = datetime.date.today()
            start = end - datetime.timedelta(days=LOOKBACK_DAYS)
            await page.fill(selectors["start_date"], start.strftime("%m/%d/%Y"))
            await page.fill(selectors["end_date"], end.strftime("%m/%d/%Y"))
            await page.click(selectors["search_btn"])
            await page.wait_for_load_state("networkidle")

            records = []
            for row in await page.locator(selectors["table_rows"]).all():
                cells = await row.locator("td").all_inner_texts()
                if len(cells) >= 3:
                    records.append({
                        "case_number": cells[0].strip(),
                        "filing_date": cells[1].strip(),
                        "raw_legal_text": cells[2].strip(),
                    })
            return records
        finally:
            await browser.close()


async def run_court_scraper(target_url, selectors, retries=3):
    for attempt in range(1, retries + 1):
        try:
            print(f"[scraper] {target_url} (attempt {attempt})")
            return await _scrape_once(target_url, selectors)
        except Exception as e:
            print(f"[scraper] error: {e}")
            await asyncio.sleep(5 * attempt)
    return []

client = Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))
MODEL = os.environ.get("CLAUDE_MODEL", "claude-sonnet-5-5")

PROMPT = """You are an expert real estate data parser. Extract details from this
court record text into valid JSON:

\"\"\"{raw_text}\"\"\"

Output ONLY a JSON object with exactly these keys:
{{
  "plaintiff": "Bank, entity, or spouse filing the suit",
  "defendant_owner": "Name of property owner being sued",
  "clean_property_address": "Full street address, City, State, Zip if clear",
  "suit_type": "Foreclosure, Divorce, Partition, Mechanics Lien, or Unknown",
  "estimated_urgency": "High, Medium, or Low"
}}
Use null for anything not stated. Do not invent data."""


def parse_legal_text_with_claude(raw_text):
    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=1000,
            temperature=0.0,
            messages=[{"role": "user", "content": PROMPT.format(raw_text=raw_text)}],
        )
        text = response.content[0].text.strip()
        text = re.sub(r"^```(?:json)?|```$", "", text, flags=re.M).strip()
        return json.loads(text)
    except Exception as e:
        print(f"[parser] failed: {e}")
        return None

def email_leads_to_partners(file_path, recipient_emails):
    sender = os.environ.get("SENDER_EMAIL")
    password = os.environ.get("SENDER_SMTP_PASSWORD")  # Gmail: use an App Password
    if not sender or not password:
        print("[email] SENDER_EMAIL / SENDER_SMTP_PASSWORD not set; skipping")
        return

    host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
    port = int(os.environ.get("SMTP_PORT", "587"))
    name = os.path.basename(file_path)

    with smtplib.SMTP(host, port) as server:
        server.starttls()
        server.login(sender, password)
        # One message per subscriber so nobody sees other subscribers' addresses.
        for rcpt in recipient_emails:
            msg = MIMEMultipart()
            msg["From"] = sender
            msg["To"] = rcpt
            msg["Subject"] = f"Daily Lis Pendens Leads - {name}"
            msg.attach(MIMEText(
                "Your clean, AI-enriched daily Lis Pendens leads sheet is attached.\n"
                "Verify records against the official court filing before acting.", "plain"))
            with open(file_path, "rb") as f:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f'attachment; filename="{name}"')
            msg.attach(part)
            server.sendmail(sender, [rcpt], msg.as_string())
            print(f"[email] sent to {rcpt}")

PARSE_CONCURRENCY = 5


async def process_county(county_id, config):
    today = datetime.date.today().strftime("%Y-%m-%d")
    raw_leads = await run_court_scraper(config["url"], config["selectors"])
    if not raw_leads:
        print(f"[{county_id}] no records")
        return

    sem = asyncio.Semaphore(PARSE_CONCURRENCY)

    async def parse(lead):
        async with sem:
            return await asyncio.to_thread(
                parse_legal_text_with_claude, lead["raw_legal_text"])

    parsed = await asyncio.gather(*(parse(l) for l in raw_leads))

    rows, seen = [], set()
    for lead, ai in zip(raw_leads, parsed):
        if not ai or lead["case_number"] in seen:
            continue
        seen.add(lead["case_number"])
        rows.append({
            "Case Number": lead["case_number"],
            "Filing Date": lead["filing_date"],
            "Property Owner / Defendant": ai.get("defendant_owner"),
            "Clean Property Address": ai.get("clean_property_address"),
            "Lawsuit Type": ai.get("suit_type"),
            "Urgency Level": ai.get("estimated_urgency"),
            "Plaintiff": ai.get("plaintiff"),
        })

    if not rows:
        print(f"[{county_id}] nothing parsed")
        return

    filename = f"{county_id}_leads_{today}.xlsx"
    pd.DataFrame(rows).to_excel(filename, index=False)
    print(f"[{county_id}] wrote {len(rows)} leads -> {filename}")

    if config["subscribers"]:
        email_leads_to_partners(filename, config["subscribers"])


async def main():
    await asyncio.gather(*(process_county(c, cfg)
                           for c, cfg in COUNTY_CONFIGS.items()))


if __name__ == "__main__":
    asyncio.run(main())
