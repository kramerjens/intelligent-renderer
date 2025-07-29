import os
import asyncio
import sys
import json
from datetime import datetime, timezone
import google.generativeai as genai
from playwright.async_api import async_playwright
from google.cloud import bigquery

# --- Konfiguration ---
PROJECT_ID = os.getenv('GCP_PROJECT')
BQ_DATASET = "job_monitor" # Ihr BigQuery-Dataset
COMPANIES_TABLE = f"{PROJECT_ID}.{BQ_DATASET}.companies"
SCRAPED_JOBS_TABLE = f"{PROJECT_ID}.{BQ_DATASET}.scraped_jobs"
REQUEST_TIMEOUT = 90000
MAX_JOB_COUNT_CHECKS = 3

# --- API & Client Initialisierung ---
# BigQuery Client
bq_client = bigquery.Client()

# Generative AI Client
api_key = os.getenv('GEMINI_API_KEY')
if not api_key:
    raise ValueError("GEMINI_API_KEY Umgebungsvariable nicht gesetzt.")
genai.configure(api_key=api_key)
model = genai.GenerativeModel("gemini-2.5-flash")

# --- BigQuery-Funktionen (NEU) ---
def fetch_next_company():
    """Holt die nächste Firma zum Scrapen."""
    print(f"Suche nach nächster Firma in Tabelle: {COMPANIES_TABLE}")
    # Diese Query holt eine Firma, die heute noch nicht gescannt wurde.
    # Sie können die Logik anpassen (z.B. die älteste zuerst).
    query = f"""
        SELECT company_id, company_name, career_page_url
        FROM `{COMPANIES_TABLE}`
        WHERE DATE(last_crawled_timestamp) IS NULL OR DATE(last_crawled_timestamp) < CURRENT_DATE()
        ORDER BY last_crawled_timestamp ASC
        LIMIT 1
    """
    query_job = bq_client.query(query)
    results = list(query_job) # In eine Liste umwandeln, um das Ergebnis zu bekommen
    
    if not results:
        print("Keine zu crawlenden Firmen gefunden.")
        return None
    
    company = results[0]
    print(f"Firma gefunden: {company.company_name} (ID: {company.company_id})")
    return company

def update_company_timestamp(company_id):
    """Aktualisiert den Zeitstempel für eine Firma."""
    query = f"""
        UPDATE `{COMPANIES_TABLE}`
        SET last_crawled_timestamp = CURRENT_TIMESTAMP()
        WHERE company_id = @company_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("company_id", "INTEGER", company_id),
        ]
    )
    bq_client.query(query, job_config=job_config).result()
    print(f"Zeitstempel für Firma {company_id} aktualisiert.")

def write_jobs_to_bigquery(jobs_to_insert):
    """Schreibt eine Liste von Jobs in die Zieltabelle."""
    if not jobs_to_insert:
        print("Keine Jobs zum Einfügen gefunden.")
        return

    errors = bq_client.insert_rows_json(SCRAPED_JOBS_TABLE, jobs_to_insert)
    if not errors:
        print(f"{len(jobs_to_insert)} neue Jobs erfolgreich in BigQuery geschrieben.")
    else:
        print("Fehler beim Einfügen der Jobs in BigQuery:")
        for error in errors:
            print(error)

# --- Web Scraper & Parser Funktionen ---
async def run_scraper(target_url):
    """Führt Playwright aus und gibt den finalen HTML-Inhalt zurück."""
    # Diese Funktion ist im Kern die gleiche wie vorher.
    # Sie fokussiert sich nur noch darauf, das HTML zu holen.
    print(f"Starte Scraper für URL: {target_url}")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        try:
            await page.goto(target_url, wait_until="networkidle", timeout=REQUEST_TIMEOUT)
            # ... Hier könnte Ihre erweiterte Klick-Logik für Cookies und "Mehr laden" stehen ...
            # Der Einfachheit halber lassen wir sie hier weg, Sie können sie bei Bedarf einfügen.
            
            print("Scraping abgeschlossen, extrahiere finalen Inhalt.")
            final_content = await page.content()
            await browser.close()
            return final_content
        except Exception as e:
            await browser.close()
            raise IOError(f"Fehler beim Scraping von {target_url}: {e}")

async def parse_html_with_ai(html_content, company_id, company_name):
    """Nutzt Gemini, um Jobs aus dem HTML zu extrahieren und für BigQuery vorzubereiten."""
    print("Starte HTML-Analyse mit Gemini, um Jobs zu extrahieren...")
    
    prompt = f"""
    Analysiere den folgenden HTML-Code einer Karriereseite. Extrahiere alle ausgeschriebenen Jobs.
    Gib das Ergebnis als JSON-Array zurück. Jedes Objekt im Array muss die folgenden Schlüssel haben: "job_titel", "job_url", "job_standort".
    - Der "job_standort" ist oft in der Nähe des Titels. Wenn kein Standort gefunden wird, setze den Wert auf "Unbekannt".
    - Die "job_url" ist die absolute URL zur Job-Detailseite.
    - Gib NUR das JSON-Array zurück, ohne zusätzlichen Text.

    HTML-Code:
    ```html
    {html_content[:20000]} 
    ```
    """ # Wir begrenzen das HTML, um Token-Limits nicht zu überschreiten
    
    try:
        response = await asyncio.to_thread(
            model.generate_content,
            prompt,
            generation_config={"response_mime_type": "application/json"}
        )
        extracted_jobs = json.loads(response.text)
        print(f"{len(extracted_jobs)} Jobs durch KI extrahiert.")
        
        # Bereite die Daten für BigQuery auf
        rows_to_insert = []
        now = datetime.now(timezone.utc).isoformat()
        for job in extracted_jobs:
            rows_to_insert.append({
                "company_id": company_id,
                "company_name": company_name,
                "job_titel": job.get("job_titel"),
                "job_standort": job.get("job_standort", "Unbekannt"),
                "job_url": job.get("job_url"),
                "first_seen_timestamp": now,
                "last_seen_timestamp": now
            })
        return rows_to_insert
    except Exception as e:
        print(f"Fehler bei der KI-Analyse: {e}")
        return []

# --- Hauptlogik ---
async def main():
    company = fetch_next_company()
    if not company:
        print("Prozess beendet.")
        return

    try:
        # 1. Scrapen
        html = await run_scraper(company.career_page_url)
        # 2. Parsen & Vorbereiten
        jobs_to_insert = await parse_html_with_ai(html, company.company_id, company.company_name)
        # 3. In BigQuery schreiben
        write_jobs_to_bigquery(jobs_to_insert)
        # 4. Zeitstempel der Firma aktualisieren
        update_company_timestamp(company.company_id)

    except Exception as e:
        print(f"Fehler im Hauptprozess für Firma {company.company_name}: {e}", file=sys.stderr)
        # Optional: Fehler in einer separaten Log-Tabelle speichern
        sys.exit(1)

# --- Einstiegspunkt des Skripts ---
if __name__ == "__main__":
    # Der Job wird jetzt ohne Argumente gestartet.
    asyncio.run(main())
