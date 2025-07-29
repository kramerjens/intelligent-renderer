import os
import asyncio
import sys
import json
import hashlib
from datetime import datetime, timezone
import google.generativeai as genai
from playwright.async_api import async_playwright
from google.cloud import bigquery

# --- Konfiguration ---
# PROJECT_ID wird jetzt direkt vom Service Account übernommen
PROJECT_ID = os.getenv('GCP_PROJECT') 
BQ_DATASET = "job_monitor"
COMPANIES_TABLE = f"{PROJECT_ID}.{BQ_DATASET}.companies"
SCRAPED_JOBS_TABLE = f"{PROJECT_ID}.{BQ_DATASET}.scraped_jobs"
REQUEST_TIMEOUT = 90000
MAX_JOB_COUNT_CHECKS = 2 # Anzahl der Versuche, bei denen sich die Jobanzahl nicht ändern darf

# --- API & Client Initialisierung ---
bq_client = bigquery.Client()
api_key = os.getenv('GEMINI_API_KEY')
if not api_key:
    raise ValueError("GEMINI_API_KEY Umgebungsvariable nicht gesetzt.")
genai.configure(api_key=api_key)
model = genai.GenerativeModel("gemini-1.5-flash")
print("--- Clients für BigQuery und Gemini AI initialisiert ---")

def update_company_timestamp(company_id):
    print(f"--> [BQ] Aktualisiere Zeitstempel für Firma {company_id}...")
    query = f"""
        UPDATE `{COMPANIES_TABLE}`
        SET last_crawled_timestamp = CURRENT_TIMESTAMP()
        WHERE company_id = @company_id
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("company_id", "INTEGER", company_id)])
    try:
        bq_client.query(query, job_config=job_config).result()
        print(f"--> [BQ] Zeitstempel für Firma {company_id} erfolgreich aktualisiert.")
    except Exception as e:
        print(f"!!! [BQ] FEHLER beim Aktualisieren des Zeitstempels für Firma {company_id}: {e}", file=sys.stderr)

def write_jobs_to_bigquery(jobs_to_insert):
    if not jobs_to_insert:
        print("--> [BQ] Keine neuen Jobs zum Einfügen in die Datenbank.")
        return
    print(f"--> [BQ] Versuche {len(jobs_to_insert)} Jobs in die Tabelle `{SCRAPED_JOBS_TABLE}` einzufügen...")
    try:
        errors = bq_client.insert_rows_json(SCRAPED_JOBS_TABLE, jobs_to_insert)
        if not errors:
            print(f"--> [BQ] {len(jobs_to_insert)} neue Jobs erfolgreich in BigQuery geschrieben.")
        else:
            print("!!! [BQ] FEHLER beim Einfügen der Jobs in BigQuery:", file=sys.stderr)
            for error in errors:
                print(error, file=sys.stderr)
    except Exception as e:
        print(f"!!! [BQ] Kritischer Fehler beim Einfügen in BigQuery: {e}", file=sys.stderr)


# --- Web Scraper & Parser Funktionen ---
async def find_and_click_element(page, purpose, unique_id_prefix):
    # Dies ist die re-integrierte, intelligente Klick-Funktion
    print(f"-> [SCRAPER] Suche nach '{purpose}'-Button...")
    
    # Hier können Sie bei Bedarf die deterministische Suche wieder einbauen, um API-Calls zu sparen.
    # Der Einfachheit halber nutzen wir hier direkt den robusten KI-Fallback.
    
    prompt_templates = {
        'cookie': "Finde den Button, der Cookies akzeptiert. Antworte NUR mit dem CSS-Selektor. Wenn nichts passt, antworte 'NONE'.",
        'load_more': "Finde den Button oder Link, der MEHR JOBS lädt oder zur NÄCHSTEN SEITE navigiert. Antworte NUR mit dem CSS-Selektor. Wenn nichts passt, antworte 'NONE'."
    }
    
    try:
        candidates = await page.query_selector_all('button, a, [role="button"]')
        simplified_elements = []
        for idx, element in enumerate(candidates):
            text = await element.inner_text()
            is_visible = await element.is_visible()
            if is_visible and len(text.strip()) > 1:
                unique_id = f"pw-{unique_id_prefix}-{idx}"
                await element.evaluate("(el, id) => el.setAttribute('data-pw-id', id)", unique_id)
                simplified_elements.append({"selector": f"[data-pw-id='{unique_id}']", "text": " ".join(text.strip().split())})

        if not simplified_elements:
            print(f"-> [SCRAPER] KI-Fallback: Keine klickbaren Kandidaten für '{purpose}' gefunden.")
            return False

        prompt = f"{prompt_templates[purpose]}\n\nElemente:\n{json.dumps(simplified_elements, indent=2)}"
        response = await asyncio.to_thread(model.generate_content, prompt)
        selector_to_click = response.text.strip().replace("`", "")

        if "none" not in selector_to_click.lower() and selector_to_click:
            print(f"-> [SCRAPER] KI hat '{purpose}'-Button identifiziert: {selector_to_click}. Klicke...")
            await page.locator(selector_to_click).first.click(timeout=5000)
            return True
        else:
            print(f"-> [SCRAPER] KI hat keinen '{purpose}'-Button gefunden.")
            return False
    except Exception as e:
        print(f"!!! [SCRAPER] Fehler im KI-Fallback für '{purpose}': {e}", file=sys.stderr)
        return False

async def run_scraper(target_url):
    print(f"-> [SCRAPER] Starte Scraping-Prozess für URL: {target_url}")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        try:
            await page.goto(target_url, wait_until="networkidle", timeout=REQUEST_TIMEOUT)
            print("-> [SCRAPER] Seite geladen. Warte kurz auf dynamische Elemente...")
            await page.wait_for_timeout(3000)

            print("-> [SCRAPER] Phase 1: Cookie-Banner-Handhabung")
            await find_and_click_element(page, 'cookie', 'cookie')
            await page.wait_for_timeout(3000)
            
            print("-> [SCRAPER] Phase 2: Interaktionsschleife zum Nachladen von Jobs")
            stagnation_counter = 0
            job_selector = 'a[href*="job"], div[class*="job"], li[class*="job"]' # Generischer Selektor
            
            for i in range(15): # Maximal 15 Klicks, um Endlosschleifen zu vermeiden
                initial_job_count = await page.locator(job_selector).count()
                print(f"--> Interaktion {i+1}: Aktuelle Job-Anzahl auf Seite: {initial_job_count}")

                clicked_something = await find_and_click_element(page, 'load_more', f'page-{i}')
                if not clicked_something:
                    print("--> Interaktion {i+1}: Kein 'Mehr laden'-Button gefunden. Schleife beendet.")
                    break
                
                print("--> Interaktion {i+1}: Warte auf Netzwerk-Aktivität nach Klick...")
                try:
                    await page.wait_for_load_state('networkidle', timeout=10000)
                except Exception:
                    print("--> Interaktion {i+1}: Netzwerk-Timeout. Mache mit kurzem Sleep weiter.")
                    await page.wait_for_timeout(5000)

                final_job_count = await page.locator(job_selector).count()
                print(f"--> Interaktion {i+1}: Job-Anzahl nach Klick: {final_job_count}")

                if final_job_count <= initial_job_count:
                    stagnation_counter += 1
                    if stagnation_counter >= MAX_JOB_COUNT_CHECKS:
                        print(f"--> Interaktion {i+1}: Job-Anzahl stagniert {stagnation_counter} mal. Schleife beendet.")
                        break
                else:
                    stagnation_counter = 0

            print("-> [SCRAPER] Interaktion abgeschlossen. Extrahiere finalen HTML-Inhalt.")
            final_content = await page.content()
            await browser.close()
            return final_content
        except Exception as e:
            await browser.close()
            raise IOError(f"!!! [SCRAPER] Kritischer Fehler beim Scraping von {target_url}: {e}")

async def parse_html_with_ai(html_content, company_id, company_name):
    print(f"--> [PARSER] Starte HTML-Analyse mit Gemini für '{company_name}'...")
    prompt = f"""
    Analysiere den folgenden HTML-Code einer Karriereseite. Extrahiere alle ausgeschriebenen Jobs.
    Gib das Ergebnis als valides JSON-Array zurück. Jedes Objekt im Array muss exakt die folgenden Schlüssel haben: "job_titel", "job_url", "job_standort".
    - Der "job_standort" ist oft in der Nähe des Titels. Wenn kein Standort gefunden wird, setze den Wert auf "Unbekannt".
    - Die "job_url" muss die absolute URL zur Job-Detailseite sein.
    - Gib NUR das JSON-Array zurück, sonst nichts.

    HTML-Code (Ausschnitt):
    ```html
    {html_content[:25000]}
    ```
    """
    try:
        response = await asyncio.to_thread(
            model.generate_content,
            prompt,
            generation_config={"response_mime_type": "application/json"}
        )
        extracted_jobs = json.loads(response.text)
        print(f"--> [PARSER] {len(extracted_jobs)} Jobs durch KI extrahiert.")
        
        rows_to_insert = []
        now = datetime.now(timezone.utc).isoformat()
        for job in extracted_jobs:
            job_titel = job.get("job_titel")
            job_url = job.get("job_url")

            # Erzeuge eine eindeutige ID für den Job
            if job_titel and job_url:
                unique_string = f"{company_id}-{job_url}-{job_titel}"
                job_id = hashlib.md5(unique_string.encode()).hexdigest()
            else:
                job_id = None # Falls Titel oder URL fehlen

            rows_to_insert.append({
                "company_id": company_id,
                "company_name": company_name,
                "job_id": job_id, # Das neue Feld
                "job_titel": job_titel,
                "job_standort": job.get("job_standort", "Unbekannt"),
                "job_url": job_url,
                "first_seen_timestamp": now,
                "last_seen_timestamp": now
            })
        return rows_to_insert
    except Exception as e:
        print(f"!!! [PARSER] FEHLER bei der KI-Analyse oder JSON-Verarbeitung: {e}", file=sys.stderr)
        print(f"KI-Antwort war: {getattr(response, 'text', 'Keine Antwort erhalten')}", file=sys.stderr)
        return []

# --- Hauptlogik (angepasst) ---
async def main():
    # Die Daten kommen jetzt aus Umgebungsvariablen, die von Cloud Tasks gesetzt werden.
    company_id = int(os.getenv("COMPANY_ID"))
    company_name = os.getenv("COMPANY_NAME")
    career_page_url = os.getenv("CAREER_PAGE_URL")

    if not all([company_id, company_name, career_page_url]):
        print("!!! FEHLER: Eine der Umgebungsvariablen (COMPANY_ID, COMPANY_NAME, CAREER_PAGE_URL) fehlt.", file=sys.stderr)
        sys.exit(1)

    print(f"\n--- JOB-AUSFÜHRUNG GESTARTET FÜR: {company_name} (ID: {company_id}) ---")

    try:
        html = await run_scraper(career_page_url)
        if html:
            jobs_to_insert = await parse_html_with_ai(html, company_id, company_name)
            write_jobs_to_bigquery(jobs_to_insert)
        
        update_company_timestamp(company_id)
        print(f"--- JOB-AUSFÜHRUNG FÜR {company_name} ERFOLGREICH BEENDET ---")

    except Exception as e:
        print(f"!!! KRITISCHER FEHLER im Hauptprozess für Firma {company_name}: {e}", file=sys.stderr)
        sys.exit(1)


# --- Einstiegspunkt des Skripts ---
if __name__ == "__main__":
    # Erwartet keine Argumente mehr, sondern Umgebungsvariablen
    asyncio.run(main())
