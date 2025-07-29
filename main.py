import os
import asyncio
import sys
import google.generativeai as genai
from playwright.async_api import async_playwright

# --- Konfiguration ---
REQUEST_TIMEOUT = 90000  # 90 Sekunden Timeout für Seiten-Aktionen
MAX_JOB_COUNT_CHECKS = 3 # Wie oft darf die Job-Anzahl gleich bleiben, bevor wir abbrechen?

# --- API Initialisierung ---
api_key = os.getenv('GEMINI_API_KEY')
if not api_key:
    raise ValueError("GEMINI_API_KEY Umgebungsvariable nicht gesetzt.")

genai.configure(api_key=api_key)
model = genai.GenerativeModel("gemini-1.5-flash")

# --- Hilfsfunktion: Intelligentes Klicken (unverändert) ---
async def find_and_click_element(page, purpose, unique_id_prefix):
    # Diese Funktion ist identisch zur vorherigen Version.
    print(f"Suche nach '{purpose}'-Button...")
    common_texts = {
        'cookie': ['Alle akzeptieren', 'Accept all', 'Zustimmen', 'Einverstanden', 'Agree'],
        'load_more': ['Mehr laden', 'Weitere Jobs anzeigen', 'Weitere anzeigen', 'Load more', 'Show more']
    }
    pagination_selectors = [
        'a[aria-label*="next" i]', 'a[aria-label*="nächste" i]',
        'button[aria-label*="next" i]', 'button[aria-label*="nächste" i]',
        'a:has-text("Next"), a:has-text("Nächste")',
        'button:has-text("Next"), button:has-text("Nächste")'
    ]
    selectors_to_try = [f'text=/{text}/i' for text in common_texts.get(purpose, [])]
    if purpose == 'load_more':
        selectors_to_try.extend(pagination_selectors)

    for selector in selectors_to_try:
        try:
            element = page.locator(selector).first
            if await element.is_visible():
                print(f"Deterministischer Erfolg: Klicke auf Element mit Selektor '{selector}'.")
                await element.click(timeout=5000)
                return True
        except Exception:
            continue

    print("Deterministische Suche erfolglos. Starte KI-Fallback...")
    try:
        candidates = await page.query_selector_all('button, a, [role="button"]')
        simplified_elements = []
        for idx, element in enumerate(candidates):
            text = await element.inner_text()
            is_visible = await element.is_visible()
            if is_visible and text.strip():
                unique_id = f"pw-{unique_id_prefix}-{idx}"
                await element.evaluate("(el, id) => el.setAttribute('data-pw-id', id)", unique_id)
                simplified_elements.append({
                    "selector": f"[data-pw-id='{unique_id}']",
                    "text": " ".join(text.strip().split())
                })
        if not simplified_elements: return False
        
        prompt_templates = {
            'cookie': "Du bist ein Experte für Web-Automatisierung. Identifiziere den EINEN Button, der Cookies akzeptiert. Achte auf 'Alle akzeptieren', 'Zustimmen', 'Agree'. Antworte NUR mit dem CSS-Selektor. Wenn nichts passt, antworte 'NONE'.",
            'load_more': "Du bist ein Experte für Web-Automatisierung. Identifiziere das EINE Bedienelement (Button, Link), das MEHR JOBS lädt oder zur NÄCHSTEN SEITE navigiert. Antworte NUR mit dem CSS-Selektor. Wenn nichts passt, antworte 'NONE'."
        }
        prompt = f"{prompt_templates[purpose]}\n\nElemente:\n{simplified_elements}"
        response = await asyncio.to_thread(model.generate_content, prompt)
        selector_to_click = response.text.strip()
        
        if "none" not in selector_to_click.lower() and selector_to_click:
            print(f"KI hat '{purpose}'-Button identifiziert: {selector_to_click}. Klicke...")
            await page.locator(selector_to_click).click(timeout=5000)
            return True
        else:
            return False
    except Exception as e:
        print(f"Fehler im KI-Fallback für '{purpose}': {e}")
        return False

# --- Hauptlogik für das Scraping ---
async def main(target_url):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        try:
            print(f"Navigiere zu: {target_url}")
            await page.goto(target_url, wait_until="networkidle", timeout=REQUEST_TIMEOUT)
            await find_and_click_element(page, 'cookie', 'cookie')
            await page.wait_for_timeout(2000)

            stagnation_counter = 0
            job_selector = 'a[href*="job"], div[class*="job"], li[class*="job"]'
            
            while True:
                initial_job_count = await page.locator(job_selector).count()
                print(f"Aktuelle Job-Anzahl auf der Seite: {initial_job_count}")
                clicked_something = await find_and_click_element(page, 'load_more', f'page-{stagnation_counter}')
                if not clicked_something:
                    print("Kein 'Mehr laden'-Element mehr gefunden. Beende Interaktion.")
                    break
                try:
                    await page.wait_for_load_state('networkidle', timeout=15000)
                except Exception as e:
                    print(f"Seite nach Klick nicht zur Ruhe gekommen (Timeout): {e}. Mache trotzdem weiter.")
                
                final_job_count = await page.locator(job_selector).count()
                if final_job_count <= initial_job_count:
                    stagnation_counter += 1
                    print(f"Job-Anzahl hat sich nicht erhöht. Stagnations-Zähler: {stagnation_counter}/{MAX_JOB_COUNT_CHECKS}")
                    if stagnation_counter >= MAX_JOB_COUNT_CHECKS:
                        print("Job-Anzahl stagniert. Beende Interaktion.")
                        break
                else:
                    stagnation_counter = 0
            
            final_content = await page.content()

            print("\n--- BEGINN FINAL HTML ---\n")
            print(final_content)
            print("\n--- ENDE FINAL HTML ---\n")
            await browser.close()
            print("Job erfolgreich beendet.")

        except Exception as e:
            if 'browser' in locals() and browser.is_connected():
                await browser.close()
            print(f"Ein schwerwiegender Fehler ist aufgetreten: {str(e)}", file=sys.stderr)
            sys.exit(1) # Beendet den Job mit einem Fehlerstatus

# --- Einstiegspunkt des Skripts ---
if __name__ == "__main__":
    # Die URL wird als Argument beim Start des Jobs übergeben.
    if len(sys.argv) < 2:
        print("Fehler: Bitte geben Sie eine URL als Argument an.", file=sys.stderr)
        sys.exit(1)
    
    url_to_scrape = sys.argv[1]
    asyncio.run(main(url_to_scrape))
