import os
import subprocess
import asyncio
import functions_framework
from flask import jsonify
from playwright.async_api import async_playwright
import google.generativeai as genai

# --- Browser-Installation sicherstellen ---
install_marker_path = "/tmp/playwright_installed"
if not os.path.exists(install_marker_path):
    print("Playwright-Browser nicht gefunden. Starte Installation...")
    subprocess.run("playwright install", shell=True, check=True)
    with open(install_marker_path, "w") as f:
        f.write("done")
    print("Playwright-Browser erfolgreich installiert.")

# --- Konfiguration ---
MAX_INTERACTIONS = 15

# --- API Initialisierung ---
api_key = os.getenv('GEMINI_API_KEY')
if not api_key:
    raise ValueError("GEMINI_API_KEY Umgebungsvariable nicht gesetzt.")

genai.configure(api_key=api_key)
model = genai.GenerativeModel("gemini-2.5-flash")

# --- Hauptlogik ---
async def run_scraper(request):
    request_json = request.get_json(silent=True)
    if not request_json or "url" not in request_json:
        return jsonify({"error": "Bitte geben Sie eine 'url' im JSON-Body an."}), 400

    target_url = request_json["url"]

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        try:
            await page.goto(target_url, wait_until="domcontentloaded", timeout=60000)
            await asyncio.sleep(5)

            # =================================================================
            # NEUER SCHRITT: Intelligente KI-basierte Cookie-Banner-Handhabung
            # =================================================================
            try:
                print("Suche nach Cookie-Banner zur Akzeptanz...")
                cookie_candidates = await page.query_selector_all('button, a, [role="button"], [onclick]')
                
                cookie_simplified_elements = []
                for idx, element in enumerate(cookie_candidates):
                    text = await element.inner_text()
                    is_visible = await element.is_visible()
                    if is_visible and text.strip():
                        unique_id = f"pw-cookie-candidate-{idx}"
                        await element.evaluate("(el, id) => el.setAttribute('data-pw-id', id)", unique_id)
                        cookie_simplified_elements.append({
                            "selector": f"[data-pw-id='{unique_id}']",
                            "text": " ".join(text.strip().split())
                        })

                if cookie_simplified_elements:
                    cookie_prompt = f"""
Du bist ein Experte für Web-Automatisierung. Deine Aufgabe ist es, den EINEN Button zu identifizieren, der Cookies akzeptiert oder der Datenverarbeitung zustimmt.
Achte auf Schlüsselwörter wie 'Alle akzeptieren', 'Zustimmen', 'Einverstanden', 'Ja', 'Accept all', 'Agree'.
Analysiere die folgende Liste von Elementen. Antworte NUR mit dem CSS-Selektor des korrekten Elements. Wenn kein passendes Element gefunden wird, antworte mit 'NONE'.

Elemente:
{cookie_simplified_elements}
"""
                    response = await asyncio.to_thread(model.generate_content, cookie_prompt)
                    cookie_selector_to_click = response.text.strip()

                    if "none" not in cookie_selector_to_click.lower() and cookie_selector_to_click:
                        print(f"KI hat Cookie-Button identifiziert: {cookie_selector_to_click}. Klicke...")
                        await page.locator(cookie_selector_to_click).click(timeout=5000)
                        await asyncio.sleep(3) # Kurze Pause nach dem Klick
                    else:
                        print("KI hat keinen Cookie-Button identifiziert.")
                else:
                    print("Keine klickbaren Elemente für Cookie-Prüfung gefunden.")

            except Exception as e:
                print(f"Fehler bei der Cookie-Banner-Handhabung (oft unkritisch): {e}")
            # =================================================================
            # ENDE DES NEUEN SCHRITTS
            # =================================================================

            # Start der Hauptschleife zur Job-Interaktion (unverändert)
            for i in range(MAX_INTERACTIONS):
                print(f"Interaktion {i + 1}/{MAX_INTERACTIONS}...")
                initial_html = await page.evaluate("() => document.body.innerHTML")

                candidates = await page.query_selector_all('button, a, [role="button"], [onclick]')
                
                simplified_elements = []
                for idx, element in enumerate(candidates):
                    text = await element.inner_text()
                    is_visible = await element.is_visible()
                    if is_visible and text.strip():
                        unique_id = f"pw-candidate-{i}-{idx}"
                        await element.evaluate("(el, id) => el.setAttribute('data-pw-id', id)", unique_id)
                        
                        simplified_elements.append({
                            "selector": f"[data-pw-id='{unique_id}']",
                            "text": " ".join(text.strip().split())
                        })
                
                if not simplified_elements:
                    print("Keine weiteren klickbaren Kandidaten gefunden.")
                    break
                
                prompt = f"""
Du bist ein Experte für Web-Automatisierung. Deine Aufgabe ist es, das EINE Bedienelement zu identifizieren, das weitere Jobs auf einer Karriereseite lädt.
Mögliche Elemente sind Buttons mit 'Mehr laden', 'Weitere Jobs', Pfeile für 'Nächste Seite' oder Seitenzahlen.
Analysiere die folgende Liste von klickbaren Elementen. Antworte NUR mit dem CSS-Selektor des korrekten Elements. Wenn kein passendes Element gefunden wird, antworte mit 'NONE'.

Elemente:
{simplified_elements}
"""
                response = await asyncio.to_thread(model.generate_content, prompt)
                selector_to_click = response.text.strip()

                if "none" in selector_to_click.lower() or not selector_to_click:
                    print("KI hat kein weiteres Element identifiziert. Beende Interaktion.")
                    break
                
                try:
                    print(f"KI hat gewählt: {selector_to_click}. Führe Klick aus.")
                    await page.locator(selector_to_click).click(timeout=10000)
                    await asyncio.sleep(5)
                except Exception as e:
                    print(f"Konnte Element '{selector_to_click}' nicht klicken: {e}. Beende Interaktion.")
                    break

                final_html = await page.evaluate("() => document.body.innerHTML")
                if final_html == initial_html:
                    print("Seiteninhalt hat sich nach Klick nicht geändert. Beende Interaktion.")
                    break

            final_content = await page.content()
            await browser.close()
            return jsonify({"html": final_content}), 200

        except Exception as e:
            if 'browser' in locals() and browser.is_connected():
                await browser.close()
            return jsonify({"error": f"Ein Fehler ist aufgetreten: {str(e)}"}), 500

# --- Entry Point (unverändert zur letzten Version) ---
@functions_framework.http
def intelligent_renderer(request):
    """
    Synchroner Wrapper, der die asynchrone Funktion mit asyncio.run() aufruft.
    """
    return asyncio.run(run_scraper(request))
