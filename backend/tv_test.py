"""Prova autenticazione via sessionid per avere più dati storici."""
import sys, requests, re
sys.path.insert(0, r'C:\Users\gianl\AppData\Roaming\Python\Python314\site-packages')
from tvdatafeed import TvDatafeed, Interval
import warnings
warnings.filterwarnings('ignore')

SESSIONID = "v3:5KkaCwzAI8jMXmyN67k69YXz/I3p1d2mE1cDbhv1T3w="

# Proviamo a ottenere l'auth_token via la pagina principale di TV
session = requests.Session()
session.cookies.set("sessionid", SESSIONID, domain=".tradingview.com")
session.cookies.set("device_t", "anonymous")

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0",
    "Accept": "application/json",
    "Referer": "https://www.tradingview.com/",
}

# Endpoint che restituisce il token auth
r = session.get("https://www.tradingview.com/pine_pubs/list/", headers=headers)
print("pine_pubs status:", r.status_code)

# Cerca auth_token nella homepage
r2 = session.get("https://www.tradingview.com/", headers=headers)
match = re.search(r'"auth_token"\s*:\s*"([^"]+)"', r2.text)
if match:
    auth_token = match.group(1)
    print("auth_token trovato:", auth_token[:30], "...")
    # Usa il token con tvdatafeed
    tv = TvDatafeed.__new__(TvDatafeed)
    tv.token = auth_token
    import random, string
    tv.session = "qs_" + "".join(random.choices(string.ascii_lowercase, k=12))
    tv.chart_session = "cs_" + "".join(random.choices(string.ascii_lowercase, k=12))
    df = tv.get_hist("XAUUSD", "OANDA", interval=Interval.in_15_minute, n_bars=5000)
    if df is not None:
        print(f"Con auth: {len(df)} bars da {df.index[0].date()}")
else:
    print("auth_token non trovato nella pagina")
    # Mostriamo un estratto della pagina per debug
    if "is-not-authenticated" in r2.text:
        print("ATTENZIONE: Sessione non autenticata")
    elif "is-authenticated" in r2.text:
        print("Sessione AUTENTICATA")
    auth_idx = r2.text.find("auth_token")
    if auth_idx > 0:
        print("Contesto auth_token:", r2.text[auth_idx-10:auth_idx+100])
