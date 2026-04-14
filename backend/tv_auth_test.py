import sys, requests, re, warnings
warnings.filterwarnings('ignore')

SESSIONID = "fy4w0mb412oikv0lpy4ha4qd48owbl4f"
SESSIONID_SIGN = "b8b435ea-0126-4269-9812-80793a4555fb"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "it-IT,it;q=0.9,en;q=0.8",
}
cookies = {
    "sessionid": SESSIONID,
    "sessionid_sign": SESSIONID_SIGN,
}

r = requests.get("https://www.tradingview.com/", headers=headers, cookies=cookies)
print("Status:", r.status_code)

if "is-not-authenticated" in r.text:
    print("ERRORE: Sessione NON autenticata")
elif "is-authenticated" in r.text:
    print("OK: Sessione AUTENTICATA")

# Cerca auth_token
match = re.search(r'"auth_token"\s*:"([^"]+)"', r.text)
if match:
    token = match.group(1)
    print(f"auth_token: {token[:40]}...")

    # Testa con tvdatafeed
    sys.path.insert(0, r'C:\Users\gianl\AppData\Roaming\Python\Python314\site-packages')
    from tvdatafeed import TvDatafeed, Interval
    import random, string

    class TvAuth(TvDatafeed):
        def __init__(self, token):
            self.token = token
            self.session = "qs_" + "".join(random.choices(string.ascii_lowercase, k=12))
            self.chart_session = "cs_" + "".join(random.choices(string.ascii_lowercase, k=12))

    tv = TvAuth(token)
    print("\nTest XAUUSD 1m (5000 bars)...")
    df = tv.get_hist("XAUUSD", "OANDA", interval=Interval.in_1_minute, n_bars=5000)
    if df is not None and not df.empty:
        print(f"OK: {len(df)} bars | da {df.index[0]} | ultimo close: {df['close'].iloc[-1]}")
    else:
        print("FAIL: nessun dato")
else:
    print("auth_token non trovato")
    # Debug
    idx = r.text.find("auth_token")
    if idx > 0:
        print("Contesto:", r.text[idx-5:idx+120])
