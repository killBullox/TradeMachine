"""
Script di autenticazione una-tantum per salvare la sessione Telegram.
Eseguire UNA VOLTA prima di avviare il server principale:
  cd backend
  python auth.py
"""
import asyncio
import os
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError

load_dotenv()

API_ID = int(os.getenv("TELEGRAM_API_ID"))
API_HASH = os.getenv("TELEGRAM_API_HASH")
PHONE = os.getenv("TELEGRAM_PHONE")


async def main():
    client = TelegramClient("trading_session", API_ID, API_HASH)
    await client.connect()

    if await client.is_user_authorized():
        me = await client.get_me()
        print(f"[Auth] Sessione gia' valida come: {me.first_name} (@{me.username})")
        await client.disconnect()
        return

    print(f"[Auth] Invio codice OTP a {PHONE}...")
    await client.send_code_request(PHONE)

    code = input("[Auth] Inserisci il codice ricevuto su Telegram: ").strip()

    try:
        await client.sign_in(PHONE, code)
    except SessionPasswordNeededError:
        pwd = input("[Auth] Inserisci la password 2FA: ").strip()
        await client.sign_in(password=pwd)

    me = await client.get_me()
    print(f"[Auth] Autenticato come: {me.first_name} (@{me.username})")
    print("[Auth] Sessione salvata in trading_session.session")
    print("[Auth] Ora puoi avviare il server con: python -m uvicorn main:app --reload --port 8000")
    await client.disconnect()


asyncio.run(main())
