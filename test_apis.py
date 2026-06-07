"""
Test de APIs: VELSA y NOVONET Bitrix24
Ejecutar desde la carpeta BotAuditor:
    python test_apis.py
"""
import os, sys, json, urllib.request, urllib.error
from dotenv import load_dotenv

load_dotenv()

VELSA_BITRIX   = os.getenv("VELSA_BITRIX",   "https://aclopecuador.bitrix24.es/rest/1/49hra49433psie0t")
NOVONET_BITRIX = os.getenv("NOVONET_BITRIX", "https://novonet.bitrix24.es/rest/87387/vcca209sfcjflxp8")

def fetch(url, label):
    print(f"\n[{label}] GET {url}")
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())
            if "error" in data:
                print(f"  ✗ ERROR Bitrix: {data['error']} - {data.get('error_description','')}")
                return False
            result = data.get("result", {})
            print(f"  ✓ OK  |  Usuario: {result.get('NAME','?')} {result.get('LAST_NAME','?')}  |  Admin: {result.get('ADMIN','?')}")
            return True
    except urllib.error.HTTPError as e:
        print(f"  ✗ HTTP {e.code}: {e.reason}")
    except Exception as e:
        print(f"  ✗ Error: {e}")
    return False

def test_deal_list(bitrix_url, label, limit=3):
    url = f"{bitrix_url}/crm.deal.list?select[]=ID&select[]=TITLE&select[]=STAGE_ID&LIMIT={limit}"
    print(f"\n[{label}] crm.deal.list (últimos {limit} deals)")
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())
            if "error" in data:
                print(f"  ✗ ERROR: {data['error']}")
                return False
            deals = data.get("result", [])
            print(f"  ✓ {len(deals)} deal(s) obtenido(s):")
            for d in deals:
                print(f"    ID={d.get('ID')}  Stage={d.get('STAGE_ID')}  Título={d.get('TITLE','')[:50]}")
            return True
    except Exception as e:
        print(f"  ✗ Error: {e}")
    return False

print("=" * 60)
print("  BOT AUDITOR - TEST DE APIs")
print("=" * 60)

ok_velsa   = fetch(f"{VELSA_BITRIX}/profile",   "VELSA")
ok_novonet = fetch(f"{NOVONET_BITRIX}/profile", "NOVONET")

if ok_velsa:
    test_deal_list(VELSA_BITRIX, "VELSA")
if ok_novonet:
    test_deal_list(NOVONET_BITRIX, "NOVONET")

print("\n" + "=" * 60)
print(f"  VELSA:   {'✓ OK' if ok_velsa   else '✗ FALLA'}")
print(f"  NOVONET: {'✓ OK' if ok_novonet else '✗ FALLA'}")
print("=" * 60)

input("\nPresiona ENTER para cerrar...")
