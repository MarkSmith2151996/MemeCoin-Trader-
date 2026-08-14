"""Test Jupiter Tokens V2 API endpoints for Strategy B candidate discovery."""
import asyncio, os, json, httpx
from datetime import datetime, timezone

API_KEY = os.environ.get("JUPITER_API_KEY", "")
BASE = "https://api.jup.ag/tokens/v2"
HEADERS = {"x-api-key": API_KEY}

async def main():
    async with httpx.AsyncClient(timeout=15) as c:
        # 1. /recent — freshest tokens by first pool creation
        r1 = await c.get(f"{BASE}/recent", headers=HEADERS)
        recent = r1.json() if r1.status_code == 200 else []
        print(f"\n=== /recent: {r1.status_code}, {len(recent)} tokens ===")

        # 2. /toporganicscore/5m — trending by organic activity
        r2 = await c.get(f"{BASE}/toporganicscore/5m?limit=100", headers=HEADERS)
        organic = r2.json() if r2.status_code == 200 else []
        print(f"=== /toporganicscore/5m: {r2.status_code}, {len(organic)} tokens ===")

        # 3. /toptrending/5m
        r3 = await c.get(f"{BASE}/toptrending/5m?limit=100", headers=HEADERS)
        trending = r3.json() if r3.status_code == 200 else []
        print(f"=== /toptrending/5m: {r3.status_code}, {len(trending)} tokens ===")

        # 4. Check field coverage on /recent tokens
        now = datetime.now(timezone.utc)
        print("\n=== Field coverage check (first 5 from /recent) ===")
        for t in recent[:5]:
            age_str = "?"
            fp = t.get("firstPool", {})
            if fp.get("createdAt"):
                created = datetime.fromisoformat(fp["createdAt"].replace("Z", "+00:00"))
                age_min = (now - created).total_seconds() / 60
                age_str = f"{age_min:.0f}m"

            audit = t.get("audit", {})
            s5 = t.get("stats5m", {})
            buys = s5.get("numBuys", 0)
            sells = s5.get("numSells", 0)
            bsr = buys / max(sells, 1)

            print(f"  {t.get('symbol','?'):10s} mint={t.get('id','?')[:12]}... "
                  f"age={age_str} mcap=${t.get('mcap',0):,.0f} liq=${t.get('liquidity',0):,.0f} "
                  f"vol5m=${s5.get('buyVolume',0)+s5.get('sellVolume',0):,.0f} "
                  f"buys={buys} sells={sells} bsr={bsr:.1f} "
                  f"organic={t.get('organicScore',0):.0f} "
                  f"mintAuth={'off' if audit.get('mintAuthorityDisabled') else 'ON'} "
                  f"freezeAuth={'off' if audit.get('freezeAuthorityDisabled') else 'ON'} "
                  f"isSus={audit.get('isSus','n/a')} "
                  f"holders={t.get('holderCount','?')}")

        # 5. Apply Strategy B gates to /recent and count pass/fail
        print("\n=== Strategy B gate simulation on /recent ===")
        passed = 0
        for t in recent:
            fp = t.get("firstPool", {})
            if not fp.get("createdAt"):
                continue
            created = datetime.fromisoformat(fp["createdAt"].replace("Z", "+00:00"))
            age_min = (now - created).total_seconds() / 60
            mcap = t.get("mcap") or t.get("fdv") or 0
            s5 = t.get("stats5m", {})
            vol = (s5.get("buyVolume", 0) or 0) + (s5.get("sellVolume", 0) or 0)
            buys = s5.get("numBuys", 0) or 0
            sells = s5.get("numSells", 0) or 0
            bsr = buys / max(sells, 1)

            # Strategy B current gates (from MT-537)
            if age_min > 22:
                continue
            if mcap < 5000:
                continue
            if vol < 500:
                continue
            if bsr < 1.0:
                continue
            passed += 1
            print(f"  PASS: {t.get('symbol','?')} age={age_min:.0f}m mcap=${mcap:,.0f} "
                  f"vol=${vol:,.0f} bsr={bsr:.1f} organic={t.get('organicScore',0):.0f}")

        print(f"\n{passed}/{len(recent)} tokens passed Strategy B gates")

        # 6. Save raw responses for offline analysis
        with open("data/jupiter_v2_test.json", "w") as f:
            json.dump({"recent": recent, "organic": organic, "trending": trending,
                        "tested_at": now.isoformat()}, f, indent=2)
        print(f"\nRaw data saved to data/jupiter_v2_test.json")

asyncio.run(main())
