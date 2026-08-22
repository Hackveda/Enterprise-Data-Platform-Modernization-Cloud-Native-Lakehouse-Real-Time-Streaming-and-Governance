from pathlib import Path
import pandas as pd
Path("data/partners").mkdir(parents=True, exist_ok=True)
pd.DataFrame([
 {"partner_id":"P1","country":"IN","risk_tier":"LOW"},
 {"partner_id":"P2","country":"US","risk_tier":"MEDIUM"}
]).to_csv("data/partners/partner_master.csv", index=False)
print("created data/partners/partner_master.csv")
