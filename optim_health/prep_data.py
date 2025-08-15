# prep_data.py
import re
from pathlib import Path
import numpy as np
import pandas as pd

def _normalize_colnames(df: pd.DataFrame) -> pd.DataFrame:
    def keyify(s): return re.sub(r"[^a-z0-9]", "", s.lower())
    keys = {c: keyify(c) for c in df.columns}

    def find(*cands):
        for orig, k in keys.items():
            if k in cands:
                return orig
        return None

    ren = {}
    if (c := find("billingcode","code","cpt","cptcode","codenorm")): ren[c] = "billing_code"
    if (c := find("description","codedescription","desc")): ren[c] = "description"
    if (c := find("rbcssubcatdesc","rbcssubcat","rbcsdesc","procedureclass","subcategory")): ren[c] = "RBCS_SubCat_Desc"
    if (c := find("uhcrate","uhcratemedian","negotiatedrate","price","mediannegotiatedrate")): ren[c] = "uhc_rate"
    if (c := find("gawcrate","gawcrates","gawc")): ren[c] = "ga_wc_rate"
    if (c := find("medicareallowed","medicareallowable","medicare")): ren[c] = "medicare_allowed"
    if (c := find("cbsatitle")): ren[c] = "CBSA Title"
    if (c := find("billingclass","billingtype")): ren[c] = "billing_class"
    if (c := find("reportingentityname","reportingentity")): ren[c] = "reporting_entity_name"
    if (c := find("lastupdatedonx","lastupdatedon","filedate","updatedon")): ren[c] = "last_updated_on_x"
    if (c := find("versionx","version")): ren[c] = "version_x"
    return df.rename(columns=ren)

def _coerce_money(s: pd.Series) -> pd.Series:
    if s is None: return s
    s = s.astype(str).str.strip()
    s = s.str.replace(r"(?<=\d):(?=\d{2}\b)", ".", regex=True)  # 615:00 -> 615.00
    s = s.str.replace(r"[\$,]", "", regex=True)
    s = s.replace({"—": np.nan, "": np.nan, "None": np.nan, "nan": np.nan})
    return pd.to_numeric(s, errors="coerce")

def _derive_ortho(df: pd.DataFrame) -> pd.Series:
    desc = df.get("description", pd.Series("", index=df.index)).fillna("").astype(str)
    proc = df.get("RBCS_SubCat_Desc", pd.Series("", index=df.index)).fillna("").astype(str)
    kw = (r"(?i)\b(orthop|ortho|shoulder|knee|hip|spine|lumbar|cervical|thoracic|"
          r"meniscus|acl|mcl|pcl|rotator|labrum|ankle|elbow|wrist|hand|foot|finger|toe|carpal|tarsal)\b")
    flag = desc.str.contains(kw) | proc.str.contains(r"(?i)orthop")
    if "billing_code" in df.columns:
        bc = df["billing_code"].astype(str)
        flag = flag | bc.str.match(r"^(23|27|73)\d{3}$")
    return flag.astype(bool)

def clean_for_app(input_path: str, out_csv: str = None, out_parquet: str = None) -> pd.DataFrame:
    # 1) read
    if input_path.endswith('.parquet'):
        df = pd.read_parquet(input_path)
    else:
        try:
            df = pd.read_csv(input_path)
        except UnicodeDecodeError:
            df = pd.read_csv(input_path, encoding="latin-1")

    # 2) normalize names
    df = _normalize_colnames(df)

    # 3) ensure required columns
    for c in ["billing_code","description","RBCS_SubCat_Desc","uhc_rate","ga_wc_rate","medicare_allowed"]:
        if c not in df.columns:
            df[c] = np.nan

    # 4) clean billing_code
    df["billing_code"] = (df["billing_code"].astype(str)
                          .str.strip().str.replace(r"[\s\.\-]+", "", regex=True).str.upper())

    # 5) numerics
    for c in ["uhc_rate","ga_wc_rate","medicare_allowed","price","negotiated_rate","uhc_rate_median"]:
        if c in df.columns:
            df[c] = _coerce_money(df[c])

    # 6) uhc_rate fallback
    if "uhc_rate" not in df.columns or df["uhc_rate"].isna().all():
        cands = [c for c in ["uhc_rate","negotiated_rate","price","uhc_rate_median"] if c in df.columns]
        if cands:
            df["uhc_rate"] = pd.to_numeric(df[cands].bfill(axis=1).iloc[:,0], errors="coerce")

    # 7) percents
    df["pct_of_ga_wc"] = np.where(df.get("ga_wc_rate", np.nan) > 0, df["uhc_rate"]/df["ga_wc_rate"], np.nan)
    df["pct_of_medicare"] = np.where(df.get("medicare_allowed", np.nan) > 0, df["uhc_rate"]/df["medicare_allowed"], np.nan)

    # 8) ortho bucket (derive if missing OR all-null)
    if "ortho_bucket" not in df.columns or df["ortho_bucket"].isna().all():
        df["ortho_bucket"] = _derive_ortho(df)

    # 9) parse dates + trim text
    if "last_updated_on_x" in df.columns:
        df["last_updated_on_x"] = pd.to_datetime(df["last_updated_on_x"], errors="coerce")
    for c in ["description","RBCS_SubCat_Desc","CBSA Title","billing_class","reporting_entity_name","version_x"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip().replace({"nan": np.nan})

    # 10) drop true dupes
    df = df.drop_duplicates()

    # 11) save
    if out_csv:
        Path(out_csv).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_csv, index=False)
    if out_parquet:
        try:
            df.to_parquet(out_parquet, index=False)
        except Exception as e:
            print("Parquet not written (install pyarrow or fastparquet):", e)

    return df

if __name__ == "__main__":
    # Example usage:
    clean_for_app("combined_df.parquet",
                  out_csv="app_df.csv",
                  out_parquet="app_df.parquet")
