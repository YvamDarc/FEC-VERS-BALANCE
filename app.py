import io
import re
from decimal import Decimal, InvalidOperation

import pandas as pd
import streamlit as st


REQUIRED_COLUMNS = [
    "JournalCode", "JournalLib", "EcritureNum", "EcritureDate",
    "CompteNum", "CompteLib", "EcritureLib", "Debit", "Credit"
]

AUX_COLUMNS = ["CompAuxNum", "CompAuxLib"]


def detect_separator(raw_bytes: bytes) -> str:
    sample = raw_bytes[:5000].decode("utf-8", errors="ignore")
    candidates = ["\t", ";", ",", "|"]
    counts = {sep: sample.count(sep) for sep in candidates}
    return max(counts, key=counts.get)


def read_fec(uploaded_file) -> pd.DataFrame:
    raw = uploaded_file.read()
    sep = detect_separator(raw)

    encodings = ["utf-8-sig", "utf-8", "latin1", "cp1252"]
    last_error = None

    for enc in encodings:
        try:
            return pd.read_csv(
                io.BytesIO(raw),
                sep=sep,
                encoding=enc,
                dtype=str,
                keep_default_na=False,
                engine="python",
            )
        except Exception as exc:
            last_error = exc

    raise ValueError(f"Impossible de lire le fichier FEC : {last_error}")


def clean_amount(value) -> Decimal:
    if value is None:
        return Decimal("0")

    text = str(value).strip()
    if text == "":
        return Decimal("0")

    text = text.replace(" ", "").replace("\u00a0", "")
    text = text.replace(",", ".")

    # Supprime les caractères parasites éventuels
    text = re.sub(r"[^0-9.\-]", "", text)

    if text in ["", "-", "."]:
        return Decimal("0")

    try:
        return Decimal(text)
    except InvalidOperation:
        return Decimal("0")


def validate_fec_columns(df: pd.DataFrame):
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError("Colonnes obligatoires manquantes : " + ", ".join(missing))


def build_balance(df: pd.DataFrame, detail_auxiliaire: bool = True) -> pd.DataFrame:
    validate_fec_columns(df)

    work = df.copy()
    work["DebitDecimal"] = work["Debit"].apply(clean_amount)
    work["CreditDecimal"] = work["Credit"].apply(clean_amount)

    # Nettoyage des libellés
    work["CompteNum"] = work["CompteNum"].astype(str).str.strip()
    work["CompteLib"] = work["CompteLib"].astype(str).str.strip()

    has_aux = all(col in work.columns for col in AUX_COLUMNS)

    if detail_auxiliaire and has_aux:
        work["CompAuxNum"] = work["CompAuxNum"].astype(str).str.strip()
        work["CompAuxLib"] = work["CompAuxLib"].astype(str).str.strip()

        # Si auxiliaire présent : on détaille le compte par auxiliaire.
        # Exemple : 401000 / FOURNISSEUR X, 411000 / CLIENT Y, etc.
        work["CompteFinal"] = work.apply(
            lambda row: f"{row['CompteNum']}-{row['CompAuxNum']}" if row["CompAuxNum"] else row["CompteNum"],
            axis=1,
        )
        work["LibelleFinal"] = work.apply(
            lambda row: row["CompAuxLib"] if row["CompAuxLib"] else row["CompteLib"],
            axis=1,
        )
    else:
        work["CompteFinal"] = work["CompteNum"]
        work["LibelleFinal"] = work["CompteLib"]

    grouped = (
        work.groupby(["CompteFinal", "LibelleFinal"], dropna=False, as_index=False)
        .agg(Debit=("DebitDecimal", "sum"), Credit=("CreditDecimal", "sum"))
    )

    # Présentation en balance : solde au débit OU au crédit, pas les deux.
    grouped["Solde"] = grouped["Debit"] - grouped["Credit"]
    grouped["Débit"] = grouped["Solde"].apply(lambda x: x if x > 0 else Decimal("0"))
    grouped["Crédit"] = grouped["Solde"].apply(lambda x: -x if x < 0 else Decimal("0"))

    balance = grouped[["CompteFinal", "LibelleFinal", "Débit", "Crédit"]].copy()
    balance.columns = ["compte", "libellé", "débit", "crédit"]

    balance = balance.sort_values("compte", kind="stable").reset_index(drop=True)

    # Format compatible CSV français
    for col in ["débit", "crédit"]:
        balance[col] = balance[col].apply(lambda x: f"{x:.2f}".replace(".", ","))

    return balance


def dataframe_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False, sep=";", encoding="utf-8-sig").encode("utf-8-sig")


st.set_page_config(page_title="FEC vers balance comptable", layout="wide")

st.title("Convertisseur FEC → Balance comptable")
st.caption("Format final : compte ; libellé ; débit ; crédit")

uploaded_file = st.file_uploader(
    "Dépose ton FEC ici",
    type=["txt", "csv"],
)

detail_auxiliaire = st.checkbox(
    "Détailler les comptes auxiliaires si CompAuxNum / CompAuxLib sont présents",
    value=True,
)

if uploaded_file is not None:
    try:
        fec = read_fec(uploaded_file)
        st.success(f"FEC chargé : {len(fec):,} lignes".replace(",", " "))

        with st.expander("Aperçu du FEC importé"):
            st.dataframe(fec.head(50), use_container_width=True)

        balance = build_balance(fec, detail_auxiliaire=detail_auxiliaire)

        debit_total = balance["débit"].str.replace(",", ".", regex=False).astype(float).sum()
        credit_total = balance["crédit"].str.replace(",", ".", regex=False).astype(float).sum()
        ecart = round(debit_total - credit_total, 2)

        col1, col2, col3 = st.columns(3)
        col1.metric("Total débit", f"{debit_total:,.2f}".replace(",", " ").replace(".", ","))
        col2.metric("Total crédit", f"{credit_total:,.2f}".replace(",", " ").replace(".", ","))
        col3.metric("Écart", f"{ecart:,.2f}".replace(",", " ").replace(".", ","))

        if abs(ecart) > 0.01:
            st.warning("La balance n'est pas équilibrée. Vérifie le FEC ou les montants débit/crédit.")
        else:
            st.success("Balance équilibrée.")

        st.subheader("Balance générée")
        st.dataframe(balance, use_container_width=True)

        st.download_button(
            label="Télécharger la balance CSV",
            data=dataframe_to_csv_bytes(balance),
            file_name="balance_depuis_fec.csv",
            mime="text/csv",
        )

    except Exception as exc:
        st.error(str(exc))
else:
    st.info("Charge un fichier FEC pour générer la balance.")
