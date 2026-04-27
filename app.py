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


# Colonnes type FEC / import ACD.
# Le tableau AN sera volontairement simple et copiable-collable dans Excel / ACD.
ACD_AN_COLUMNS = [
    "JournalCode",
    "JournalLib",
    "EcritureNum",
    "EcritureDate",
    "CompteNum",
    "CompteLib",
    "CompAuxNum",
    "CompAuxLib",
    "PieceRef",
    "PieceDate",
    "EcritureLib",
    "Debit",
    "Credit",
]


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


def format_decimal_fr(value: Decimal) -> str:
    return f"{value:.2f}".replace(".", ",")


def validate_fec_columns(df: pd.DataFrame):
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError("Colonnes obligatoires manquantes : " + ", ".join(missing))


def prepare_work_dataframe(df: pd.DataFrame, detail_auxiliaire: bool = True) -> pd.DataFrame:
    validate_fec_columns(df)

    work = df.copy()
    work["DebitDecimal"] = work["Debit"].apply(clean_amount)
    work["CreditDecimal"] = work["Credit"].apply(clean_amount)

    work["CompteNum"] = work["CompteNum"].astype(str).str.strip()
    work["CompteLib"] = work["CompteLib"].astype(str).str.strip()

    has_aux = all(col in work.columns for col in AUX_COLUMNS)

    if detail_auxiliaire and has_aux:
        work["CompAuxNum"] = work["CompAuxNum"].astype(str).str.strip()
        work["CompAuxLib"] = work["CompAuxLib"].astype(str).str.strip()
    else:
        work["CompAuxNum"] = ""
        work["CompAuxLib"] = ""

    return work


def build_balance(df: pd.DataFrame, detail_auxiliaire: bool = True) -> pd.DataFrame:
    work = prepare_work_dataframe(df, detail_auxiliaire=detail_auxiliaire)

    if detail_auxiliaire:
        # Si auxiliaire présent : on détaille le compte par auxiliaire.
        # Exemple : 401000-FOURN001 / FOURNISSEUR X, 411000-CLIENT001 / CLIENT Y, etc.
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

    grouped["Solde"] = grouped["Debit"] - grouped["Credit"]
    grouped["Débit"] = grouped["Solde"].apply(lambda x: x if x > 0 else Decimal("0"))
    grouped["Crédit"] = grouped["Solde"].apply(lambda x: -x if x < 0 else Decimal("0"))

    balance = grouped[["CompteFinal", "LibelleFinal", "Débit", "Crédit"]].copy()
    balance.columns = ["compte", "libellé", "débit", "crédit"]

    balance = balance.sort_values("compte", kind="stable").reset_index(drop=True)

    for col in ["débit", "crédit"]:
        balance[col] = balance[col].apply(format_decimal_fr)

    return balance


def build_an_acd(
    df: pd.DataFrame,
    detail_auxiliaire: bool = True,
    journal_code: str = "AN",
    journal_lib: str = "A nouveaux",
    an_date: str = "20260101",
    ecriture_num: str = "AN",
    piece_ref: str = "AN",
    ecriture_lib: str = "A nouveaux",
) -> pd.DataFrame:
    work = prepare_work_dataframe(df, detail_auxiliaire=detail_auxiliaire)

    # Les à-nouveaux reprennent uniquement les comptes de bilan.
    # Les comptes 6 et 7 sont exclus car soldés par le résultat.
    work = work[work["CompteNum"].str.match(r"^[1-5]")].copy()

    if work.empty:
        return pd.DataFrame(columns=ACD_AN_COLUMNS)

    group_cols = ["CompteNum", "CompteLib"]
    if detail_auxiliaire:
        group_cols += ["CompAuxNum", "CompAuxLib"]

    grouped = (
        work.groupby(group_cols, dropna=False, as_index=False)
        .agg(DebitTotal=("DebitDecimal", "sum"), CreditTotal=("CreditDecimal", "sum"))
    )

    grouped["Solde"] = grouped["DebitTotal"] - grouped["CreditTotal"]

    # Suppression des comptes soldés.
    grouped = grouped[grouped["Solde"] != Decimal("0")].copy()

    grouped["Debit"] = grouped["Solde"].apply(lambda x: x if x > 0 else Decimal("0"))
    grouped["Credit"] = grouped["Solde"].apply(lambda x: -x if x < 0 else Decimal("0"))

    # Sécurité : si pas de détail auxiliaire, on conserve les colonnes vides.
    if "CompAuxNum" not in grouped.columns:
        grouped["CompAuxNum"] = ""
    if "CompAuxLib" not in grouped.columns:
        grouped["CompAuxLib"] = ""

    an = pd.DataFrame({
        "JournalCode": journal_code,
        "JournalLib": journal_lib,
        "EcritureNum": ecriture_num,
        "EcritureDate": an_date,
        "CompteNum": grouped["CompteNum"],
        "CompteLib": grouped["CompteLib"],
        "CompAuxNum": grouped["CompAuxNum"],
        "CompAuxLib": grouped["CompAuxLib"],
        "PieceRef": piece_ref,
        "PieceDate": an_date,
        "EcritureLib": ecriture_lib,
        "Debit": grouped["Debit"].apply(format_decimal_fr),
        "Credit": grouped["Credit"].apply(format_decimal_fr),
    })

    an = an[ACD_AN_COLUMNS]
    an = an.sort_values(["CompteNum", "CompAuxNum"], kind="stable").reset_index(drop=True)

    return an


def dataframe_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False, sep=";", encoding="utf-8-sig").encode("utf-8-sig")


def dataframe_to_tsv_text(df: pd.DataFrame, header: bool = True) -> str:
    return df.to_csv(index=False, sep="\t", header=header, lineterminator="\n")


def guess_next_year_an_date(df: pd.DataFrame) -> str:
    if "EcritureDate" not in df.columns:
        return "20260101"

    dates = df["EcritureDate"].astype(str).str.replace(r"[^0-9]", "", regex=True)
    years = dates.str[:4]
    years = years[years.str.match(r"^20[0-9]{2}$", na=False)]

    if years.empty:
        return "20260101"

    next_year = int(years.max()) + 1
    return f"{next_year}0101"


st.set_page_config(page_title="FEC vers balance comptable", layout="wide")

st.title("Convertisseur FEC → Balance comptable + AN ACD")
st.caption("Balance : compte ; libellé ; débit ; crédit — AN : format tabulé copiable-collable")

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

        guessed_an_date = guess_next_year_an_date(fec)

        with st.expander("Paramètres des à-nouveaux ACD", expanded=True):
            col_a, col_b, col_c = st.columns(3)
            with col_a:
                journal_code = st.text_input("Journal", value="AN")
                ecriture_num = st.text_input("N° écriture", value="AN")
            with col_b:
                journal_lib = st.text_input("Libellé journal", value="A nouveaux")
                piece_ref = st.text_input("Pièce", value="AN")
            with col_c:
                an_date = st.text_input("Date AN AAAAMMJJ", value=guessed_an_date)
                ecriture_lib = st.text_input("Libellé écriture", value="A nouveaux")

        with st.expander("Aperçu du FEC importé"):
            st.dataframe(fec.head(50), use_container_width=True)

        tab_balance, tab_an = st.tabs(["Balance", "AN ACD"])

        with tab_balance:
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

        with tab_an:
            an_acd = build_an_acd(
                fec,
                detail_auxiliaire=detail_auxiliaire,
                journal_code=journal_code,
                journal_lib=journal_lib,
                an_date=an_date,
                ecriture_num=ecriture_num,
                piece_ref=piece_ref,
                ecriture_lib=ecriture_lib,
            )

            debit_an = an_acd["Debit"].str.replace(",", ".", regex=False).astype(float).sum() if not an_acd.empty else 0
            credit_an = an_acd["Credit"].str.replace(",", ".", regex=False).astype(float).sum() if not an_acd.empty else 0
            ecart_an = round(debit_an - credit_an, 2)

            col1, col2, col3 = st.columns(3)
            col1.metric("Total débit AN", f"{debit_an:,.2f}".replace(",", " ").replace(".", ","))
            col2.metric("Total crédit AN", f"{credit_an:,.2f}".replace(",", " ").replace(".", ","))
            col3.metric("Écart AN", f"{ecart_an:,.2f}".replace(",", " ").replace(".", ","))

            if abs(ecart_an) > 0.01:
                st.warning(
                    "Les AN ne sont pas équilibrés. C'est souvent normal si le résultat n'a pas été affecté. "
                    "Ajoute ou contrôle le compte de résultat / report à nouveau avant import."
                )
            else:
                st.success("AN équilibrés.")

            st.subheader("Écritures AN au format ACD")
            st.dataframe(an_acd, use_container_width=True)

            st.download_button(
                label="Télécharger les AN CSV",
                data=dataframe_to_csv_bytes(an_acd),
                file_name="an_acd_depuis_fec.csv",
                mime="text/csv",
            )

            st.subheader("Bloc copiable-collable dans Excel / ACD")
            include_header = st.checkbox("Inclure les en-têtes dans le bloc copiable", value=True)
            tsv_text = dataframe_to_tsv_text(an_acd, header=include_header)
            st.text_area(
                "Copie ce bloc",
                value=tsv_text,
                height=350,
            )

    except Exception as exc:
        st.error(str(exc))
else:
    st.info("Charge un fichier FEC pour générer la balance et les AN.")
