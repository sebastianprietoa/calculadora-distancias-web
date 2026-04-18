from __future__ import annotations

import time
from io import BytesIO
from pathlib import Path

import pandas as pd
import streamlit as st

from app.services.coordenadas_service import CoordenadasService
from app.utils.excel import dataframe_to_excel_bytes, validate_extension


st.set_page_config(page_title="Calculadora de Distancias - Coordenadas", layout="wide")
st.title("Calculadora de Distancias · Coordenadas")
st.caption("UI Streamlit para validación previa y descarga controlada de resultados.")

service = CoordenadasService()


def read_uploaded_file(uploaded_file) -> pd.DataFrame:
    suffix = validate_extension(uploaded_file.name)
    content = uploaded_file.getvalue()
    if not content:
        raise ValueError("El archivo está vacío")

    if suffix == ".csv":
        return pd.read_csv(BytesIO(content))
    return pd.read_excel(BytesIO(content))


def render_initial_panels() -> tuple:
    st.subheader("Estado inicial")
    col1, col2, col3 = st.columns(3)
    col1.metric("Progreso", "0%")
    col2.metric("Tiempo estimado", "--")
    col3.metric("Estado", "Esperando archivo")

    st.subheader("Alertas pre-descarga")
    st.info("Aún no hay alertas. Sube un archivo y pulsa 'Validar archivo (sin descargar)'.")

    st.subheader("Resumen")
    summary_cols = st.columns(6)
    labels = ["Total", "OK", "No encontrado", "Faltan datos", "Errores", "Precisión promedio"]
    for col, label in zip(summary_cols, labels):
        col.metric(label, "--")

    progress_bar = st.progress(0, text="Progreso: 0%")
    eta_placeholder = st.empty()
    eta_placeholder.caption("Tiempo estimado restante: --")
    return progress_bar, eta_placeholder


def run_validation(df: pd.DataFrame, progress_bar, eta_placeholder) -> pd.DataFrame:
    estimated_seconds = max(3.0, min(30.0, len(df) / 8 if len(df) else 3.0))
    steps = 10
    for idx in range(steps):
        pct = int(((idx + 1) / steps) * 100)
        remaining = max(0.0, estimated_seconds - ((idx + 1) * (estimated_seconds / steps)))
        progress_bar.progress(pct, text=f"Progreso: {pct}%")
        eta_placeholder.caption(f"Tiempo estimado restante: {remaining:.1f}s")
        time.sleep(0.05)

    return service.process(df)


def render_results(result_df: pd.DataFrame) -> None:
    summary = {
        "total": int(len(result_df)),
        "ok": int((result_df["Estado"] == "OK").sum()),
        "not_found": int((result_df["Estado"] == "NO ENCONTRADO").sum()),
        "missing": int((result_df["Estado"] == "FALTAN DATOS").sum()),
        "errors": int(result_df["Estado"].astype(str).str.startswith("ERROR").sum()),
        "precision": round(float(result_df.get("Precision_pct", 0).fillna(0).mean()), 2) if len(result_df) else 0.0,
    }

    st.subheader("Resumen de validación")
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Total", summary["total"])
    c2.metric("OK", summary["ok"])
    c3.metric("No encontrado", summary["not_found"])
    c4.metric("Faltan datos", summary["missing"])
    c5.metric("Errores", summary["errors"])
    c6.metric("Precisión promedio", f"{summary['precision']}%")

    issue_count = summary["not_found"] + summary["missing"] + summary["errors"]
    problem_df = result_df[result_df["Estado"] != "OK"]

    st.subheader("Alertas pre-descarga")
    allow_download = True
    if issue_count > 0:
        st.warning(f"Se detectaron {issue_count} filas con observaciones antes de descargar.")
        st.dataframe(problem_df.head(100), use_container_width=True)
        allow_download = st.checkbox("Entendido, descargar de todos modos")
    else:
        st.success("Sin alertas: puedes descargar el resultado.")

    st.subheader("Vista previa de resultados")
    st.dataframe(result_df.head(200), use_container_width=True)

    excel_bytes = dataframe_to_excel_bytes(result_df, sheet_name="coordenadas_output")
    st.download_button(
        label="Descargar resultado XLSX",
        data=excel_bytes,
        file_name="coordenadas_output.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        disabled=not allow_download,
    )


st.markdown("### Cargar archivo")
uploaded_file = st.file_uploader("Sube .xlsx o .csv con columnas Ciudad y País", type=["xlsx", "csv"])
validate_click = st.button("Validar archivo (sin descargar)")

progress_bar, eta_placeholder = render_initial_panels()

if validate_click:
    if uploaded_file is None:
        st.error("Selecciona un archivo antes de validar.")
    else:
        try:
            df = read_uploaded_file(uploaded_file)
            result_df = run_validation(df, progress_bar, eta_placeholder)
            render_results(result_df)
        except ValueError as exc:
            st.error(str(exc))
        except Exception as exc:
            st.error(f"Error interno procesando archivo: {exc}")

st.markdown("---")
st.caption("Ejecución local: `streamlit run streamlit_app.py`")
