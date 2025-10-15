
import os
import sys
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt

# ---------- Config ----------
CSV_IN         = "sales_data_sample.csv"
XLSX_OUT_RAW   = "sales_data_sample.xlsx"
CSV_CLEAN      = "sales_data_sample_clean.csv"
XLSX_CLEAN     = "sales_data_sample_clean.xlsx"
CONCLUSIONES   = "conclusiones.txt"
TOP_N          = 10

COL_PRODUCT    = "PRODUCTCODE"
COL_LINE       = "PRODUCTLINE"
COL_QTY        = "QUANTITYORDERED"
COL_PRICE      = "PRICEEACH"
COL_SALES      = "SALES"
COL_ORDERDATE  = "ORDERDATE"

# ---------- Utilidades ----------

def log(msg: str):
    """Imprime en consola y agrega al archivo de conclusiones con timestamp (modo append)."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(CONCLUSIONES, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def ensure_columns(df: pd.DataFrame, required: list):
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas requeridas: {missing}")

# ---------- Etapas del pipeline ----------

def load_csv(file_path: str) -> pd.DataFrame:
    if not os.path.exists(file_path):
        print(f"ERROR: no se encontró {file_path} en la carpeta actual: {os.getcwd()}")
        sys.exit(1)
    df = pd.read_csv(file_path, encoding="latin1")
    log(f"Archivo CSV leído correctamente: {file_path} (filas={len(df)})")
    return df


def export_raw_excel(df: pd.DataFrame, output_path: str):
    df.to_excel(output_path, index=False)
    log(f"Exportado Excel RAW → {output_path} (filas={len(df)})")


def report_nulls(df: pd.DataFrame):
    nulls = df.isnull().sum().sort_values(ascending=False)
    nulls.to_csv("nulls_por_columna.csv", header=["nulos"])
    log("Valores nulos por columna (TOP 10):")
    for col, n in nulls.head(10).items():
        log(f"  - {col}: {n}")
    log("Guardado reporte de nulos → nulls_por_columna.csv")


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    # Duplicados
    dup_antes = len(df)
    df = df.drop_duplicates()
    log(f"Duplicados eliminados: {dup_antes - len(df)}")

    # Trim strings
    for c in df.select_dtypes(include=["object"]).columns:
        df[c] = df[c].astype(str).str.strip()

    # Tipos numéricos
    for c in [COL_QTY, COL_PRICE, COL_SALES]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Calcular SALES si no existe
    if COL_SALES not in df.columns and all(c in df.columns for c in [COL_QTY, COL_PRICE]):
        df[COL_SALES] = df[COL_QTY] * df[COL_PRICE]
        log("Columna SALES no existía; se calculó como QUANTITYORDERED * PRICEEACH.")

    # Filas inválidas
    before_drop = len(df)
    df = df.dropna(subset=[COL_PRODUCT])
    if COL_QTY in df.columns:
        df = df[df[COL_QTY].notna() & (df[COL_QTY] > 0)]
    log(f"Filas eliminadas por producto/cantidad inválida: {before_drop - len(df)}")

    return df


def top_products(df: pd.DataFrame, n: int):
    ensure_columns(df, [COL_PRODUCT])
    results = {}

    if COL_QTY in df.columns:
        top_qty = (
            df.groupby(COL_PRODUCT)[COL_QTY]
              .sum()
              .sort_values(ascending=False)
              .head(n)
        )
        log(f"Top {n} productos por unidades:")
        for p, v in top_qty.items():
            log(f"  - {p}: {int(v)} unidades")
        results["qty"] = top_qty

    if COL_SALES in df.columns:
        top_sales = (
            df.groupby(COL_PRODUCT)[COL_SALES]
              .sum()
              .sort_values(ascending=False)
              .head(n)
        )
        log(f"Top {n} productos por ventas ($):")
        for p, v in top_sales.items():
            log(f"  - {p}: ${float(v):,.2f}")
        results["sales"] = top_sales

    return results


def plot_top_products(top_qty, top_sales, n: int):
    os.makedirs("charts", exist_ok=True)

    if top_qty is not None and len(top_qty) > 0:
        plt.figure(figsize=(10, 6))
        top_qty.plot(kind="bar")
        plt.title(f"Top {n} productos por unidades")
        plt.xlabel("Producto")
        plt.ylabel("Unidades vendidas")
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        path = f"charts/top_{n}_productos_unidades.png"
        plt.savefig(path, dpi=150)
        plt.close()
        log(f"Gráfico guardado → {path}")

    if top_sales is not None and len(top_sales) > 0:
        plt.figure(figsize=(10, 6))
        top_sales.plot(kind="bar")
        plt.title(f"Top {n} productos por ventas ($)")
        plt.xlabel("Producto")
        plt.ylabel("Ventas ($)")
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        path = f"charts/top_{n}_productos_ventas.png"
        plt.savefig(path, dpi=150)
        plt.close()
        log(f"Gráfico guardado → {path}")


def save_clean_dataset(df: pd.DataFrame):
    df.to_csv(CSV_CLEAN, index=False)
    df.to_excel(XLSX_CLEAN, index=False)
    log(f"Dataset limpio guardado → {CSV_CLEAN} y {XLSX_CLEAN} (filas={len(df)})")


def analyze_top4_daily(df: pd.DataFrame, top_sales):
    if top_sales is None or len(top_sales) < 4:
        return

    top4 = top_sales.head(4).index.tolist()
    if COL_ORDERDATE not in df.columns:
        log("ORDERDATE no existe; se omite análisis diario de top 4.")
        return

    df[COL_ORDERDATE] = pd.to_datetime(df[COL_ORDERDATE], errors="coerce")
    df_top4 = df[df[COL_PRODUCT].isin(top4) & df[COL_ORDERDATE].notna()]

    ventas_diarias = (
        df_top4.groupby([df_top4[COL_ORDERDATE].dt.date, COL_PRODUCT])[COL_SALES]
        .sum()
        .reset_index()
        .rename(columns={COL_ORDERDATE: "DATE", COL_PRODUCT: "PRODUCT", COL_SALES: "SALES"})
    )
    ventas_diarias.to_csv("ventas_diarias_top4.csv", index=False)
    log("Guardado CSV → ventas_diarias_top4.csv")

    os.makedirs("charts", exist_ok=True)
    # Gráfico comparativo
    plt.figure(figsize=(12, 6))
    for prod in top4:
        subset = ventas_diarias[ventas_diarias["PRODUCT"] == prod]
        plt.plot(subset["DATE"], subset["SALES"], marker="o", label=prod)
    plt.title("Ventas diarias - Top 4 productos ($)")
    plt.xlabel("Fecha")
    plt.ylabel("Ventas ($)")
    plt.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("charts/ventas_diarias_top4_comparativo.png", dpi=150)
    plt.close()
    log("Gráfico comparativo guardado → charts/ventas_diarias_top4_comparativo.png")

    # Gráficos individuales
    for prod in top4:
        subset = ventas_diarias[ventas_diarias["PRODUCT"] == prod]
        plt.figure(figsize=(10, 5))
        plt.plot(subset["DATE"], subset["SALES"], marker="o")
        plt.title(f"Ventas diarias - {prod}")
        plt.xlabel("Fecha")
        plt.ylabel("Ventas ($)")
        plt.xticks(rotation=45)
        plt.tight_layout()
        fname = f"charts/ventas_diarias_{prod}.png"
        plt.savefig(fname, dpi=150)
        plt.close()
        log(f"Gráfico individual guardado → {fname}")


# ---------- Función principal ----------

def main():
    df = load_csv(CSV_IN)
    export_raw_excel(df, XLSX_OUT_RAW)
    report_nulls(df)
    df = clean_data(df)
    tops = top_products(df, TOP_N)
    plot_top_products(tops.get("qty"), tops.get("sales"), TOP_N)
    save_clean_dataset(df)
    analyze_top4_daily(df, tops.get("sales"))
    log("Análisis completado ✅")


if __name__ == "__main__":
    main()


