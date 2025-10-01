
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

# Columnas esperadas (dataset clásico de ventas muestra/productos)
COL_PRODUCT    = "PRODUCTCODE"
COL_QTY        = "QUANTITYORDERED"
COL_PRICE      = "PRICEEACH"
COL_SALES      = "SALES"           # Si no existe, se calculará como qty*price
COL_ORDERDATE  = "ORDERDATE"       # Usada para series diarias

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

def main():
    if not os.path.exists(CSV_IN):
        print(f"ERROR: no se encontró {CSV_IN} en la carpeta actual: {os.getcwd()}")
        sys.exit(1)

    # 1) Leer CSV (si te falla la codificación probá encoding="utf-8")
    df = pd.read_csv(CSV_IN, encoding="latin1")

    # 2) Exportar CSV a Excel (RAW)
    df.to_excel(XLSX_OUT_RAW, index=False)
    log(f"Exportado Excel RAW → {XLSX_OUT_RAW} (filas={len(df)})")

    # 3) Nulos
    nulls = df.isnull().sum().sort_values(ascending=False)
    log("Valores nulos por columna (TOP 10 con más nulos):")
    for col, n in nulls.head(10).items():
        log(f"  - {col}: {n}")
    nulls.to_csv("nulls_por_columna.csv", header=["nulos"])
    log("Guardado reporte de nulos → nulls_por_columna.csv")

    # 4) Limpieza básica
    # 4.1) Duplicados
    dup_antes = len(df)
    df = df.drop_duplicates()
    dup_eliminados = dup_antes - len(df)
    log(f"Duplicados eliminados: {dup_eliminados}")

    # 4.2) Trim strings
    str_cols = df.select_dtypes(include=["object"]).columns
    for c in str_cols:
        df[c] = df[c].astype(str).str.strip()

    # 4.3) Tipos numéricos
    for c in [COL_QTY, COL_PRICE, COL_SALES]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # 4.4) Calcular SALES si no existe
    if COL_SALES not in df.columns and all(c in df.columns for c in [COL_QTY, COL_PRICE]):
        df[COL_SALES] = df[COL_QTY] * df[COL_PRICE]
        log("Columna SALES no existía; se calculó como QUANTITYORDERED * PRICEEACH.")

    # 4.5) Filas inválidas (producto vacío o cantidad <= 0)
    before_drop = len(df)
    df = df.dropna(subset=[COL_PRODUCT])
    if COL_QTY in df.columns:
        df = df[df[COL_QTY].notna() & (df[COL_QTY] > 0)]
    dropped = before_drop - len(df)
    log(f"Filas eliminadas por producto/cantidad inválida: {dropped}")

    # 5) Top productos
    ensure_columns(df, [COL_PRODUCT])

    top_qty = None
    if COL_QTY in df.columns:
        top_qty = (
            df.groupby(COL_PRODUCT)[COL_QTY]
              .sum()
              .sort_values(ascending=False)
              .head(TOP_N)
        )
        log(f"Top {TOP_N} productos por unidades:")
        for p, v in top_qty.items():
            log(f"  - {p}: {int(v)} unidades")

    top_sales = None
    if COL_SALES in df.columns:
        top_sales = (
            df.groupby(COL_PRODUCT)[COL_SALES]
              .sum()
              .sort_values(ascending=False)
              .head(TOP_N)
        )
        log(f"Top {TOP_N} productos por ventas ($):")
        for p, v in top_sales.items():
            log(f"  - {p}: ${float(v):,.2f}")

    # 6) Gráficos Top N (unidades y ventas)
    os.makedirs("charts", exist_ok=True)

    if top_qty is not None and len(top_qty) > 0:
        plt.figure(figsize=(10, 6))
        top_qty.plot(kind="bar")
        plt.title(f"Top {TOP_N} productos por unidades")
        plt.xlabel("Producto")
        plt.ylabel("Unidades vendidas")
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        qty_png = f"charts/top_{TOP_N}_productos_unidades.png"
        plt.savefig(qty_png, dpi=150)
        plt.close()
        log(f"Gráfico guardado → {qty_png}")

    if top_sales is not None and len(top_sales) > 0:
        plt.figure(figsize=(10, 6))
        top_sales.plot(kind="bar")
        plt.title(f"Top {TOP_N} productos por ventas ($)")
        plt.xlabel("Producto")
        plt.ylabel("Ventas ($)")
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        sales_png = f"charts/top_{TOP_N}_productos_ventas.png"
        plt.savefig(sales_png, dpi=150)
        plt.close()
        log(f"Gráfico guardado → {sales_png}")

    # 7) Guardar dataset limpio
    df.to_csv(CSV_CLEAN, index=False)
    df.to_excel(XLSX_CLEAN, index=False)
    log(f"Dataset limpio guardado → {CSV_CLEAN} y {XLSX_CLEAN} (filas={len(df)})")

    # 8) Análisis diario de los 4 productos con más ventas ($)
    if top_sales is not None and len(top_sales) >= 4:
        top4 = top_sales.head(4).index.tolist()
        # Convertir a fecha
        if COL_ORDERDATE in df.columns:
            df[COL_ORDERDATE] = pd.to_datetime(df[COL_ORDERDATE], errors="coerce")
        else:
            log("ORDERDATE no existe; se omite análisis diario de top 4.")
            print("No se encontró ORDERDATE; fin.")
            return

        # Filtrar top 4
        df_top4 = df[df[COL_PRODUCT].isin(top4)].copy()
        df_top4 = df_top4[df_top4[COL_ORDERDATE].notna()]

        # Agrupar por día y producto
        ventas_diarias = (
            df_top4.groupby([df_top4[COL_ORDERDATE].dt.date, COL_PRODUCT])[COL_SALES]
            .sum()
            .reset_index()
            .rename(columns={COL_ORDERDATE: "DATE", COL_PRODUCT: "PRODUCT", COL_SALES: "SALES"})
        )

        ventas_diarias.to_csv("ventas_diarias_top4.csv", index=False)
        log("Guardado CSV → ventas_diarias_top4.csv")

        # Gráfico comparativo: los 4 productos juntos (una curva por producto)
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
        comp_png = "charts/ventas_diarias_top4_comparativo.png"
        plt.savefig(comp_png, dpi=150)
        plt.close()
        log(f"Gráfico comparativo guardado → {comp_png}")

        # Gráfico individual por producto
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

    log("Análisis completado ✅")

if __name__ == "__main__":
    main()
