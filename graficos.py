import pandas as pd
import matplotlib.pyplot as plt
import calendar
import os

def cantidad_unidades_por_mes(df):
    # Definir las líneas de producto de interés
    lineas_interes = ['Vintage Cars', 'Classic Cars', 'Trucks and Buses', 'Motorcycles']

    # ---  Preparación de Datos ---

    #  Convertir ORDERDATE a datetime (DD/MM/YYYY) y extraer el mes
    df['ORDERDATE'] = pd.to_datetime(df['ORDERDATE'], dayfirst=True) 
    df['MONTH_ID'] = df['ORDERDATE'].dt.month # Extraemos el número del mes

    # Filtrar solo las líneas de producto deseadas
    df_filtrado = df[df['PRODUCTLINE'].isin(lineas_interes)].copy()

    #Convertir QUANTITYORDERED a tipo numérico, por si acaso
    df_filtrado['QUANTITYORDERED'] = pd.to_numeric(df_filtrado['QUANTITYORDERED'], errors='coerce')
    df_filtrado.dropna(subset=['QUANTITYORDERED'], inplace=True)

    # Agrupar y sumar la CANTIDAD VENDIDA (QUANTITYORDERED) solo por Mes y Línea de Producto
    cantidad_mensual = df_filtrado.groupby(['MONTH_ID', 'PRODUCTLINE'])['QUANTITYORDERED'].sum().reset_index()

    # Añadir el nombre del mes para las etiquetas del gráfico
    cantidad_mensual['MONTH_NAME'] = cantidad_mensual['MONTH_ID'].apply(lambda x: calendar.month_name[x])

    # Ordenar por el ID del Mes para una secuencia correcta (Enero, Febrero, etc.)
    cantidad_mensual = cantidad_mensual.sort_values(by='MONTH_ID')


    # --- Creación de 4 Gráficos Individuales ---

    # Inicializar una figura grande para mostrar todos juntos si lo deseas, o simplemente generar individuales.
    # Para 4 gráficos separados:

    for producto in lineas_interes:
        datos_producto = cantidad_mensual[cantidad_mensual['PRODUCTLINE'] == producto]
        
        # Solo graficar si hay datos para el producto
        if not datos_producto.empty:
            plt.figure(figsize=(10, 6)) # Crea una NUEVA FIGURA para cada producto
            
            # Graficamos MONTH_NAME vs. QUANTITYORDERED
            plt.bar(
                datos_producto['MONTH_NAME'], 
                datos_producto['QUANTITYORDERED'], 
                color="#b41f1f"
            )

            # Personalizar el Gráfico
            plt.title(f'Cantidad de Unidades Vendidas por Mes (Acumulado) - {producto}', fontsize=14)
            plt.xlabel('Mes', fontsize=12)
            plt.ylabel('Cantidad de Unidades Vendidas', fontsize=12)
            
            # Rotar etiquetas del eje X para que sean legibles
            plt.xticks(rotation=45, ha='right')
            plt.grid(axis='y', linestyle='--', alpha=0.7)
            plt.tight_layout()
            
            # Guardar el gráfico
            nombre_archivo = f'cantidad_unidades_por_mes_{producto.replace(" ", "_").lower()}.png'
            plt.savefig(nombre_archivo)
            plt.close() # Cierra la figura
            
            print(f"Gráfico de barras '{nombre_archivo}' generado.")
            
def plot_cantidad_por_territorio(df):
    """
    Genera un gráfico de barras comparativo de la cantidad de unidades vendidas 
    por Territorio, filtrando solo 'Vintage Cars' y 'Classic Cars'.
    """
    lineas_interes = ['Vintage Cars', 'Classic Cars']

    # Filtrar las líneas de producto de interés
    df_filtrado = df[df['PRODUCTLINE'].isin(lineas_interes)].copy()
    
    # Convertir a numérico y agrupar la cantidad pedida por Territorio y Producto
    df_filtrado['QUANTITYORDERED'] = pd.to_numeric(df_filtrado['QUANTITYORDERED'], errors='coerce')
    
    ventas_territorio = df_filtrado.groupby(['TERRITORY', 'PRODUCTLINE'])['QUANTITYORDERED'].sum().unstack(fill_value=0)
    
    # Crear el gráfico de barras agrupadas
    plt.figure(figsize=(12, 7))
    ventas_territorio.plot(kind='bar', ax=plt.gca(), cmap='Pastel2')
    
    # Personalización
    plt.title('Cantidad Total de Unidades Vendidas por Territorio', fontsize=16)
    plt.xlabel('Territorio', fontsize=12)
    plt.ylabel('Cantidad de Unidades Vendidas', fontsize=12)
    plt.xticks(rotation=45, ha='right')
    plt.legend(title='Línea de Producto')
    plt.grid(axis='y', linestyle='--', alpha=0.6)
    plt.tight_layout()
    plt.show()

def plot_boxplot_precio(df):
    """
    Genera un Box Plot (diagrama de caja) comparando la distribución del 
    precio de venta por unidad ('PRICEEACH') entre 'Vintage Cars' y 'Classic Cars'.
    """
    lineas_interes = ['Vintage Cars', 'Classic Cars']
    
    # Filtrar las líneas de producto y columnas necesarias
    df_filtrado = df[df['PRODUCTLINE'].isin(lineas_interes)].copy()
    
    # Asegurar que el precio es numérico y eliminar valores nulos/cero para la gráfica
    df_filtrado['PRICEEACH'] = pd.to_numeric(df_filtrado['PRICEEACH'], errors='coerce')
    df_filtrado = df_filtrado.dropna(subset=['PRICEEACH'])
    
    # Crear la lista de datos a graficar (un array por categoría)
    datos_boxplot = [
        df_filtrado[df_filtrado['PRODUCTLINE'] == 'Vintage Cars']['PRICEEACH'],
        df_filtrado[df_filtrado['PRODUCTLINE'] == 'Classic Cars']['PRICEEACH']
    ]
    
    # rear el Box Plot
    plt.figure(figsize=(8, 6))
    plt.boxplot(datos_boxplot, labels=lineas_interes, patch_artist=True)
    
    # Personalización
    plt.title('Distribución del Precio de Venta (PRICEEACH)', fontsize=16)
    plt.ylabel('Precio de Venta por Unidad ($)', fontsize=12)
    plt.xlabel('Línea de Producto', fontsize=12)
    plt.grid(axis='y', linestyle='--', alpha=0.6)
    plt.tight_layout()
    plt.show()


