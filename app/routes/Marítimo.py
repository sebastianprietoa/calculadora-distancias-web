import pandas as pd
import unicodedata
import re
from rapidfuzz.process import extractOne
from rapidfuzz.fuzz import ratio

def normalizar_texto(texto):
    if isinstance(texto, str):
        texto = texto.lower()
        texto = unicodedata.normalize("NFKD", texto).encode("ASCII", "ignore").decode("utf-8")
        texto = re.sub(r"\s+", "", texto)  # Eliminar espacios
        return texto
    return texto

def encontrar_distancia_mas_cercana(origen_ciudad, destino_ciudad, df_distancias):
    # Normalizar claves
    clave_busqueda_primaria = normalizar_texto(f"{origen_ciudad} -> {destino_ciudad}")
    clave_busqueda_secundaria = normalizar_texto(destino_ciudad)  # Solo ciudad destino
    
    # Crear claves en el dataframe de distancias
    df_distancias["Clave"] = df_distancias.apply(lambda row: normalizar_texto(f"{row['Ciudad Origen']} -> {row['Ciudad Destino']}"), axis=1)
    df_distancias["Clave Secundaria"] = df_distancias["Ciudad Destino"].apply(normalizar_texto)
    
    # Encontrar la clave más cercana usando RapidFuzz
    coincidencia = extractOne(clave_busqueda_primaria, df_distancias["Clave"], scorer=ratio)
    tipo_coincidencia = "Primaria"
    
    if coincidencia:
        clave_encontrada, score, _ = coincidencia
        if score < 90:
            # Si la similitud es menor a 90, buscar solo por la ciudad de destino
            coincidencia_secundaria = extractOne(clave_busqueda_secundaria, df_distancias["Clave Secundaria"], scorer=ratio)
            if coincidencia_secundaria:
                clave_encontrada, score, _ = coincidencia_secundaria
                clave_encontrada = df_distancias.loc[df_distancias["Clave Secundaria"] == clave_encontrada, "Clave"].values[0]
                tipo_coincidencia = "Secundaria"
        
        distancia = df_distancias.loc[df_distancias["Clave"] == clave_encontrada, "Distancia"].values[0]
        nivel_coincidencia = round(score, 2)  # Mantener precisión con dos decimales
        return distancia, clave_encontrada, nivel_coincidencia, tipo_coincidencia
    else:
        return None, None, None, None

def completar_distancias():
    # Definir la ruta base de los archivos
    ruta_base = "D:\\GREEN_TICKET\\Distancias\\"
    
    # Leer el archivo de distancias desde la carpeta especificada
    ruta_distancias = ruta_base + "Distancias.xlsx"
    df_distancias = pd.read_excel(ruta_distancias, sheet_name="Maritima")
    
    # Leer el archivo de cálculo de distancias desde la misma carpeta
    ruta_calculo = ruta_base + "Calculo de distancias.xlsx"
    df_calculo = pd.read_excel(ruta_calculo)
    
    # Asegurar que las columnas tengan los nombres correctos
    columnas = ["Ciudad Origen", "Ciudad Destino"]
    
    if not all(col in df_calculo.columns for col in columnas):
        raise ValueError("Las columnas del archivo de cálculo de distancias no coinciden con las esperadas")
    
    # Inicializar nuevas columnas
    df_calculo["Distancia"] = None
    df_calculo["Mejor coincidencia"] = None
    df_calculo["Nivel de coincidencia"] = None
    df_calculo["Tipo de coincidencia"] = None
    
    # Iterar sobre las filas del archivo de cálculo de distancias
    for index, row in df_calculo.iterrows():
        distancia, clave_encontrada, nivel_coincidencia, tipo_coincidencia = encontrar_distancia_mas_cercana(row["Ciudad Origen"], row["Ciudad Destino"], df_distancias)
        
        df_calculo.at[index, "Distancia"] = distancia
        df_calculo.at[index, "Mejor coincidencia"] = clave_encontrada
        df_calculo.at[index, "Nivel de coincidencia"] = nivel_coincidencia
        df_calculo.at[index, "Tipo de coincidencia"] = tipo_coincidencia
    
    # Guardar el resultado en un nuevo archivo en la misma carpeta
    ruta_resultado = ruta_base + "Calculo_de_distancias_completado.xlsx"
    df_calculo.to_excel(ruta_resultado, index=False)
    print(f"Proceso completado. Archivo guardado como '{ruta_resultado}'")

# Ejecutar la función
completar_distancias()
