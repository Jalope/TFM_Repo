import requests
import pandas as pd

def obtener_tabla_ine(tabla_id, nult=None, tip=None, filtros=None):
    """
    Devuelve un DataFrame con todos los registros de la tabla INE indicada.
    - tabla_id: entero (p.ej. 6507)
    - nult: (opcional) número de últimos registros
    - tip: (opcional) tipo de frecuencia (p.ej. 'A', 'AM')
    - filtros: (opcional) lista de tuplas [('var1','val1'), ('var2','val2'), …]
    """
    base = f"https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/{tabla_id}"
    params = {}
    if nult is not None:   params['nult'] = nult
    if tip   is not None:   params['tip']  = tip
    if filtros:
        for var, val in filtros:
            params.setdefault('tv', []).append(f"{var}:{val}")

    resp = requests.get(base, params=params)
    resp.raise_for_status()
    jd = resp.json()

    # extraer nombres de columnas
    cols = [c['VAR_NAME'] for c in jd['COLUMNS']]
    # volcar cada fila de datos en un dict
    registros = [dict(zip(cols, fila)) for fila in jd['Data']]
    return pd.DataFrame(registros)

# Ejemplo de uso:
#df6507 = obtener_tabla_ine(6507)
#print(df6507.head())