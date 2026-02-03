# Reporte de Evaluación de Código

He analizado los archivos `matomo2parquet.py`, `s3parquet2elastic.py` y los componentes en el directorio `stages/`. A continuación presento mis hallazgos y recomendaciones.

---

## 1. matomo2parquet.py

Este script es crítico ya que extrae datos de MySQL y los guarda en S3.

### ✅ CORREGIDO: Vulnerabilidad de Inyección SQL
El código utilizaba f-strings y `.format()` para construir consultas SQL.

**Solución implementada:**
- Función `build_date_range()` que usa `datetime.datetime` para construir fechas de forma segura
- Validación explícita de tipos de parámetros antes de usarlos en queries
- Conversión explícita a `int()` para valores numéricos

### ✅ CORREGIDO: Gestión de Memoria
El script cargaba todo el resultado de la consulta en memoria.

**Solución implementada:**
- Uso de `SSCursor` (Server-Side Cursor) de PyMySQL para streaming
- La query se ejecuta **una sola vez** en el servidor y los resultados se transmiten en chunks
- Tamaño de chunk configurable via `config.ini` (default: 100,000 filas)
- Liberación de memoria después de cada chunk con `gc.collect()`

### ✅ CORREGIDO: Argumentos de Argparse
Los argumentos `--verbose` y `--dry_run` usaban `type=bool` que no funciona correctamente.

**Solución implementada:**
- Cambiados a `action='store_true'` que es el patrón correcto para flags booleanos
- `--site` ahora es `type=int` ya que se usa como entero en las queries

### ✅ CORREGIDO: Código Muerto
- Eliminados imports no utilizados: `requests`, `json`, `xxhash`, `atexit`, `unicodedata.name`
- Eliminado argumento `--type` que nunca se usaba
- Eliminado código comentado `atexit.register()`
- Eliminada redundancia en verificación de `debug_mode`

---

## 2. s3parquet2elastic.py
Este script orquesta el pipeline. Se recomienda externalizar la configuración del pipeline (lista de stages) para mayor flexibilidad.

---

## 3. Evaluación de Stages del Pipeline (`stages/`)

Los stages presentan una estructura consistente pero tienen problemas significativos de performance.

### 🔴 Crítico: Performance en `AggByItemFilterStage`
Este stage itera manualmente sobre cada fila del DataFrame usando `iterrows()`:
```python
# aggbyitem_fstage.py
for index, row in data.events_df.iterrows():
    # ... lógica manual de agregación ...
```
`iterrows()` es extremadamente lento y anti-patrón en Pandas para operaciones que pueden ser vectorizadas.

**Recomendación:** Reemplazar todo el bucle con una operación `groupby()` seguida de `to_dict()`. Esto podría acelerar este paso entre 100x y 1000x para grandes volúmenes de datos.

### 🟡 Optimización: `AssetsFilterStage`
Usa `apply()` con una lambda para filtrar strings:
```python
data.events_df['action_url'].apply(regex_filter)
```
**Recomendación:** Usar operaciones vectorizadas de strings como `.str.endswith()` o `.str.match()`.

### 🟡 Optimización: `MetricsFilterStage`
Usa bucles `for` para crear columnas binarias (dummies) fila por fila.

**Recomendación:** Usar `pd.get_dummies()` o asignaciones vectorizadas (`df.loc[condicion, columna] = 1`).

### 🟢 S3ParquetInputStage y ElasticOutputStage
- **Input:** Seguro contra inyección SQL (usa parquet). Validar manejo de errores de red.
- **Output:** Usa `bulk_size` lo cual es bueno. La generación de IDs con `xxhash` es correcta para consistencia.

---

## Resumen de Estado

### Corregidos ✅
| Archivo | Problema | Solución |
|---------|----------|----------|
| `matomo2parquet.py` | SQL Injection | Validación de tipos + datetime |
| `matomo2parquet.py` | Carga en memoria | SSCursor + chunks |
| `matomo2parquet.py` | Argparse type=bool | action='store_true' |
| `matomo2parquet.py` | Imports no usados | Eliminados |
| `matomo2parquet.py` | Código muerto | Eliminado |

### Pendientes 🔴
| Archivo | Problema | Prioridad |
|---------|----------|-----------|
| `aggbyitem_fstage.py` | `iterrows()` anti-patrón | Alta |
| `assets_fstage.py` | `apply()` no vectorizado | Media |
| `metrics_fstage.py` | Bucles no vectorizados | Media |

---

*Última actualización: 2026-02-03*
