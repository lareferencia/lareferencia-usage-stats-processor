# Identifier Normalization in `s3parquet2elastic.py`

## Objetivo

Este documento explica como el pipeline transforma identificadores antes de indexar en OpenSearch.

La meta es que:

- identificadores equivalentes queden unificados en una sola clave final
- las metricas (views, downloads, conversions, outlinks) se acumulen correctamente
- `stats_by_country` se mantenga consistente despues del reemplazo

## Donde se configura

La regla vive en la tabla `source` (usage-db), por `site_id`:

- `identifier_map_type`
- `identifier_map_regex`
- `identifier_map_replace`
- `identifier_map_filename`

En el admin, estos campos se editan en la vista de `Source`.

## Orden real en el pipeline

En `s3parquet2elastic.py`, el reemplazo de identificador NO ocurre al leer S3.
Ocurre despues del agregado por identificador.

Orden:

1. `S3ParquetInputStage`
2. `RobotsFilterStage`
3. `AssetsFilterStage`
4. `MetricsFilterStage`
5. `AggByItemFilterStage`
6. `IdentifierFilterStage`  <-- aqui se normaliza/reemplaza
7. `ElasticOutputStage`

## Datos que entran al stage de identificador

Antes de `IdentifierFilterStage`, el pipeline ya tiene:

- `data.agg_dict`: diccionario por identificador original con metricas agregadas
- `data.country_by_identifier_dict`: pais por identificador original (cuando aplica)
- `data.source`: la fila `source` del `site_id` procesado

## Modos de `identifier_map_type`

### 0: Normalizacion OAI (default)

Aplica `normalize_oai_identifier(old_identifier)`.

Ejemplo:

- entrada: `oai:https://repositorio.ejemplo.org/rest:12345`
- salida: `oai:repositorio.ejemplo.org:12345`

Uso recomendado cuando solo quieres limpiar prefijos/proxy/rutas en el prefijo OAI.

### 1: Regex replace

Compila `identifier_map_regex` y aplica:

- `new_identifier = regex.sub(identifier_map_replace, old_identifier)`

Si el regex no compila, el stage falla con `ValueError`.

Uso recomendado cuando necesitas reglas puntuales de transformacion.

### 2: Map from file

Lee un archivo CSV simple con formato:

```text
old_identifier,new_identifier
old_identifier_2,new_identifier_2
```

Luego, para cada identificador:

- si existe en el mapa, reemplaza y cuenta `hits`
- si no existe, deja el identificador original

Si el archivo no esta definido o no se puede leer, el stage falla.

## Merge de colisiones (parte clave)

Si varios identificadores originales terminan en el mismo identificador final, el pipeline:

- suma metricas a nivel raiz
- suma metricas por pais dentro de `stats_by_country`

Esto evita duplicar documentos para el mismo recurso normalizado.

## Salida del stage

Despues de `IdentifierFilterStage`:

- `data.agg_dict` contiene solo identificadores finales
- `data.country_by_identifier_dict` queda alineado con esos identificadores
- se imprime:
  - `Identifiers: <N antes>`
  - `Normalized identifiers: <N despues>`
  - `Hits in map: <N>` (solo en `map_type=2`)

## Interaccion con `ElasticOutputStage`

`ElasticOutputStage` usa `data.agg_dict` final para construir `data.documents`.
El campo `identifier` del documento ya sale normalizado/reemplazado.

## Checklist operativo

Antes de correr ETL para un `site_id`:

1. Verificar `identifier_map_type` en `source`.
2. Si `map_type=1`, validar regex y reemplazo.
3. Si `map_type=2`, validar que el archivo exista en el contenedor.
4. Ejecutar una corrida de prueba y revisar:
   - `Identifiers`
   - `Normalized identifiers`
   - `Hits in map` (si aplica)

## Casos comunes de error

- "Identifier map file is not defined..."
  - `map_type=2` sin `identifier_map_filename`.

- "Error reading identifier map file ..."
  - ruta inexistente, permisos o formato invalido.

- "Invalid regex ..."
  - regex mal formada en `identifier_map_regex`.

## Nota importante sobre rutas de archivo (`map_type=2`)

La ruta en `identifier_map_filename` debe existir dentro del filesystem del contenedor `stats-processor`.
Si la DB guarda una ruta antigua (por ejemplo `/opt/.../fecyt.csv`) y ese archivo no existe en el contenedor actual, el stage fallara.

