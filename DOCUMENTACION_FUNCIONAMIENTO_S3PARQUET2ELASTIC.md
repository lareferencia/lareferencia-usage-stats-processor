# Funcionamiento de `s3parquet2elastic.py`

## 1. Objetivo

`s3parquet2elastic.py` ejecuta un pipeline de procesamiento de estadísticas de uso:

1. Lee eventos y visitas desde Parquet en S3.
2. Filtra ruido (robots, assets no relevantes).
3. Calcula métricas por identificador.
4. Agrega resultados por país.
5. Indexa documentos finales en OpenSearch/Elastic.

Archivo de entrada principal: `s3parquet2elastic.py`.

## 2. Flujo general

El script arma y ejecuta esta secuencia de stages:

1. `stages.S3ParquetInputStage`
2. `stages.RobotsFilterStage`
3. `stages.AssetsFilterStage`
4. `stages.MetricsFilterStage`
5. `stages.AggByItemFilterStage`
6. `stages.IdentifierFilterStage`
7. `stages.ElasticOutputStage`

El armado del pipeline ocurre en `UsageStatsProcessorPipeline` (`processorpipeline.py`).

## 3. Entrada por línea de comandos

Argumentos soportados por `s3parquet2elastic.py`:

- `--config_file_path` (`-c`): ruta al ini de configuración.
- `--site` (`-s`): id de sitio.
- `--year` (`-y`): año.
- `--month` (`-m`): mes.
- `--day` (`-d`): día (opcional).
- `--type` (`-t`): nivel/tipo de salida (ej. `R`, `L`, `N`).

## 4. Configuración requerida

`ConfigurationContext` (`configcontext.py`) carga:

- `GENERAL.ACTIONS` y `GENERAL.ACTIONS_ID`.
- Labels en sección `LABELS` (ej. `COUNTRY`, `OAI_IDENTIFIER`, `ID_VISIT`).
- Config de S3 (`S3_STATS`) para rutas `VISITS_PATH` y `EVENTS_PATH`.
- Config de salida (`OUTPUT`) para URL de Elastic y prefijo de índice.
- Helper de base (`UsageStatsDatabaseHelper`, dependencia externa `lareferenciastatsdb`).

## 5. Detalle por stage

### 5.1 `S3ParquetInputStage`

- Obtiene fuente del sitio desde BD (`get_source_by_site_id`).
- Define partición S3 por `idsite/year/month/day`.
- Lee `events` y `visits` con `awswrangler.s3.read_parquet`.
- Ajusta columna de identificador según tipo de fuente:
  - repositorio: `custom_var_v1`
  - otros: `custom_var_v6`
- Normaliza columna país:
  - regional: extrae país desde `custom_var_v2`.
  - no regional: usa `source.country_iso`.

Salida principal:

- `data.events_df`
- `data.visits_df`
- `data.source`

### 5.2 `RobotsFilterStage`

- Calcula `total_time` y `avg_action_time` en visitas.
- Filtra visitas por query configurable (`ROBOTS_FILTER.QUERY_STR`).
- Deja solo eventos cuyo `idvisit` exista en visitas filtradas.

### 5.3 `AssetsFilterStage`

- Compila regex configurable (`ASSETS_FILTER.REGEX`).
- Excluye eventos de URLs que matchean patrón de assets.

### 5.4 `MetricsFilterStage`

- Crea flags por acción (`views`, `downloads`, `outlinks`, etc.) usando `ACTIONS` y `ACTIONS_ID`.
- Agrupa por (`idvisit`, `oai_identifier`) usando `max` por acción.
- Mezcla con visitas por `idvisit`.
- Calcula `conversions` cuando se cumple vista + (descarga u outlink).
- Construye `country_by_identifier_dict` para país principal por identificador.

### 5.5 `AggByItemFilterStage`

- Recorre eventos agregados y construye `data.agg_dict` por identificador.
- Acumula métricas globales.
- Acumula métricas por país en `stats_by_country`.

### 5.6 `IdentifierFilterStage`

Mapea identificadores según tipo configurado en la fuente:

- normalización (`normalize_oai_identifier`)
- reemplazo regex
- mapeo desde archivo CSV simple `clave,valor`

Salida:

- `data.agg_dict` con identificadores finales.
- `data.country_by_identifier_dict` alineado con esos identificadores.

### 5.7 `ElasticOutputStage`

- Arma mapping dinámico (acciones + nested `stats_by_country`).
- Construye documentos finales con campos:
  - `id`, `idsite`, `date`, `year`, `month`, `day`, `level`, `identifier`, `country`, métricas.
- Crea índice si no existe.
- Indexa documentos en bulk (`wr.opensearch.index_documents`).

## 6. Estructura del objeto compartido (`UsageStatsData`)

`UsageStatsData` transporta el estado entre stages, incluyendo:

- `source`
- `events_df`
- `visits_df`
- `country_by_identifier_dict`
- `agg_dict`
- `documents`

## 7. Ejecución típica

Ejemplo:

```bash
python3 s3parquet2elastic.py \
  --config_file_path=config.ini \
  --site=48 \
  --year=2025 \
  --month=1 \
  --day=15 \
  --type=R
```

## 8. Dependencias clave

- `awswrangler` (lectura S3 y escritura OpenSearch)
- `pandas` (transformaciones tabulares)
- `xxhash` (id estable de documentos)
- `lareferenciastatsdb` (helper de BD y utilidades de identifier)
