# lareferencia-usage-stats-processor

Motor ETL de estadísticas de uso. Contiene dos etapas macro:

1. Extracción desde Matomo MySQL y persistencia en S3 parquet.
2. Lectura desde S3, filtrado/agregación e indexación en OpenSearch.

## Vista funcional

```mermaid
flowchart LR
    M[(Matomo MySQL)] --> A[matomo2parquet.py]
    A --> S[(S3 Parquet)]
    S --> B[s3parquet2elastic.py]
    B --> O[(OpenSearch)]
```

## Entrypoints principales

- `matomo2parquet.py`
  - Extrae `visits` y `events` por sitio/período.
  - Usa streaming con `SSCursor` + chunks configurables.
  - Escribe datasets parquet particionados en S3.

- `s3parquet2elastic.py`
  - Ejecuta pipeline completo de transformación/indexación.

- `s3parquetQueryByIdentifier.py`
  - Pipeline de diagnóstico para imprimir métricas de un identificador.

- `s3stats.py`
  - Pipeline de diagnóstico para generar gráfico local de volumen.

- `runner.py`
  - Orquestador batch: ejecuta un proceso parametrizado por sitio/fecha/rango.

## Pipeline interno

El framework está en:

- `processorpipeline.py` (input -> filtros -> output)
- `configcontext.py` (config + args + labels + helper DB)

### Stages disponibles

```mermaid
flowchart LR
    I[S3ParquetInputStage] --> F1[RobotsFilterStage]
    F1 --> F2[AssetsFilterStage]
    F2 --> F3[MetricsFilterStage]
    F3 --> F4[AggByItemFilterStage]
    F4 --> F5[IdentifierFilterStage]
    F5 --> O1[ElasticOutputStage]
```

Stages:

- `S3ParquetInputStage`: carga `events_df` y `visits_df` desde S3 según `idsite/year/month/day`; enriquece país según tipo de fuente.
- `RobotsFilterStage`: filtra visitas no humanas y sincroniza eventos asociados.
- `AssetsFilterStage`: excluye assets estáticos por regex de URL.
- `MetricsFilterStage`: calcula columnas binarias por acción y `conversions`.
- `AggByItemFilterStage`: agrega por identificador y por país (`stats_by_country`).
- `IdentifierFilterStage`: normaliza/mapea identificadores (regex o archivo).
- `ElasticOutputStage`: crea mapping si hace falta e indexa documentos bulk.

## Configuración (`config.model.ini`)

Secciones clave:

- `GENERAL`: acciones y IDs de acción Matomo.
- `LABELS`: nombres de columnas semánticas.
- `ROBOTS_FILTER`: expresión de filtro sobre visitas.
- `ASSETS_FILTER`: regex de exclusión.
- `OUTPUT`: `ELASTIC_URL`, `INDEX_PREFIX`.
- `USAGE_STATS_DB`: DB de metadatos compartida.
- `MATOMO_DB`: conexión MySQL origen.
- `S3_STATS`: paths de datasets parquet.
- `S3_LOGS`: bucket/path de logs.
- `PROCESSING`: `CHUNK_SIZE`.

## Ejecución

### 1) Matomo -> S3

```bash
python matomo2parquet.py -c config.ini -s <site_id> -y <yyyy> -m <mm> [-d <dd>] [--debug] [--dry_run]
```

Notas CLI:

- Parámetros obligatorios: `-s/--site`, `-y/--year`, `-m/--month`.
- `-d/--day` es opcional, pero si se usa debe ser válido para el `year/month` enviado.
- Ayuda rápida: `python matomo2parquet.py --help`.

### 2) S3 -> OpenSearch

```bash
python s3parquet2elastic.py -c config.ini -s <site_id> -y <yyyy> -m <mm> [-d <dd>] -t <R|N|L>
```

### 3) Batch

```bash
python runner.py --process="python matomo2parquet.py" -c config.ini -s all -y 2026 --from_month 1 --to_month 12
```

## Contrato de salida en OpenSearch

Documento por identificador/período con campos:

- `id`, `identifier`, `idsite`, `date`, `year`, `month`, `day`, `level`, `country`
- métricas raíz (`views`, `downloads`, `conversions`, `outlinks`)
- nested `stats_by_country` con métricas por país

## Integración con otros módulos

- Lee metadata de `lareferenciastatsdb` para tipo de fuente y reglas de identificador.
- Escribe índices que luego consulta `lareferencia-usage-stats-service`.

## Utilidades auxiliares

- `s3logger.py`: logging + upload a S3.
- `import2matomo.py`: envío batch de requests al endpoint bulk de Matomo.
- `parse_fecyt.py`: script ad-hoc de conversión SQL->CSV.

## Notas técnicas

- El rendimiento del agregado principal puede mejorar con vectorización adicional en pandas.
- `runner.py` usa `subprocess.run(..., shell=True)`; validar entradas si se usa en contextos multiusuario.
