# Plan de Implementación Detallado: Migración de `s3parquet2elastic.py` a PySpark

Fecha: 2026-02-11  
Proyecto: `lareferencia-usage-stats-processor`  
Alcance del plan: diseño e implementación de un pipeline PySpark con paridad funcional y rollout seguro.

## 1. Objetivo

Implementar una versión PySpark del proceso actual de `s3parquet2elastic.py` que:

- mantenga paridad funcional con el pipeline existente,
- sea más robusta para volúmenes grandes,
- mejore observabilidad y control de errores,
- permita una transición gradual sin interrumpir operación.

## 2. Alcance

En alcance:

- nuevo job PySpark paralelo al job actual,
- lectura de Parquet en S3 por particiones,
- reproducción de filtros y agregaciones actuales,
- mapeo de identificadores en los 3 modos existentes,
- indexación en OpenSearch/Elastic,
- validación de paridad y plan de cutover.

Fuera de alcance en primera entrega:

- reemplazo del job actual en producción desde día 1,
- rediseño funcional de reglas de negocio,
- cambios de estructura de índices históricos existentes,
- reescritura del módulo externo `lareferenciastatsdb`.

## 3. Estado Actual (AS-IS)

Pipeline actual (Python/Pandas):

1. `S3ParquetInputStage`
2. `RobotsFilterStage`
3. `AssetsFilterStage`
4. `MetricsFilterStage`
5. `AggByItemFilterStage`
6. `IdentifierFilterStage`
7. `ElasticOutputStage`

Entradas:

- S3 Parquet de `events` y `visits`.
- metadatos de fuente desde BD (`get_source_by_site_id`).
- configuración desde `config.ini`.

Salida:

- documentos indexados por `idsite` en OpenSearch.

## 4. Principios de Migración

- Paridad primero: igual resultado antes de optimizar reglas.
- Compatibilidad operacional: nueva ruta de ejecución, sin romper la actual.
- Idempotencia: misma corrida no debe duplicar/romper resultados.
- Observabilidad obligatoria: métricas por etapa y logs estructurados.
- Rollout gradual: shadow index y comparación antes de cutover.

## 5. Arquitectura Objetivo (TO-BE)

Componentes:

1. CLI PySpark (`s3parquet2elastic_spark.py`).
2. `SparkConfigurationContext` (wrapper de configuración + args + helper BD).
3. `SparkUsageStatsPipeline` con etapas equivalentes.
4. writer de OpenSearch con manejo de índice idempotente.
5. utilidades de comparación de paridad.

Estructura sugerida:

```text
/Users/lmatas/source/lareferencia-usage-stats/lareferencia-usage-stats-processor/
  spark_jobs/
    s3parquet2elastic_spark.py
  spark_pipeline/
    __init__.py
    context.py
    pipeline.py
    schemas.py
    stages/
      input_stage.py
      robots_stage.py
      assets_stage.py
      metrics_stage.py
      agg_stage.py
      identifier_stage.py
      output_stage.py
    writers/
      opensearch_writer.py
    utils/
      ids.py
      logging.py
      parity.py
```

## 6. Contrato de Datos y Paridad

## 6.1 Entradas mínimas

`events`:

- `idvisit`
- `server_time`
- identificador crudo (`custom_var_v1` o `custom_var_v6`)
- `action_type`
- `action_url`
- `action_url_prefix`
- `custom_var_v2` (solo fuente regional)

`visits`:

- `idvisit`
- `visit_last_action_time`
- `visit_first_action_time`
- `visit_total_actions`
- `location_country`

## 6.2 Salida final esperada por documento

- `id`
- `identifier`
- `idsite`
- `year`
- `month`
- `day`
- `date`
- `level`
- `country`
- acciones en raíz (`views`, `downloads`, `outlinks`, `conversions`, etc.)
- `stats_by_country` como arreglo de objetos `{country, acciones...}`

## 6.3 Reglas de paridad obligatorias

- Misma semántica de filtrado de robots y assets.
- Misma semántica de `conversions`.
- Misma semántica de mapeo/normalización de identifiers.
- Mismo criterio de país por identifier.
- Mismo índice destino por `idsite` (prefijo + helper BD).

## 7. Diseño Detallado por Etapa

## 7.1 Stage 1: Input (S3 Parquet)

Objetivo:

- leer particiones relevantes de `events` y `visits`,
- normalizar columnas necesarias,
- anexar información de fuente.

Lógica:

1. obtener `source = db_helper.get_source_by_site_id(site_id)` en driver,
2. construir filtro de partición por `idsite/year/month/day`,
3. leer parquet con poda de partición,
4. resolver columna de identifier por tipo de fuente,
5. resolver `country`:
   - regional: derivar de `custom_var_v2` (`XX_...` -> `XX`),
   - no regional: usar `source.country_iso`.

Implementación recomendada:

- `spark.read.parquet("s3://...")` + `filter` por columnas particionadas.
- aplicar `select` temprano para reducir ancho.
- tipado explícito de timestamps y numéricos.

Salida:

- `events_df` normalizado,
- `visits_df` normalizado,
- `source` en contexto.

## 7.2 Stage 2: Robots Filter

Objetivo:

- eliminar visitas/eventos con comportamiento robot.

Lógica:

1. convertir tiempos a timestamp,
2. calcular `total_time` y `avg_action_time`,
3. aplicar query de configuración `ROBOTS_FILTER.QUERY_STR`,
4. filtrar `events_df` con semi-join por `idvisit`.

Implementación Spark:

```python
visits = visits.withColumn("visit_last_action_time", F.to_timestamp("visit_last_action_time"))
visits = visits.withColumn("visit_first_action_time", F.to_timestamp("visit_first_action_time"))
visits = visits.withColumn("visit_total_actions", F.col("visit_total_actions").cast("double"))
visits = visits.withColumn("total_time", F.col("visit_last_action_time").cast("long") - F.col("visit_first_action_time").cast("long"))
visits = visits.withColumn("avg_action_time", F.when(F.col("visit_total_actions") > 0, F.col("total_time") / F.col("visit_total_actions")))
visits_filtered = visits.filter(F.expr(robots_query))
events_filtered = events.join(visits_filtered.select("idvisit"), ["idvisit"], "left_semi")
```

Comportamiento edge:

- si `visits` queda vacío, `events` debe quedar vacío.

## 7.3 Stage 3: Assets Filter

Objetivo:

- excluir URLs de assets no contabilizables.

Lógica equivalente al actual:

- evaluar regex sobre `lower(right(action_url, 9))`.

Implementación Spark:

```python
events = events.filter(~F.expr("lower(right(coalesce(action_url, ''), 9)) rlike '{}'".format(regex)))
```

Nota:

- preservar esta semántica primero, luego evaluar mejora funcional si negocio lo aprueba.

## 7.4 Stage 4: Metrics

Objetivo:

- crear métricas por visita+identifier,
- mezclar con visitas,
- calcular conversiones,
- derivar país por identifier.

Lógica:

1. para cada acción con `action_id > 0`, crear flag binaria,
2. agrupar por (`idvisit`, `oai_identifier`) con `max` por acción,
3. join con `visits` por `idvisit`,
4. calcular `conversions`,
5. construir `country_by_identifier`.

Implementación Spark:

- usar `when(...).otherwise(0)` para flags,
- `groupBy(...).agg(F.max(...))`,
- `conversions = (views == 1) AND ((downloads == 1) OR (outlinks == 1))`.

Salida:

- `metrics_df` con una fila por visita+identifier,
- `country_by_identifier_df` con una fila por identifier.

## 7.5 Stage 5: Aggregation By Item

Objetivo:

- consolidar métricas por identifier,
- construir `stats_by_country`.

Lógica Spark recomendada:

1. agregado raíz por `identifier`: `sum(action)`.
2. agregado por `identifier,country`: `sum(action)`.
3. reconstrucción nested:
   - `collect_list(struct(country, acciones...)) AS stats_by_country`.
4. join entre agregado raíz y nested.

Resultado:

- `agg_df`: una fila por identifier con métricas raíz + nested por país.

## 7.6 Stage 6: Identifier Mapping

Objetivo:

- mapear identifier final según `identifier_map_type`.

Modos:

1. `IDENTIFIER_MAP_NORMALIZE`:
   - aplicar `normalize_oai_identifier`.
2. `IDENTIFIER_MAP_REGEX_REPLACE`:
   - aplicar `regexp_replace`.
3. `IDENTIFIER_MAP_FROM_FILE`:
   - mapear desde archivo `key,value`.

Estrategia de implementación:

- dado que datos ya están agregados por identifier, cardinalidad suele ser menor,
- se puede usar UDF para `normalize_oai_identifier` sin alto costo,
- para map from file:
  - opción A: cargar map como DataFrame y hacer join,
  - opción B: broadcast dict + UDF (si mapa pequeño),
  - umbral sugerido: si mapa > 1M claves, preferir join DataFrame.

Paso obligatorio post-mapeo:

- reagrupar por identifier final para mergear colisiones,
- volver a sumar métricas raíz y por país,
- alinear `country_by_identifier` con identifier final.

## 7.7 Stage 7: Output OpenSearch

Objetivo:

- construir documentos finales e indexar en bulk.

Lógica:

1. construir `id` estable con `(idsite, level, identifier, year, month, day)`,
2. construir `date` con año/mes/día,
3. resolver `country` raíz desde `country_by_identifier`,
4. crear índice si no existe,
5. indexar con id mapping.

Manejo de índices:

- crear mapping dinámico por acciones y nested `stats_by_country`,
- usar chequeo explícito `indices.exists`,
- manejar solo excepciones esperables (`already exists`).

Writer recomendado:

- usar `org.opensearch.spark.sql` para escritura distribuida,
- configurar `opensearch.mapping.id = id`.

Condición de salida rápida:

- si no hay documentos, no conectar ni indexar.

## 8. Diseño de Código

## 8.1 Contratos internos

`SparkUsageStatsData` sugerido:

- `events_df`
- `visits_df`
- `metrics_df`
- `agg_df`
- `country_by_identifier_df`
- `documents_df`
- `source`

`SparkStage` base:

- `run(data, ctx) -> data`

`SparkUsageStatsPipeline`:

- lista de stages inmutables,
- ejecución secuencial,
- log de tiempo por stage.

## 8.2 Compatibilidad CLI

Mantener argumentos equivalentes:

- `-c --config_file_path`
- `-s --site`
- `-y --year`
- `-m --month`
- `-d --day`
- `-t --type`

Agregar opcionales PySpark:

- `--master`
- `--shuffle_partitions`
- `--checkpoint_dir`
- `--dry_run`

## 8.3 Configuración Spark recomendada

Base sugerida:

- `spark.sql.adaptive.enabled=true`
- `spark.sql.adaptive.coalescePartitions.enabled=true`
- `spark.sql.shuffle.partitions=<dinámico>`
- `spark.serializer=org.apache.spark.serializer.KryoSerializer`
- `spark.sql.broadcastTimeout=600`

Guía de `shuffle_partitions`:

- inicial: `max(200, executors * cores * 4)`.
- ajustar con métricas reales de shuffle.

## 9. Estrategia de Dependencias

Dependencias Python:

- `pyspark`
- `opensearch-py`
- `xxhash`
- `pandas`
- `pyarrow`

Dependencias JAR (cluster):

- conector OpenSearch Spark compatible con versión de Spark.

Empaquetado:

- crear `requirements-spark.txt`.
- empaquetar código como zip/whl para `spark-submit --py-files`.

## 10. Plan de Pruebas

## 10.1 Unit tests por stage

Casos mínimos:

1. input: selección de identifier según tipo de fuente.
2. robots: comportamiento con `visits` vacío.
3. assets: regex aplicado sobre últimos 9 chars.
4. metrics: cálculo de flags y `conversions`.
5. agg: construcción correcta de nested por país.
6. identifier: normalize/regex/from_file y merge de colisiones.
7. output: construcción de `id` y schema final.

## 10.2 Pruebas de paridad

Datasets de referencia:

- caso pequeño (1 día, 1 sitio),
- caso mediano (1 mes, 1 sitio),
- caso grande (1 día, sitio de alto tráfico).

Comparaciones:

- conteo de documentos,
- suma de cada acción global,
- suma por país,
- cantidad de identifiers únicos,
- distribución de `country` raíz,
- diff por documento (`id`) en muestra.

Tolerancia:

- objetivo 100% para reglas actuales,
- cualquier diferencia debe quedar documentada y aprobada.

## 10.3 Pruebas de integración

- lectura real de S3,
- conexión real a OpenSearch staging,
- creación de índice y escritura bulk,
- re-ejecución idempotente sobre mismo período.

## 11. Plan de Implementación por Fases

## Fase 0: Preparación (2-3 días)

Entregables:

- documento de contrato de salida (golden spec),
- script de comparación Pandas vs Spark,
- dataset de prueba controlado.

Criterio de salida:

- aprobada definición de paridad por negocio/tech lead.

## Fase 1: Esqueleto PySpark (2 días)

Entregables:

- CLI PySpark con args compatibles,
- contexto de configuración + helper BD,
- pipeline base con stages vacíos y logging.

Criterio de salida:

- job ejecuta inicio-fin en modo no-op.

## Fase 2: Input + Filters (3-4 días)

Entregables:

- Stage Input implementado,
- Stage Robots implementado,
- Stage Assets implementado.

Criterio de salida:

- resultados intermedios comparables con pipeline actual en caso pequeño.

## Fase 3: Metrics + Aggregation (3-4 días)

Entregables:

- Stage Metrics completo,
- Stage Aggregation completo.

Criterio de salida:

- paridad de agregaciones en caso pequeño y mediano.

## Fase 4: Identifier Mapping (3-5 días)

Entregables:

- soporte completo 3 modos,
- merge de colisiones,
- alineación de país por identifier final.

Criterio de salida:

- tests unitarios y de paridad para mapping aprobados.

## Fase 5: Output OpenSearch (3-4 días)

Entregables:

- writer OpenSearch distribuido,
- creación idempotente de índice,
- manejo de cero documentos.

Criterio de salida:

- indexación en staging exitosa y repetible.

## Fase 6: Hardening y Performance (3-5 días)

Entregables:

- tuning de particiones/shuffle,
- métricas de performance,
- manejo de errores y retries.

Criterio de salida:

- SLA objetivo en dataset grande de prueba.

## Fase 7: Rollout Gradual (1-2 semanas)

Entregables:

- dual run (actual + Spark) con shadow index,
- reporte diario de diffs,
- plan de cutover y rollback probado.

Criterio de salida:

- ventana acordada con diffs en cero y estabilidad operacional.

## 12. Estrategia de Rollout y Cutover

Pasos:

1. ejecutar Spark en índice sombra por 1-2 semanas,
2. comparar resultados diarios con proceso actual,
3. corregir desvíos,
4. activar alias de lectura al índice Spark,
5. mantener proceso anterior como fallback por ventana definida.

Rollback:

- revertir alias a índice anterior,
- detener job Spark,
- retomar job actual sin cambios.

## 13. Observabilidad y Operación

Métricas por ejecución:

- filas leídas events/visits,
- filas tras robots,
- filas tras assets,
- cantidad de identifiers,
- cantidad de documentos,
- tiempo por stage,
- throughput de indexación,
- cantidad de errores y retries.

Logging:

- logs estructurados JSON en driver,
- `run_id`, `site`, `year`, `month`, `day`, `level` en cada evento.

Alertas:

- job failed,
- documentos indexados en cero cuando históricamente no es normal,
- diferencia de paridad > umbral.

## 14. Riesgos y Mitigaciones

| Riesgo | Probabilidad | Impacto | Mitigación |
|---|---:|---:|---|
| Diferencias funcionales con Pandas | Media | Alta | Golden dataset + diff automático + rollout sombra |
| Dependencia `lareferenciastatsdb` no disponible en cluster | Media | Alta | Empaquetar wheel interno o API wrapper mínima |
| Rendimiento bajo por UDF de normalización | Media | Media | Aplicar UDF luego de agregar (menos filas), perf test dedicado |
| Mapping file muy grande | Media | Media | Join DataFrame en lugar de broadcast dict |
| Errores OpenSearch por mapping dinámico | Baja | Alta | Plantilla de índice validada + staging previo |
| Costos de shuffle altos | Media | Media | AQE + tuning partitions + profiling |

## 15. Criterios de Aceptación Final

1. Paridad funcional validada en casos chico/mediano/grande.
2. Idempotencia validada con re-ejecución de mismas fechas.
3. Performance igual o mejor que pipeline actual para volumen objetivo.
4. Observabilidad completa por stage y ejecución.
5. Rollback probado y documentado.

## 16. Checklist Técnico de Implementación

1. Crear estructura `spark_jobs/` y `spark_pipeline/`.
2. Implementar parser CLI compatible.
3. Implementar contexto + helper BD.
4. Implementar Input stage con poda de particiones.
5. Implementar Robots stage con semántica actual.
6. Implementar Assets stage con semántica actual.
7. Implementar Metrics stage.
8. Implementar Aggregation stage.
9. Implementar Identifier stage con 3 modos.
10. Implementar merge de colisiones post-mapeo.
11. Implementar Output stage + writer OpenSearch.
12. Implementar creación idempotente de índice.
13. Implementar métricas y logging estructurado.
14. Implementar tests unitarios por stage.
15. Implementar pruebas de paridad automatizadas.
16. Ejecutar dual run con shadow index.
17. Ejecutar cutover con alias y plan de rollback.

## 17. Comandos de Ejecución Sugeridos

Ejemplo local:

```bash
spark-submit \
  --master local[*] \
  --py-files spark_pipeline.zip \
  spark_jobs/s3parquet2elastic_spark.py \
  --config_file_path=config.ini \
  --site=48 \
  --year=2025 \
  --month=1 \
  --day=15 \
  --type=R
```

Ejemplo cluster:

```bash
spark-submit \
  --deploy-mode cluster \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.shuffle.partitions=400 \
  --jars /path/opensearch-spark-connector.jar \
  --py-files s3://bucket/artifacts/spark_pipeline.zip \
  s3://bucket/jobs/s3parquet2elastic_spark.py \
  --config_file_path=s3://bucket/config/config.ini \
  --site=48 --year=2025 --month=1 --day=15 --type=R
```

## 18. Entregables Finales del Proyecto

1. Código PySpark productivo del pipeline.
2. Documentación técnica y operativa.
3. Suite de tests unitarios y paridad.
4. Dashboard de métricas de ejecución.
5. Runbook de operación, cutover y rollback.
6. Informe de validación de paridad y performance.

---

Este plan está diseñado para ejecutar la migración sin modificar el proceso actual hasta que la nueva ruta esté validada en producción en modo sombra.
