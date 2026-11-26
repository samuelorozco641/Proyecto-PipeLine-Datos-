# Pipeline de datos: accidentalidad vial en Colombia

Proyecto académico de **Gestión de Datos Masivos -- G002**, de la
Institución Universitaria Pascual Bravo. El objetivo es construir un
pipeline que lleve datos abiertos desde una API hasta una zona de
servicio y un tablero de indicadores.

## 1. Pregunta del proyecto

**¿Qué patrones se observan en los vehículos involucrados en siniestros
viales con heridos o muertos en Colombia, según tipo de vehículo,
gravedad, antigüedad y ubicación?**

Los indicadores pueden ser utilizados como insumo exploratorio por
autoridades de tránsito, secretarías de movilidad, investigadores y
entidades interesadas en la seguridad vial.

> **Alcance:** el conjunto de datos se refiere a vehículos involucrados
> en siniestros con resultado de muerte o lesión. No debe interpretarse
> como un inventario de todos los accidentes del país ni permite
> calcular tasas de riesgo sin datos de exposición, como parque
> automotor o kilómetros recorridos.

## 2. Fuente de datos

-   **Portal:** [Datos Abiertos de Colombia](https://www.datos.gov.co/)
-   **Conjunto:** Vehículos involucrados en un accidente de tránsito --
    Ley 2251 de 2022.
-   **Identificador Socrata:** `6jmc-vaxk`
-   **Página del conjunto:** [Consultar conjunto de
    datos](https://www.datos.gov.co/Transporte/VEHICULOS-INVOLUCRADOS-EN-UN-ACCIDENTE-DE-TRANSITO/6jmc-vaxk)
-   **Endpoint API:** `https://www.datos.gov.co/resource/6jmc-vaxk.json`

El notebook consulta la API mediante solicitudes HTTP y paginación. El
número de registros puede cambiar cuando la entidad actualice la fuente.

### Identificador de registro

La fuente no proporciona en el notebook un identificador único de fila
garantizado. Por eso se calcula `id_registro` como un hash SHA-256 del
contenido original de cada registro, serializado de forma ordenada. Esto
permite detectar registros idénticos. Si dos eventos distintos tienen
exactamente los mismos atributos disponibles, este identificador
derivado no permite distinguirlos.

## 3. Arquitectura

El diseño propuesto es **Lambda**: combina procesamiento batch para el
histórico con una capa de streaming para procesar mensajes que llegan a
Kafka.

``` text
                    API de datos.gov.co
                            |
                            v
                  Python / requests
                            |
                            v
                   ZONA RAW (JSONL)
                            |
                            v
                    Apache Kafka
                            |
                 +----------+-----------+
                 |                      |
                 v                      v
          Consumidor Kafka       Spark Structured
                 |                 Streaming
                 v                      |
             HDFS / Lake                |
                 |                      |
                 v                      |
          Python MapReduce              |
                 |                      |
                 v                      |
            PySpark batch               |
                 |                      |
                 v                      |
          ZONA CURADA (Parquet)         |
                 |                      |
                 +----------+-----------+
                            |
                            v
                    SQLite / SERVICIO
                            |
                            v
                     Tablero Plotly
```

**Nota de implementación:** el notebook contiene las celdas de
integración para Kafka, HDFS y Structured Streaming. Estas fases
requieren que los servicios correspondientes estén instalados y
accesibles. Si se ejecuta en Google Colab sin broker Kafka o clúster
HDFS, esas fases no se consideran ejecutadas; el notebook permite
continuar con almacenamiento local para las partes batch.

## 4. Fases implementadas en el notebook

El archivo principal es `Pipeline_Accidentalidad_Vial_Colombia.ipynb`.

  -----------------------------------------------------------------------
  Fase                    Actividad               Resultado esperado
  ----------------------- ----------------------- -----------------------
  1                       Fuente, pregunta y      Definición del
                          arquitectura            problema, fuente e
                                                  identificación

  2                       Ingesta API             Archivos JSON Lines
                                                  (`.jsonl`) y manifiesto

  3                       Kafka                   Tópico, productor y
                                                  consumidor

  4                       HDFS y MapReduce        Almacenamiento por lote
                                                  y conteo por gravedad

  5                       PySpark                 Limpieza y escritura de
                                                  Parquet particionado
                                                  por año

  6                       Calidad e idempotencia  Validaciones, reporte y
                                                  marca de agua

  7                       Streaming               Ventanas, watermark y
                                                  checkpoint

  8                       Servicio y tablero      Indicadores en SQLite y
                                                  visualizaciones Plotly
  -----------------------------------------------------------------------

### Zona Raw

Cada registro se guarda dentro de un sobre con los campos:

-   `fuente`
-   `dataset`
-   `id_registro`
-   `fecha_ingesta`
-   `fecha_evento`
-   `payload`: registro original de la API.

Los archivos se particionan por `fecha_ingesta=AAAA-MM-DD`. También se
genera un manifiesto JSON con la URL, parámetros y cantidad descargada.

### Zona curada

PySpark normaliza campos de texto, convierte los campos numéricos,
interpreta fechas completas y parciales (por ejemplo `01/2022`), elimina
duplicados por `id_registro` y conserva registros con ubicación
faltante, etiquetándolos como `SIN DATO`. Las fechas que no se pueden
interpretar quedan como nulas y se particionan bajo
`anio_accidente=DESCONOCIDO`.

### Zona de servicio

SQLite contiene tablas agregadas para el tablero:

-   `vehiculos_por_tipo`
-   `vehiculos_por_departamento`
-   `vehiculos_por_gravedad`
-   `edad_promedio_por_tipo`

Las visualizaciones se generan con Plotly.

## 5. Archivos del repositorio

Una estructura sugerida para GitHub es:

``` text
.
├── Pipeline_Accidentalidad_Vial_Colombia.ipynb
├── README.md
├── requirements.txt
├── data/
│   └── .gitkeep
├── evidencias/
│   └── .gitkeep
└── .gitignore
```

Los datos descargados, archivos Parquet, bases SQLite, checkpoints y
resultados de ejecución se generan al ejecutar el notebook. No es
recomendable subir al repositorio el conjunto completo si ocupa
demasiado espacio; se puede incluir una muestra pequeña y evidencias.

## 6. Requisitos

-   Python 3.10 o superior recomendado.
-   Jupyter Notebook o Google Colab.
-   Conexión a Internet para consultar la API.
-   Java compatible con la versión de PySpark instalada.
-   Para las fases distribuidas: Apache Kafka y Apache Hadoop/HDFS
    configurados.
-   Para el tablero: Plotly.

### Dependencias de Python

Crear un archivo `requirements.txt` con:

``` text
requests
pandas
pyarrow
pyspark
kafka-python
sqlalchemy
plotly
```

En Google Colab se pueden instalar las dependencias ejecutando:

``` python
%pip install requests pandas pyarrow pyspark kafka-python sqlalchemy plotly
```

> La instalación de las librerías Python no instala ni configura por sí
> sola un broker Kafka o un clúster HDFS.

## 7. Ejecución en Google Colab

1.  Subir `Pipeline_Accidentalidad_Vial_Colombia.ipynb` a Google Drive o
    abrirlo directamente en Colab.
2.  Seleccionar **Entorno de ejecución → Ejecutar todas** o ejecutar las
    celdas en orden.
3.  Instalar las dependencias cuando sea necesario.
4.  Ejecutar la fase de ingesta para consultar la API.
5.  Revisar los archivos Raw en el panel **Archivos** de Colab, dentro
    de `pipeline_accidentalidad/raw/`.
6.  Ejecutar las transformaciones PySpark y revisar la salida Parquet.
7.  Revisar el reporte de calidad y la demostración de deduplicación.
8.  Ejecutar las agregaciones, cargar SQLite y visualizar los gráficos.

La ruta base del proyecto en Colab es
`/content/pipeline_accidentalidad/`. En otros entornos se crea una
carpeta `pipeline_accidentalidad` en el directorio de trabajo.

### Archivos generados

``` text
pipeline_accidentalidad/
├── raw/
│   └── fecha_ingesta=AAAA-MM-DD/
│       └── lote_0001.jsonl
├── manifests/
│   └── manifest_*.json
├── lake/
│   └── datos.gov.co/
│       └── vehiculos_involucrados_accidentes_transito/
├── curated/
│   └── vehiculos_accidentes_parquet/
├── reports/
│   ├── reporte_calidad.json
│   └── watermark.json
└── service/
    └── accidentalidad.db
```

## 8. Configuración de Kafka y HDFS

Para conectar el notebook a Kafka, definir la variable de entorno:

``` python
import os
os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "localhost:9092"
```

El notebook utiliza el tópico `accidentalidad-raw`, con tres particiones
y el grupo consumidor `grupo-accidentalidad`. El productor publica cada
sobre con `key=id_registro`.

Para HDFS, configurar `HDFS_NAMENODE` con la dirección del NameNode y
disponer del comando `hdfs` en el entorno. Por ejemplo:

``` python
import os
os.environ["HDFS_NAMENODE"] = "hdfs://localhost:9000"
```

Los valores anteriores son ejemplos para un entorno local; deben
reemplazarse por las direcciones reales de los servicios. En Google
Colab, `localhost` apunta a la máquina virtual de Colab, no al
computador personal.

## 9. Calidad, idempotencia y consideraciones

El notebook genera controles de volumen, identificador no nulo,
duplicados y valores numéricos negativos. También guarda una marca de
agua con la fecha máxima que pudo interpretar.

La deduplicación por `id_registro` permite reconocer registros repetidos
dentro del conjunto procesado. Para demostrar idempotencia en un entorno
productivo se debe controlar también la escritura de lotes y el estado
de offsets del consumidor. Ejecutar dos veces un productor Kafka puede
publicar nuevamente los mismos mensajes: la clave del mensaje no elimina
duplicados automáticamente.

El procesamiento streaming requiere Kafka, el conector Kafka de Spark
compatible con la versión de Spark utilizada y una ruta persistente de
checkpoint. El checkpoint debe conservarse entre reinicios para que
Spark pueda recuperar el progreso.

## 10. Evidencias recomendadas

Agregar capturas o archivos en `evidencias/` que demuestren:

-   Consulta a la API y manifiesto de ingesta.
-   Archivo JSONL de la zona Raw.
-   Tópico Kafka con tres particiones, publicación y consumo.
-   Estructura de carpetas en HDFS.
-   Resultado MapReduce comparado con Spark sobre los mismos datos y
    reglas.
-   Conteos antes y después de la limpieza.
-   Comparación del tamaño JSONL y Parquet.
-   Reporte de calidad e idempotencia.
-   Streaming antes y después de reiniciar, con el mismo checkpoint.
-   Tablas SQLite y tablero Plotly.

No se deben incluir capturas de fases que solo estén descritas en el
notebook pero que no se hayan ejecutado.

## 11. Limitaciones conocidas

-   La interpretación de fechas parciales como `MM/YYYY` asigna el
    primer día del mes como fecha representativa. Esto facilita agrupar
    por año, pero no significa que se conozca el día real del siniestro.
-   El hash del contenido es un identificador técnico derivado, no un ID
    oficial de la entidad.
-   Los conteos representan vehículos registrados en el conjunto, no
    necesariamente el número de siniestros.
-   Los resultados dependen de la actualización y calidad de la fuente
    original.
-   Kafka, HDFS y streaming necesitan infraestructura adicional; el
    notebook no crea automáticamente un clúster completo.

## 12. Créditos

Proyecto académico elaborado para la asignatura **Gestión de Datos
Masivos -- G002**, Unidad 3, Institución Universitaria Pascual Bravo.

Fuente de datos: Portal Nacional de Datos Abiertos de Colombia, conjunto
de vehículos involucrados en accidentes de tránsito, identificador
Socrata `6jmc-vaxk`.
