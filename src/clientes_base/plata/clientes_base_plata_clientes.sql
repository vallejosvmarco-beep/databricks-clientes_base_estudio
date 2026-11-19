-- clientes_base - plata: maestro de clientes SIGEL (1 fila por NISE, ultimo periodo cargado).
-- Llave de cruce con AMI_base: NISE (STRING, mismo formato que plata_carga / plata_consumo /
-- plata_950_carga). En LOCALIZACION de SIGEL no esta el NISE de AMI.
-- CORREO y TELEFONO se quedan en el bronce (no hacen falta para cruzar con AMI).
-- Marcas para analisis:
--   FLAG_SIN_AMI_BASE: el NISE no aparece en plata_carga ni en plata_consumo de AMI_base
--     (ventana de meses de AMI_base). EN_AMI_CARGA / EN_AMI_CONSUMO detallan la fuente.
--   FLAG_NISE_DUPLICADO: el NISE tiene mas de un registro en SIGEL (se conserva 1:
--     Activo primero, luego la lectura mas reciente). Todos los registros en
--     clientes_base_plata_duplicados.
--   FLAG_MEDIDOR_DUPLICADO: el medidor (distinto de 0) esta en mas de un registro SIGEL.
--   FLAG_SIN_MEDIDOR: medidor 0 en SIGEL (servicios sin medicion: alumbrado, semaforos...).
CREATE OR REFRESH MATERIALIZED VIEW clientes_base_plata_clientes
COMMENT 'clientes_base: maestro de clientes SIGEL tipado, 1 fila por NISE; cruza con AMI_base por NISE'
AS
WITH ultimo AS (
  SELECT max(_periodo) AS periodo FROM clientes_base_bronce_sigel_infoclientes
),
clientes AS (
  SELECT c.*,
         CAST(CAST(c.NISE AS DECIMAL(20, 0)) AS STRING) AS NISE_TXT,
         row_number() OVER (
           PARTITION BY CAST(CAST(c.NISE AS DECIMAL(20, 0)) AS STRING)
           ORDER BY CASE WHEN c.ESTADO_SER = 'Activo' THEN 0 ELSE 1 END,
                    c.FECHA_LECT DESC NULLS LAST, try_cast(c.OBJECTID AS BIGINT) DESC
         ) AS rn,
         count(*) OVER (PARTITION BY CAST(CAST(c.NISE AS DECIMAL(20, 0)) AS STRING)) AS registros_nise,
         count(*) OVER (PARTITION BY c.NUMEROMEDI) AS registros_medidor
  FROM clientes_base_bronce_sigel_infoclientes c
  JOIN ultimo u ON c._periodo = u.periodo
),
servicio_bt AS (
  SELECT NUMEROMEDI, IDALIMENTA, N_TRANSFOR, FASESASIGN, TECNOLOGIA
  FROM (
    SELECT s.*, row_number() OVER (PARTITION BY s.NUMEROMEDI ORDER BY try_cast(s.OBJECTID AS BIGINT) DESC) AS rn
    FROM clientes_base_bronce_servicio_baja_tension s
    JOIN ultimo u ON s._periodo = u.periodo
  )
  WHERE rn = 1
),
gd AS (
  SELECT DISTINCT CAST(CAST(g.NISE AS DECIMAL(20, 0)) AS STRING) AS NISE
  FROM clientes_base_bronce_medigendistribuida g
  JOIN ultimo u ON g._periodo = u.periodo
  WHERE g.NISE IS NOT NULL
),
-- Presencia en AMI_base (MVs del pipeline AMI_base en el mismo esquema)
ami_carga AS (
  SELECT DISTINCT NISE FROM plata_carga
),
ami_consumo AS (
  SELECT DISTINCT NISE FROM plata_consumo
)
SELECT
  c.NISE_TXT                                        AS NISE,
  c.NUMEROMEDI                                      AS MEDIDOR,
  c.LOCALIZACI                                      AS LOCALIZACION,
  c.NUMEROABON                                      AS NUMERO_ABONADO,
  c.NOMBREABON                                      AS NOMBRE_ABONADO,
  c.PROVINCIA, c.CANTON, c.DISTRITO, c.DIRECCION, c.REGION,
  c.OFI_EJE                                         AS OFICINA,
  try_cast(c.TARIFA AS INT)                         AS TARIFA,
  c.TIPOCIU                                         AS TIPO_CIU,
  c.DESCTIPOCI                                      AS DESC_TIPO_CIU,
  try_cast(c.SECTOR AS INT)                         AS SECTOR,
  c.DESCSECTOR                                      AS DESC_SECTOR,
  c.DESCSERV                                        AS DESC_SERVICIO,
  c.ESTADO_SER                                      AS ESTADO_SERVICIO,
  c.ESTADO_NEG                                      AS ESTADO_NEGOCIO,
  c.CICLO,
  try_cast(c.RUTA AS INT)                           AS RUTA,
  try_to_date(c.FECHA_CONE, 'yyyyMMdd')             AS FECHA_CONEXION,
  try_to_date(c.FEC_INST_M, 'yyyyMMdd')             AS FECHA_INSTALACION_MEDIDOR,
  CASE upper(c.MED_AMI) WHEN 'VERDADERO' THEN true WHEN 'FALSO' THEN false END AS MEDIDOR_AMI,
  c.MODELO                                          AS MODELO_MEDIDOR,
  c.FORMA_MED                                       AS FORMA_MEDICION,
  c.TIPO_CONEX                                      AS TIPO_CONEXION,
  c.VOLTAJE,
  c.CARGA_CONE                                      AS CARGA_CONECTADA,
  try_cast(c.CONSTANTE AS DOUBLE)                   AS CONSTANTE,
  c.TIPOCURVA                                       AS TIPO_CURVA,
  try_cast(c.MGE AS BIGINT)                         AS MGE,
  try_cast(c.CONMGE AS BIGINT)                      AS CON_MGE,
  c.IND_PERDID                                      AS IND_PERDIDAS,
  c.PROPIEDAD, c.PRIORIDAD,
  -- Ultima lectura de facturacion reportada por SIGEL
  try_to_date(c.FECHA_LECT, 'yyyyMMdd')             AS FECHA_LECTURA,
  try_to_date(c.FECHA_DESD, 'yyyyMMdd')             AS FECHA_DESDE,
  try_to_date(c.FECHA_HAST, 'yyyyMMdd')             AS FECHA_HASTA,
  try_cast(c.Q_DIAS AS INT)                         AS DIAS_PERIODO,
  try_cast(c.KWH_PERIOD AS DOUBLE)                  AS KWH_PERIODO,
  try_cast(c.KW_PERIOD AS DOUBLE)                   AS KW_PERIODO,
  try_cast(c.KVAR_PERIO AS DOUBLE)                  AS KVAR_PERIODO,
  try_cast(c.KWH_PROMED AS DOUBLE)                  AS KWH_PROMEDIO,
  try_cast(c.KW_PROMEDI AS DOUBLE)                  AS KW_PROMEDIO,
  try_cast(c.KWH_PUNTA AS DOUBLE)                   AS KWH_PUNTA,
  try_cast(c.KWH_VALLE AS DOUBLE)                   AS KWH_VALLE,
  try_cast(c.KWH_NOCT AS DOUBLE)                    AS KWH_NOCHE,
  try_cast(c.DEMANDA AS DOUBLE)                     AS DEMANDA,
  try_cast(c.KW_MAX_PER AS DOUBLE)                  AS KW_MAX_PERIODO,
  -- Red (Servicio_Baja_Tension por numero de medidor)
  s.IDALIMENTA                                      AS CIRCUITO,
  s.N_TRANSFOR                                      AS TRANSFORMADOR,
  try_cast(s.FASESASIGN AS INT)                     AS FASES_ASIGNADAS,
  s.TECNOLOGIA                                      AS TECNOLOGIA_MEDICION,
  -- Generacion distribuida (MediGenDistribuida)
  (gd.NISE IS NOT NULL)                             AS TIENE_GD,
  -- Representacion en AMI_base
  (ac.NISE IS NOT NULL)                             AS EN_AMI_CARGA,
  (ao.NISE IS NOT NULL)                             AS EN_AMI_CONSUMO,
  (ac.NISE IS NULL AND ao.NISE IS NULL)             AS FLAG_SIN_AMI_BASE,
  -- Duplicados en SIGEL
  c.registros_nise                                  AS REGISTROS_SIGEL,
  (c.registros_nise > 1)                            AS FLAG_NISE_DUPLICADO,
  (c.NUMEROMEDI <> '0' AND c.registros_medidor > 1) AS FLAG_MEDIDOR_DUPLICADO,
  (c.NUMEROMEDI = '0')                              AS FLAG_SIN_MEDIDOR,
  c._periodo                                        AS PERIODO_SIGEL,
  c._fecha_ingesta                                  AS FECHA_INGESTA
FROM clientes c
LEFT JOIN servicio_bt s ON s.NUMEROMEDI = c.NUMEROMEDI AND c.NUMEROMEDI <> '0'
LEFT JOIN gd ON gd.NISE = c.NISE_TXT
LEFT JOIN ami_carga ac ON ac.NISE = c.NISE_TXT
LEFT JOIN ami_consumo ao ON ao.NISE = c.NISE_TXT
WHERE c.rn = 1;

-- Todos los registros SIGEL duplicados (por NISE o por medidor distinto de 0), sin deduplicar,
-- para analizarlos. ES_REGISTRO_CONSERVADO = el que quedo en clientes_base_plata_clientes.
CREATE OR REFRESH MATERIALIZED VIEW clientes_base_plata_duplicados
COMMENT 'clientes_base: registros SIGEL con NISE o medidor (<> 0) repetido, para analisis'
AS
WITH ultimo AS (
  SELECT max(_periodo) AS periodo FROM clientes_base_bronce_sigel_infoclientes
),
base AS (
  SELECT c.*,
         CAST(CAST(c.NISE AS DECIMAL(20, 0)) AS STRING) AS NISE_TXT,
         count(*) OVER (PARTITION BY CAST(CAST(c.NISE AS DECIMAL(20, 0)) AS STRING)) AS registros_nise,
         count(*) OVER (PARTITION BY c.NUMEROMEDI) AS registros_medidor,
         row_number() OVER (
           PARTITION BY CAST(CAST(c.NISE AS DECIMAL(20, 0)) AS STRING)
           ORDER BY CASE WHEN c.ESTADO_SER = 'Activo' THEN 0 ELSE 1 END,
                    c.FECHA_LECT DESC NULLS LAST, try_cast(c.OBJECTID AS BIGINT) DESC
         ) AS rn
  FROM clientes_base_bronce_sigel_infoclientes c
  JOIN ultimo u ON c._periodo = u.periodo
)
SELECT
  CASE
    WHEN registros_nise > 1 AND NUMEROMEDI <> '0' AND registros_medidor > 1 THEN 'NISE_Y_MEDIDOR'
    WHEN registros_nise > 1 THEN 'NISE'
    ELSE 'MEDIDOR'
  END                                   AS TIPO_DUPLICADO,
  NISE_TXT                              AS NISE,
  NUMEROMEDI                            AS MEDIDOR,
  LOCALIZACI                            AS LOCALIZACION,
  try_cast(OBJECTID AS BIGINT)          AS OBJECTID_SIGEL,
  ESTADO_SER                            AS ESTADO_SERVICIO,
  DESCSERV                              AS DESC_SERVICIO,
  DESCSECTOR                            AS DESC_SECTOR,
  try_cast(TARIFA AS INT)               AS TARIFA,
  try_to_date(FECHA_CONE, 'yyyyMMdd')   AS FECHA_CONEXION,
  try_to_date(FECHA_LECT, 'yyyyMMdd')   AS FECHA_LECTURA,
  registros_nise                        AS REGISTROS_NISE,
  registros_medidor                     AS REGISTROS_MEDIDOR,
  (rn = 1)                              AS ES_REGISTRO_CONSERVADO,
  _periodo                              AS PERIODO_SIGEL
FROM base
WHERE registros_nise > 1 OR (NUMEROMEDI <> '0' AND registros_medidor > 1);
