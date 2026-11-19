"""clientes_base - bronce: ingesta de archivos DBF (dBase III) del volumen.

Lee /Volumes/<catalog>/<schema>/clientes_base/<AAAA>/<MM>/*.dbf y escribe una tabla
bronce por archivo: <catalog>.<schema>.clientes_base_bronce_<nombre_archivo>.

* Todas las columnas como STRING (tipado en la capa plata), mas metadatos
  _periodo (AAAA-MM de la carpeta), _archivo, _fecha_ingesta.
* Incremental: tabla de control clientes_base_bronce_manifiesto (ruta, tamano,
  modificado). Un archivo ya cargado y sin cambios no se vuelve a leer.
* Idempotente: antes de escribir se borra el _periodo del archivo en su tabla.

Parametros (posicionales): catalog schema ruta_base periodo
  periodo = "ultimo" (ultima carpeta AAAA/MM) | "todos" | "AAAA-MM"
"""
import os
import re
import struct
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F, types as T

spark = SparkSession.builder.getOrCreate()
# Fechas del manifiesto en UTC para comparar con os.stat sin desfase de zona horaria
spark.conf.set("spark.sql.session.timeZone", "UTC")

IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
LOTE = 100_000  # filas por escritura


def nombre_tabla(archivo: str) -> str:
    base = os.path.splitext(archivo)[0].lower()
    base = re.sub(r"[^a-z0-9]+", "_", base).strip("_")
    return f"clientes_base_bronce_{base}"


def leer_encabezado(f):
    h = f.read(32)
    nrec, hlen, rlen = struct.unpack("<IHH", h[4:12])
    campos = []
    while True:
        d = f.read(32)
        if not d or d[0] == 0x0D:
            break
        nombre = d[:11].split(b"\x00")[0].decode("latin1").strip().upper()
        campos.append((nombre, d[16]))
    return nrec, hlen, rlen, campos


def decodificar(b: bytes) -> str:
    try:
        return b.decode("utf-8")
    except UnicodeDecodeError:
        return b.decode("latin1")


def leer_dbf(ruta):
    """Devuelve (campos, generador de lotes de tuplas de strings)."""
    f = open(ruta, "rb")
    nrec, hlen, rlen, campos = leer_encabezado(f)
    cortes, pos = [], 1
    for _, largo in campos:
        cortes.append((pos, pos + largo))
        pos += largo

    def lotes():
        try:
            f.seek(hlen)
            lote = []
            for _ in range(nrec):
                rec = f.read(rlen)
                if len(rec) < rlen or rec[:1] == b"\x1a":
                    break
                if rec[:1] == b"*":  # registro borrado
                    continue
                fila = []
                for a, b in cortes:
                    v = decodificar(rec[a:b]).strip()
                    fila.append(v if v != "" else None)
                lote.append(tuple(fila))
                if len(lote) >= LOTE:
                    yield lote
                    lote = []
            if lote:
                yield lote
        finally:
            f.close()

    return [c for c, _ in campos], nrec, lotes()


def carpetas_periodo(ruta_base, periodo):
    encontradas = []
    for anio in sorted(os.listdir(ruta_base)):
        if not re.fullmatch(r"\d{4}", anio):
            continue
        for mes in sorted(os.listdir(os.path.join(ruta_base, anio))):
            if re.fullmatch(r"\d{2}", mes):
                encontradas.append((f"{anio}-{mes}", os.path.join(ruta_base, anio, mes)))
    if not encontradas:
        raise ValueError(f"No hay carpetas AAAA/MM en {ruta_base}")
    if periodo == "todos":
        return encontradas
    if periodo == "ultimo":
        return [encontradas[-1]]
    sel = [e for e in encontradas if e[0] == periodo]
    if not sel:
        raise ValueError(f"No existe la carpeta del periodo {periodo}")
    return sel


def main(catalog, schema, ruta_base, periodo):
    for v in (catalog, schema):
        if not IDENT.match(v):
            raise ValueError(f"Identificador invalido: {v}")
    fq = f"`{catalog}`.`{schema}`"
    manifiesto = f"{fq}.clientes_base_bronce_manifiesto"
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {manifiesto} (
              ruta STRING, periodo STRING, tabla STRING, tamano BIGINT,
              modificado TIMESTAMP, filas BIGINT, fecha_ingesta TIMESTAMP)
            COMMENT 'clientes_base: control de archivos DBF ya cargados al bronce'"""
    )
    cargados = {
        (r.ruta, r.tamano, r.modificado)
        for r in spark.table(manifiesto).select("ruta", "tamano", "modificado").collect()
    }

    for per, carpeta in carpetas_periodo(ruta_base, periodo):
        for archivo in sorted(os.listdir(carpeta)):
            if not archivo.lower().endswith(".dbf"):
                continue
            ruta = os.path.join(carpeta, archivo)
            st = os.stat(ruta)
            modificado = datetime.fromtimestamp(int(st.st_mtime), tz=timezone.utc).replace(tzinfo=None)
            if (ruta, st.st_size, modificado) in cargados:
                print(f"[omitido] {ruta} ya cargado")
                continue

            tabla = f"{fq}.`{nombre_tabla(archivo)}`"
            campos, nrec, lotes = leer_dbf(ruta)
            esquema = T.StructType([T.StructField(c, T.StringType()) for c in campos])
            ahora = datetime.now(timezone.utc).replace(tzinfo=None)
            if spark.catalog.tableExists(f"{catalog}.{schema}.{nombre_tabla(archivo)}"):
                spark.sql(f"DELETE FROM {tabla} WHERE _periodo = '{per}'")

            filas = 0
            for lote in lotes:
                df = (
                    spark.createDataFrame(lote, esquema)
                    .withColumn("_periodo", F.lit(per))
                    .withColumn("_archivo", F.lit(ruta))
                    .withColumn("_fecha_ingesta", F.lit(ahora).cast("timestamp"))
                )
                (df.write.mode("append").option("mergeSchema", "true").saveAsTable(tabla))
                filas += len(lote)
            print(f"[cargado] {ruta} -> {tabla}: {filas} filas (encabezado {nrec})")

            ruta_sql = ruta.replace("'", "''")
            spark.sql(f"DELETE FROM {manifiesto} WHERE ruta = '{ruta_sql}'")
            spark.createDataFrame(
                [(ruta, per, nombre_tabla(archivo), st.st_size, modificado, filas, ahora)],
                spark.table(manifiesto).schema,
            ).write.mode("append").saveAsTable(manifiesto)


if __name__ == "__main__":
    args = sys.argv[1:]
    catalog = args[0] if len(args) > 0 else "datosami"
    schema = args[1] if len(args) > 1 else "datos"
    ruta_base = args[2] if len(args) > 2 else "/Volumes/datosami/datos/clientes_base"
    periodo = args[3] if len(args) > 3 else "ultimo"
    main(catalog, schema, ruta_base, periodo)
