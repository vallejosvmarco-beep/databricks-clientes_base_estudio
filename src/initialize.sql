-- Inicializacion de clientes_base: solo verificacion.
-- No se crean esquemas ni volumenes (sin permiso CREATE SCHEMA en datosami):
-- las tablas clientes_base_* viven en datosami.datos y los DBF en el volumen
-- existente datosami.datos.clientes_base (subcarpetas AAAA/MM).
DESCRIBE SCHEMA datosami.datos;
DESCRIBE VOLUME datosami.datos.clientes_base;
