# Quimibond SGI - Mapa de procesos

Módulo de **datos** de `quimibond_sgi` (decisión 6: el SGI se instala vacío).
Trae el mapa de procesos de producción en `data/mapa.json` (14 procesos, sus
etapas, actividades con numeral congelado, roles, entregables, entradas,
familias de puestos, indicadores con términos de fórmula, objetivos y planes
de control).

- **No se instala en producción.** Solo en staging o en una recuperación.
  Instalarlo no crea registros.
- **Carga:** SGI → Administración SGI → Configuración → «Cargar mapa de
  procesos» (menú `menu_sgi_mapa_load`), solo para Administrador SGI.
  Siempre **Probar** primero (modo de prueba, no escribe), leer el reporte y
  luego **Cargar**. Usa `sgi.process.load_payload`; todo va por llave
  natural (clave de proceso, numeral, código, nombre de puesto), así que
  cargar dos veces no duplica.
- **Regenerar:** el mismo asistente descarga el mapa de la base
  (`sgi.process.export_payload`). `tools/generar_mapa.py` es el script con el
  que se armó el primero desde lecturas de producción.
- **Depende de:** `quimibond_sgi`.
- **Pruebas:** `tests/test_mapa.py`.
