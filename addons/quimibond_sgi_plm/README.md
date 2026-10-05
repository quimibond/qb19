# Quimibond SGI - Puente PLM

Satélite de `quimibond_sgi` (`auto_install` con `mrp_plm`).

- **Qué agrega:** en el cambio de ingeniería (ECO), la pestaña «SGI» con la
  casilla «Requiere PPAP». El SGI la marca solo (y lo dice en el chatter y en
  un aviso de la pestaña) cuando el producto se vendió en los últimos 12 meses
  (parámetro `quimibond_sgi.ppap_sales_window_months`, pedidos confirmados de
  la compañía del ECO) o ya tiene PPAP con un cliente marcado «Exige PPAP ante
  cambios» en esa compañía; si es un solo cliente, también lo pone como
  «Cliente del PPAP». Solo en ECO sin aplicar y sin la casilla; nunca la
  desmarca. Al aplicar un ECO marcado crea **un PPAP por cliente** (motivo:
  cambio de ingeniería; sin repetir el de un cliente que ya lo tiene), le liga
  el AMEF y el plan de control del ECO y, si no hay cliente ni producto,
  agenda una actividad al Jefe MAST; si el cambio pide aviso al cliente,
  agenda otra al equipo de ventas.
- **Usa del núcleo:** `sgi.ppap`, `sgi.fmea`, `sgi.control.plan`,
  `res.partner.sgi_requires_ppap`, `sgi.cron._sgi_schedule` y
  `_sgi_manager_user_id`. Los elementos del PPAP se buscan por `sequence` de
  la plantilla (6 = AMEF, 7 = plan de control); si cambia el catálogo de
  elementos, revisar aquí (A-021).
- **Pendiente:** cuando PPAP salga a un satélite automotriz, este puente
  cambia de dependencia.
- **Depende de:** `quimibond_sgi`, `mrp_plm`.
- **Pruebas:** `tests/test_eco_ppap.py`; corren en el build de Odoo.sh con
  `--test-tags /quimibond_sgi_plm` (junto con `/quimibond_sgi`).

## Cambios

- **3.2.0 (2026-10-05, SGI 57.100.0, N-14):** «Requiere PPAP» por los clientes
  del producto que lo exigen (nunca se desmarca), un PPAP por cliente al
  aplicar (`sgi_ppap_ids`; `sgi_ppap_id` queda como el primero), botón «PPAP»
  con el número de expedientes, folio en negritas en el chatter (K-07) y
  primeras pruebas propias. Los ECO viejos con `sgi_ppap_id` no se copian a
  `sgi_ppap_ids` (en producción no hay PPAP).
- **3.1.0:** pestaña «SGI» del ECO (revisión de vistas V-B03; antes «SGI -
  Control de cambios»).
