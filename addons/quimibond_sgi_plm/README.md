# Quimibond SGI - Puente PLM

Satélite de `quimibond_sgi` (`auto_install` con `mrp_plm`).

- **Qué agrega:** en el cambio de ingeniería (ECO), la casilla «Requiere
  PPAP». Al aplicar un ECO marcado, crea el expediente PPAP (motivo: cambio
  de ingeniería), le liga el AMEF y el plan de control del ECO y agenda una
  actividad al Jefe MAST; si el cambio pide aviso al cliente, agenda otra al
  equipo de ventas.
- **Usa del núcleo:** `sgi.ppap`, `sgi.fmea`, `sgi.control.plan`,
  `sgi.cron._sgi_schedule` y `_sgi_manager_user_id`. Los elementos del PPAP
  se buscan por `sequence` de la plantilla (6 = AMEF, 7 = plan de control);
  si cambia el catálogo de elementos, revisar aquí (A-021).
- **Pendiente:** cuando PPAP salga a un satélite automotriz, este puente
  cambia de dependencia.
- **Pestaña «SGI»** del ECO (3.1.0, revisión de vistas V-B03; antes «SGI -
  Control de cambios»).
- **Depende de:** `quimibond_sgi`, `mrp_plm`. Sin pruebas propias.
