# Quimibond SGI - Puente Pesaje

Satélite de `quimibond_sgi` (`auto_install` con `pesaje_rollos_tejido`).

- **Qué agrega:** al confirmar el pesaje de un rollo **forzando** un peso
  fuera de tolerancia, crea una alerta de calidad ligada a la orden de
  fabricación con `quality.alert.sgi_auto_create('pesaje_rollo_fuera_peso', …)`
  (fuente de NC automática que MAST puede apagar en SGI → Administración SGI →
  Configuración → Fuentes de NC automáticas). Una sola alerta por rollo.
- **Parámetro:** `quimibond_sgi.pesaje_tolerance_kg` (de fábrica 3 kg,
  contra el Tamaño de Rollo Estándar). Lo siembra este módulo al instalarse y
  se edita en Ajustes → SGI → Piso.
- **No cambia** `pesaje_rollos_tejido`: solo agrega el gancho.
- **Depende de:** `quimibond_sgi`, `pesaje_rollos_tejido`.
- **Pruebas:** `tests/test_pesaje_alert.py`.
