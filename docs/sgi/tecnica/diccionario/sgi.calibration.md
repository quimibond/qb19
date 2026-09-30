<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.calibration`

**Calibración de equipo de medición (P-C03)** (Model). Hereda de: `mail.activity.mixin`, `mail.thread`.

Calibración o verificación de un equipo de medición (P-C03) con resultado y certificado. Un resultado fuera de tolerancia levanta la NC ligada; la fecha siguiente alimenta el cron de vencimientos.

Orden: `date desc, id desc`.

Archivos: `addons/quimibond_sgi/models/sgi_calibration.py`.

## Campos (11)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `calibration_type` | Selection | Tipo |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_calibration.py:140` |
| `certificate_file` | Binary | Certificado (PDF) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_calibration.py:148` |
| `certificate_filename` | Char | Nombre del certificado |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_calibration.py:149` |
| `certificate_ref` | Char | N° de certificado |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_calibration.py:145` |
| `date` | Date | Fecha |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_calibration.py:138` |
| `equipment_id` | Many2one | Equipo |  | sí | `maintenance.equipment` |  |  | `addons/quimibond_sgi/models/sgi_calibration.py:136` |
| `next_date` | Date | Próxima calibración |  |  |  | compute `_compute_next_date`, guardado |  | `addons/quimibond_sgi/models/sgi_calibration.py:155` |
| `notes` | Text | Notas |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_calibration.py:154` |
| `provider_id` | Many2one | Laboratorio / Proveedor |  |  | `res.partner` |  |  | `addons/quimibond_sgi/models/sgi_calibration.py:144` |
| `result` | Selection | Resultado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_calibration.py:150` |
| `sgi_alert_id` | Many2one | NC generada |  |  | `quality.alert` |  |  | `addons/quimibond_sgi/models/sgi_calibration.py:157` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `create` | — |
