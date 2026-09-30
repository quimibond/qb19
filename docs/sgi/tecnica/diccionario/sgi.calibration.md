<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.calibration`

**Calibración de equipo de medición (P-C03)** (Model). Hereda de: `mail.activity.mixin`, `mail.thread`.

Orden: `date desc, id desc`.

Archivos: `addons/quimibond_sgi/models/sgi_calibration.py`.

## Campos (11)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `calibration_type` | Selection | Tipo |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_calibration.py:137` |
| `certificate_file` | Binary | Certificado (PDF) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_calibration.py:145` |
| `certificate_filename` | Char | Nombre del certificado |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_calibration.py:146` |
| `certificate_ref` | Char | N° de certificado |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_calibration.py:142` |
| `date` | Date | Fecha |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_calibration.py:135` |
| `equipment_id` | Many2one | Equipo |  | sí | `maintenance.equipment` |  |  | `addons/quimibond_sgi/models/sgi_calibration.py:133` |
| `next_date` | Date | Próxima calibración |  |  |  | compute `_compute_next_date`, guardado |  | `addons/quimibond_sgi/models/sgi_calibration.py:152` |
| `notes` | Text | Notas |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_calibration.py:151` |
| `provider_id` | Many2one | Laboratorio / Proveedor |  |  | `res.partner` |  |  | `addons/quimibond_sgi/models/sgi_calibration.py:141` |
| `result` | Selection | Resultado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_calibration.py:147` |
| `sgi_alert_id` | Many2one | NC generada |  |  | `quality.alert` |  |  | `addons/quimibond_sgi/models/sgi_calibration.py:154` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `create` | — |
