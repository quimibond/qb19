<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.calibration`

**Calibración de equipo de medición (P-C03)** (Model). Hereda de: `mail.activity.mixin`, `mail.thread`.

Calibración o verificación de un equipo de medición (P-C03) con resultado y certificado. Un resultado fuera de tolerancia levanta la NC ligada; la fecha siguiente alimenta el cron de vencimientos.

Orden: `date desc, id desc`.

Archivos: `addons/quimibond_sgi/models/sgi_calibration.py`, `addons/quimibond_sgi/models/sgi_format_map.py`.

## Campos (11)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `calibration_type` | Selection | Tipo | Interna (la hace personal de Quimibond), externa (laboratorio) o verificación: la revisión periódica del equipo de laboratorio. La verificación no cambia las fechas de calibración; fuera de tolerancia bloquea el equipo igual que una calibración. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_calibration.py:189` |
| `certificate_file` | Binary | Certificado (PDF) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_calibration.py:203` |
| `certificate_filename` | Char | Nombre del certificado |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_calibration.py:204` |
| `certificate_ref` | Char | N° de certificado |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_calibration.py:200` |
| `date` | Date | Fecha | Fecha en que se hizo la calibración. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_calibration.py:186` |
| `equipment_id` | Many2one | Equipo | Equipo de medición calibrado. | sí | `maintenance.equipment` |  |  | `addons/quimibond_sgi/models/sgi_calibration.py:183` |
| `next_date` | Date | Próxima calibración | Fecha de la siguiente calibración. Se calcula con el intervalo del equipo; se puede cambiar. |  |  | compute `_compute_next_date`, guardado |  | `addons/quimibond_sgi/models/sgi_calibration.py:211` |
| `notes` | Text | Notas |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_calibration.py:210` |
| `provider_id` | Many2one | Laboratorio / Proveedor | Laboratorio o proveedor que calibró. |  | `res.partner` |  |  | `addons/quimibond_sgi/models/sgi_calibration.py:198` |
| `result` | Selection | Resultado | Conforme o fuera de tolerancia. Fuera de tolerancia levanta una NC. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_calibration.py:205` |
| `sgi_alert_id` | Many2one | NC generada | NC que se levantó por una calibración fuera de tolerancia. |  | `quality.alert` |  |  | `addons/quimibond_sgi/models/sgi_calibration.py:215` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `create` | — |
