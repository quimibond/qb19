<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.alert.source`

**Fuente de NC automática** (Model). Hereda de: `mail.thread`.

Fuente de NC automática (pesaje, calibración, indicador en rojo…). MAST la enciende o apaga sin tocar código; ``quality.alert.sgi_auto_create`` la consulta y cuenta lo suprimido.

Orden: `sequence, code`.

Archivos: `addons/quimibond_sgi/models/sgi_alert_source.py`.

## Campos (10)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `alert_count` | Integer | # NC generadas |  |  |  | compute `_compute_alert_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_alert_source.py:52` |
| `code` | Char | Clave técnica | Identificador que usa el código para pedir permiso antes de levantar la NC. No se edita: lo fija el módulo que la dispara. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_alert_source.py:28` |
| `enabled` | Boolean | Activa | Desactívala para dejar de generar No Conformidades por este motivo. El cambio queda registrado en el historial con autor y fecha, para poder justificarlo en auditoría. |  |  |  |  | `addons/quimibond_sgi/models/sgi_alert_source.py:34` |
| `last_suppressed_on` | Datetime | Última omisión |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_alert_source.py:57` |
| `name` | Char | Fuente |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_alert_source.py:32` |
| `origin_module` | Char | Módulo |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_alert_source.py:50` |
| `sequence` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_alert_source.py:33` |
| `suppressed_count` | Integer | # NC omitidas | Cuántas veces se cumplió la condición mientras la fuente estaba apagada. Sirve para dimensionar lo que se dejó de registrar. |  |  |  |  | `addons/quimibond_sgi/models/sgi_alert_source.py:53` |
| `trigger_note` | Text | Qué la dispara | Descripción en lenguaje de piso de la condición que genera la NC. |  |  |  |  | `addons/quimibond_sgi/models/sgi_alert_source.py:47` |
| `trigger_type` | Selection | Disparo | Automático: lo levanta el sistema solo; al desactivarlo, la NC simplemente no se crea. Manual: lo levanta una persona con un botón; al desactivarlo, el botón avisa que la fuente está apagada en vez de fallar en silencio. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_alert_source.py:39` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_view_alerts` | — |
