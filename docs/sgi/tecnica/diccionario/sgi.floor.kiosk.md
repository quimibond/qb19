<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.floor.kiosk`

**SGI en planta: servicios de la pantalla de la tableta** (AbstractModel).

Servicios de la pantalla «SGI en planta». Cada método público valida la tableta, a la persona y su PIN, y escribe con sudo a nombre de la persona.

Archivos: `addons/quimibond_sgi/models/sgi_floor_kiosk.py`.

## Métodos públicos (10)

| Método | Qué hace (docstring) |
|---|---|
| `kiosk_ack_document` | — |
| `kiosk_check_pin` | — |
| `kiosk_checklist_save` | Guarda respuestas y notas, «Marcar el resto como Bien» y, con ``finish``, firma la hoja a nombre de la persona. Todo va en una transacción: si la firma falla («Faltan…»), lo de la misma llamada no qu… |
| `kiosk_checklists` | — |
| `kiosk_document_file` | — |
| `kiosk_employees` | — |
| `kiosk_epp` | — |
| `kiosk_pending_docs` | — |
| `kiosk_report_near_miss` | — |
| `kiosk_sign_epp` | — |
