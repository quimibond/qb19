<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.floor.tablet`

**Tableta de planta (SGI en planta)** (Model).

Tableta de planta: una cuenta compartida, sus departamentos y sus checklists.

Orden: `name`.

Archivos: `addons/quimibond_sgi/models/sgi_floor_kiosk.py`.

## Campos (7)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `active` | Boolean |  | Archive la tableta para que deje de abrir SGI en planta sin borrarla (lo firmado en ella la sigue citando). |  |  |  |  | `addons/quimibond_sgi/models/sgi_floor_kiosk.py:76` |
| `checklist_template_ids` | Many2many | Checklists que se llenan aquí | Las hojas de estas plantillas salen en «Checklist de mi equipo» de la tableta. |  | `sgi.checklist.template` |  |  | `addons/quimibond_sgi/models/sgi_floor_kiosk.py:69` |
| `company_id` | Many2one | Empresa | Empresa del SGI: solo su gente sale en la tableta. | sí | `res.company` |  |  | `addons/quimibond_sgi/models/sgi_floor_kiosk.py:73` |
| `department_ids` | Many2many | Departamentos | Las personas de estos departamentos (y de sus subdepartamentos) salen en la pantalla y pueden firmar en esta tableta. | sí | `hr.department` |  |  | `addons/quimibond_sgi/models/sgi_floor_kiosk.py:64` |
| `name` | Char | Tableta | Nombre que ve la gente y que queda en lo firmado: «Tableta Tejido», «Tableta Mantenimiento». | sí |  |  |  | `addons/quimibond_sgi/models/sgi_floor_kiosk.py:54` |
| `note` | Text | Dónde está | Lugar de la tableta y quién la cuida. |  |  |  |  | `addons/quimibond_sgi/models/sgi_floor_kiosk.py:79` |
| `user_id` | Many2one | Cuenta de la tableta | Usuario compartido con el que se abre la tableta (por ejemplo supervisor@). No debe estar ligado a un empleado: lo firmado queda a nombre de quien teclea su PIN. Recibe el grupo «Tableta de planta (SGI)» al guardar. Pida a Sistemas que ponga «SGI en planta» como su acción de inicio y que le quite l… | sí | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_floor_kiosk.py:57` |

## Métodos públicos (2)

| Método | Qué hace (docstring) |
|---|---|
| `create` | — |
| `write` | — |
