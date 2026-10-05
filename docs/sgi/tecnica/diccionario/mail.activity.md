<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `mail.activity`

Modelo de otra app que el SGI extiende.

56.37.0 (G-004, G-005, G-025): los avisos de los crons del SGI llevan una clave estable por registro y un episodio.

Archivos: `addons/quimibond_sgi/models/sgi_cron.py`, `addons/quimibond_sgi/models/sgi_nonconformity.py`.

## Campos (4)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_cron_key` | Char | Clave del aviso (SGI) | Clave estable del aviso automático del SGI sobre este registro. |  |  |  |  | `addons/quimibond_sgi/models/sgi_cron.py:53` |
| `sgi_cron_kind` | Char | Clase del aviso (SGI) |  |  |  | compute `_compute_sgi_cron_kind`, guardado |  | `addons/quimibond_sgi/models/sgi_cron.py:66` |
| `sgi_cron_run` | Char | Última corrida que lo vio (SGI) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_cron.py:59` |
| `sgi_episode_closed` | Boolean | Episodio cerrado (SGI) | La causa del aviso ya se resolvió; si vuelve, nace otro aviso. |  |  |  |  | `addons/quimibond_sgi/models/sgi_cron.py:56` |

