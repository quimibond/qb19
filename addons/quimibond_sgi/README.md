# quimibond_sgi — Sistema de Gestión Integral de PNTQ

Addon de Odoo 19 Enterprise con el SGI de Productora de No Tejidos Quimibond
(empresa 1 de la instancia, decisión D-03). Convierte los procedimientos del
Dropbox en **procesos y actividades** con responsable por puesto, vencimiento,
entregable medible y escalamiento, y extiende las apps nativas (Documentos,
Calidad, Aprobaciones, Helpdesk, Proyecto, Mantenimiento, Encuestas, Firmas)
sin duplicarlas. Versión y cambios: `CHANGELOG.md` (una entrada por versión).

**Normas** (D-28; organismo SIDE Certificaciones, acreditado ante la ema):

| Norma | Estado |
|---|---|
| ISO 9001:2015 | Certificada desde el 24-ago-2021; certificado 24070237A vigente al 22-ago-2027 |
| ISO 14001:2015 | Certificada desde el 3-jul-2023; el certificado vigente lo confirma MAST |
| ISO 45001:2018 | En certificación (auditoría del 22 al 24-sep-2026, resultado pendiente) |
| IATF 16949 | No certificada. Lo que piden los clientes automotrices va en «Requisitos específicos de clientes» |

## Qué trae y qué no

- **Se instala vacío** (decisión 6): trae estructura (modelos, vistas, menús,
  grupos, secuencias, tipos de documento, áreas, normas ISO con sus cláusulas,
  catálogo de indicadores automáticos, crons y parámetros), **no** el mapa de
  procesos. El mapa de producción está en `quimibond_sgi_mapa` y solo se
  instala en staging o en una recuperación; su carga es manual, con modo de
  prueba primero.
- **Estructura contra datos** (decisión 4): procesos, actividades, roles y
  entregables son **dato** que se captura en pantalla o por carga
  (`sgi.process.load_payload`), nunca XML del núcleo. La norma «Requisitos
  específicos de clientes» y sus cláusulas CLI-01…CLI-10 existen solo en
  producción, sin XML ID; no se agregan por XML sin una migración que cree
  antes los XML IDs de los registros existentes (A-029). Las cláusulas de
  tercer nivel de ISO 9001, 14001 y 45001 (57.97.0) sí traen XML ID
  (`data/sgi_norms_tercer_nivel.xml`); las CLI siguen sin él.
- **Claves:** en pantalla solo la clave nueva (`PR-{proceso}`, `F-{proceso}-{nn}`,
  `IT-…`, `DA-…`; D-02). La clave del Dropbox solo aparece en «Del Dropbox a
  Odoo» (decisión 1).

## Glosario de pantallas

Así se escriben los términos del SGI en etiquetas, ayudas y avisos
(revisión de vistas V-B04, 57.81.0). Todo el texto va en «usted».

| Se escribe | No | Nota |
|---|---|---|
| no conformidad (NC); en plural, «no conformidades» o «NC» | No Conformidad, NCs | Mayúscula solo al inicio de un título o una oración |
| certificado de calidad (CoA) | COA, certificado de análisis | |
| indicador | KPI | También «indicadores en rojo», «indicadores automáticos» |
| casi accidente | casi-accidente | |
| Jefe MAST | Jefe de MAST | «MAST» solo, para el área |
| Revisión por la dirección | Revisión por la Dirección | «Dirección» con mayúscula es el grupo que aprueba |
| Sin sufijo «SGI» dentro de la app SGI | «Áreas SGI», «(SGI)» | En fichas de otras apps (contacto, empleado, equipo…), la pestaña se llama «SGI» |
| Descartar | Volver, Cancelar | Botón para salir de un asistente sin hacer nada |

## Menú

Cinco entradas bajo **SGI**: Inicio (Mis pendientes, Mi procedimiento,
Documentos vigentes, Mis indicadores, Mi equipo), Procesos (mapa, actividades,
matriz de responsabilidades, «Del Dropbox a Odoo»), Mejora (NC, reclamaciones,
acciones, mejora continua, auditorías), Seguridad y ambiente, Dirección y
Administración SGI. El árbol completo con grupos está en
`tools/sgi_menu_tree.txt`, y `tests/test_menu_tree.py` lo compara con la base.

## Grupos

| Grupo | Para quién |
|---|---|
| Usuario SGI | Todo el personal con usuario: sus pendientes, su procedimiento, levantar NC |
| Dueño de proceso (SGI) | Jefes que son dueños de un proceso (implica Usuario) |
| Captura de eficiencias | Jefe o supervisor de área |
| Jefe MAST y SGI | Quien administra el SGI |
| Administrador SGI | Carga del catálogo y cambios de estructura (implica Jefe MAST) |
| Dirección de Operaciones (SGI) | Consulta y aprueba; **no** implica Jefe MAST |
| Auditor SGI | Lectura para auditar (D-05); no ve salud ni salarios |
| Salud ocupacional (SGI) | Solo el Coordinador de RH: exámenes y estudios de higiene |
| Salarios de eficiencias (SGI) | Importes de eficiencias (RH y Nóminas) |
| Comisión de Seguridad e Higiene (SGI) | Recorridos y hallazgos de la CSH |
| Tableta de planta (SGI) | Cuenta compartida de una tableta: solo la app SGI en planta; firma a nombre de quien teclea su PIN |

**Capturista de planta** = persona sin usuario que firma en la tableta (app
SGI en planta) con su PIN de empleado; las cuentas de tableta se dan de alta
en Configuración → Tabletas de planta. El PIN no tiene límite de intentos; se
acepta en planta (F-018). **Apagado por ahora** (decisión de Dirección,
2026-10-02): no hay tabletas dadas de alta; encenderlo pide antes contestar el
límite de intentos (Q12).

## Satélites

| Módulo | Qué agrega | Se instala |
|---|---|---|
| `quimibond_sgi_mapa` | El mapa de procesos de producción (JSON) y el asistente de carga | A mano, solo staging o recuperación |
| `quimibond_sgi_pesaje` | Rollo fuera de tolerancia → alerta de calidad; parámetro `pesaje_tolerance_kg` | `auto_install` con `pesaje_rollos_tejido` |
| `quimibond_sgi_plm` | ECO que requiere PPAP → expediente PPAP y aviso a ventas | `auto_install` con `mrp_plm` |
| `quimibond_sgi_revisado` | Pareto de defectos del revisado e indicador MA-03 «Calidad PQ» | `auto_install` con `mrp_revisado_telas` |
| `quimibond_sgi_knowledge` | Instructivo escrito en Conocimiento y publicado como IT | `auto_install` con `knowledge` |
| `quimibond_sgi_studio` | Regla de aprobación de Studio para el rol «Aprueba» | `auto_install` con `web_studio` |

**Lo que el núcleo garantiza a los satélites** (A-021; no cambiar sin revisar
los satélites): `quality.alert.sgi_auto_create(source_code, vals)`,
`sgi.cron._sgi_schedule(...)`, `sgi.cron._sgi_manager_user_id()`, los XML IDs
de los equipos de calidad (`sgi_quality_team_internal`) y del indicador
`sgi_ind_calidad_pq`. Fuera de los satélites, `quimibond_intelligence`
(señales) lee modelos `sgi.*`; al renombrar o sacar un modelo, revisar
`quimibond_intelligence/models/senales/` (K-020).

## Instalar, actualizar y probar

```bash
# Odoo.sh, shell de la rama. Varios módulos se separan con COMAS, sin espacios.
odoo-update quimibond_sgi,quimibond_sgi_pesaje && odoosh-restart http && odoosh-restart cron
```

- Verificaciones después del despliegue: `docs/RUNBOOK_DESPLIEGUE.md`, sección SGI.
- **Pruebas:** el CI de GitHub no instala el SGI (depende de Enterprise). Se
  corren en el build de desarrollo de Odoo.sh de la rama, **solo con
  `--test-tags`**, porque el suite completo se detiene en las pruebas de
  nómina:
  `odoo-bin -u quimibond_sgi --test-tags /quimibond_sgi --stop-after-init --no-http`
  (agregar `,/quimibond_sgi_pesaje`… para los satélites).
- `post_init_hook` (`__init__.py`): al instalar, liga los puntos de control de
  los equipos «CALIDAD Materia Prima» y «Revisado de Tela» a los planes de
  control del módulo; busca por nombre y nunca detiene la instalación (A-030).

## Reglas para programar aquí

- **Versión y CHANGELOG:** todo cambio en `addons/quimibond_sgi/` sube la
  versión y agrega su `## <versión>` en `CHANGELOG.md` en el mismo commit
  (`tools/check_addons.py` lo exige).
- **Sin herencias propias:** el SGI no hereda sus propias vistas (0 desde
  57.30.0). La ficha propia va completa en un solo `<record>`; la herencia es
  para vistas de otros módulos. Ver `CLAUDE.md` de la raíz.
- Menús: todos en `views/sgi_menus.xml`, al final del manifest; si cambian,
  cambia `tools/sgi_menu_tree.txt` en el mismo commit.
- Crons en `noupdate`: cambiar uno en la base requiere migración.
- **Nombres** (A-024, C-020; no renombrar lo existente): modelos y campos en
  inglés, etiquetas en español; en lo nuevo, NC ligada = `alert_id`, área =
  `area_id` (M2O a `sgi.area`), responsable = `responsible_id`, revisión
  `Integer`, sin prefijo `sgi_` en campos de modelos propios.
- Numerales de actividad (`number`): no se renumeran; son la llave natural de
  la carga y la exportación. Los huecos son esperados (H-017).
- Campos calculados sin `store`: ninguno se usa donde Odoo exige almacenarlo
  (C-023). Si Mis pendientes se vuelve lento, el candidato es `store=True` en
  `quality.alert.sgi_stage_is_closing/is_cancel`.
- Adjuntos de salud (`sgi.health.record`): pendiente de verificar al capturar
  el primero que el PDF quede ligado al registro (F-019).
- `help` de campos en «usted» (D-29), para la persona que llena el campo.

## Documentación

| Qué | Dónde |
|---|---|
| Manuales por rol, administración y transición | `docs/sgi/` |
| Técnica generada del código (modelos, campos, vistas, menús, crons) | `docs/sgi/tecnica/` (`python3 tools/sgi_docs.py`) |
| Decisiones de Jose | `docs/audit/decisiones.md` |
| Historia (README anterior, manuales viejos) | `docs/historico/sgi/` |

Licencia: OPL-1 (D-31). Autor: Quimibond.
