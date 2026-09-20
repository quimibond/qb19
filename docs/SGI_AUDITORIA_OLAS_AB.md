# SGI — Mapa de hallazgos G· y su tratamiento (Olas A + B)

> **Procedencia y alcance de este documento.**
> Este mapa está **reconstruido a partir de la evidencia versionada en el
> propio repo** (mensajes de commit de `9c91dd0`, `f0b58ac`, `ec36895`, los
> archivos de test `test_ola_a_*`, `test_ola_b`, y las referencias `G··` en el
> código). El **informe de auditoría narrativo original** que enumeró los
> hallazgos `G1..Gn` se produjo en otra sesión de trabajo
> (`session_01BoaSU8CNsJgxTicQ1tH4AU`) y **no quedó versionado en este repo**;
> por eso aquí sólo se documentan con certeza los hallazgos que el código
> referencia de forma explícita. Los huecos de numeración se señalan como
> **pendientes de recuperar del informe original** (ver §4), para que la
> revisora sepa exactamente qué está confirmado y qué falta.
>
> Numeración: los hallazgos `G··` son los de esta auditoría (Olas A/B). Son
> **distintos** de los `H1..H22` que originaron las Olas 0/1/2 (motor de mejora,
> línea dorada, cascada); no se mezclan.

---

## 1. Hallazgos confirmados y resueltos en Ola A + Ola B

| G· | Hallazgo (riesgo) | Cláusula ISO | Fix | Evidencia (código) | Test |
|----|-------------------|--------------|-----|--------------------|------|
| **G11** | Un objetivo integral podía crearse **sin política** que lo encabece: la cascada general→particular tenía fugas en el eslabón política→objetivo. | 9001/14001 6.2 | Un objetivo nuevo **hereda por defecto la política vigente** (versión suave: `default`, no `required`, para no romper el `-u` con objetivos históricos sin liga). | `sgi_objective.py` (default `policy_id` = política vigente) | `test_ola_b.py` B.2 |
| **G12** | Un incidente SST **grave/fatal** podía quedarse **sin No Conformidad**: la investigación SCAT y las acciones correctivas no tenían candado que las forzara. | 45001 10.2 | El incidente grave/fatal **fuerza una NC mayor** ligada en ambos sentidos (`quality.alert.sgi_incident_id ↔ sgi.incident.sgi_alert_id`), idempotente. Cerrar el incidente exige el **IPER ligado** (`risk_id`) para cerrar la cadena SST→riesgo. | `sgi_incident.py` (`_sgi_create_alert`, `_sgi_check_can_close`) | `test_ola_b.py` B.1 |
| **G13** | El cierre de una **NC mayor** no atestiguaba que la **lección** se hubiera aplicado al AMEF / plan de control / documento: el aprendizaje se perdía. | IATF 10.2.3 | `quality.alert.sgi_lesson_captured` (con tracking): **candado real** que exige atestiguar la lección aplicada antes de cerrar una NC mayor. | `sgi_nonconformity.py` (`sgi_lesson_captured`, `_sgi_check_can_close`) | `test_ola_b.py` B.3 |
| **G14** | **Procedimiento vivo vs PDF controlado**: al editar las actividades/responsabilidades como datos, el PDF impreso (revisión aprobada) dejaba de coincidir sin que nadie lo notara. | 9001 7.5.2 | Sello de integridad: `documents.document.sgi_procedure_dirty` (+ `_since` / `_by`). Editar el cuerpo del procedimiento marca el documento **VIGENTE** como "pendiente de revisión" con aviso único al dueño y banner en el formulario; se limpia al aprobar una nueva revisión. | `sgi_document.py`, `sgi_process_procedure.py` (`_sgi_flag_procedure_dirty`) | `test_ola_a_procedure.py` |
| **G21** | La **salud** de política/objetivo (semáforo por agregación) no se recomputaba al cambiar los datos aguas abajo: podía quedar en un color viejo. | 9001 9.1 | `policy/objective._compute_health` con `@api.depends` → **recomputo reactivo**. | `sgi_policy.py`, `sgi_objective.py` | `test_ola_b.py` B.4 |

## 2. Trabajo de Ola A sin número G· explícito en el código

Estos dos frentes de la Ola A **no llevan una etiqueta `G··` en el código**;
se documentan por su cláusula ISO y su evidencia. Si en el informe original
tenían número (probablemente en el rango de "blindaje de evidencia" y
"calibración"), **debe confirmarse contra ese informe** (§4).

| Frente | Hallazgo (riesgo) | Cláusula | Fix | Evidencia | Test |
|--------|-------------------|----------|-----|-----------|------|
| **A.1 — Blindaje de evidencia** | Las líneas de evidencia (AMEF, PPAP, hallazgos de auditoría) se podían **borrar** aun con el padre publicado/cerrado: la evidencia dejaba de ser inmutable. | 9001 7.5.3 / IATF | Guardas de `unlink` por estado en `sgi.fmea.line`, `sgi.ppap.element`, `sgi.audit.finding`: no se borran con el padre publicado (AMEF vigente/obsoleto, PPAP aprobado, auditoría cerrada). En borrador el equipo edita libre; **MAST exento**. ACL del transitorio de cierre forzado de NC a solo-manager. | `sgi_fmea.py`, `sgi_ppap.py`, `sgi_audit.py` | `test_ola_a_security.py` (6) |
| **A.2 — Bloqueo real de calibración** | Se podía **dictaminar** (pass/fail) una inspección con un instrumento **bloqueado** (fuera de tolerancia) o con **calibración vencida**. | IATF 7.1.5.2.1 | Nuevo `sgi_equipment_id` en `quality.point` y `quality.check` (heredado del punto, editable) + constraint que impide dictaminar con gauge bloqueado o calibración vencida (evaluada en línea contra hoy, aplica aunque el cron no haya corrido). Sin equipo configurado no bloquea (retro-compatible). | `sgi_calibration.py`, quality.point/quality.check | `test_calibration.py` (bloqueo, 5) |

## 3. Cierre de feedback de la revisora (post-aprobación Olas A+B)

Dos huecos detectados por la revisora tras aprobar A+B, corregidos en
`v19.0.9.1.0`:

| # | Hueco | Fix |
|---|-------|-----|
| Fix 1 | La **semilla** `seed_procedure_ventas()` disparaba G14 (falso positivo): cargar la Rev.15 vigente marcaba el procedimiento como "divergente". | `_sgi_flag_procedure_dirty` sale temprano con contexto `sgi_bypass_dirty=True`; la semilla corre todas sus escrituras bajo ese contexto (es la revisión vigente, no una divergencia). |
| Fix 2 | **Escalar** la severidad de un incidente (leve/moderado → grave/fatal) en la investigación **nunca** generaba su NC: `_sgi_create_alert` sólo corría en `create`. | `write` en `sgi.incident`: al reclasificar a grave/fatal se llama `_sgi_notify_if_serious()` + `_sgi_create_alert()` para los registros que antes no eran graves (ambos idempotentes). |

## 4. Pendiente — recuperar del informe original

Para completar el mapa `G1..Gn` la revisora necesita del informe de auditoría
original (sesión `session_01BoaSU8CNsJgxTicQ1tH4AU`), que **no está en el repo**:

- Los hallazgos **G1–G10, G15–G20** (y cualquiera > G21): número, enunciado y
  severidad. No son recuperables desde el código y no se inventan aquí.
- El **número G·** exacto de los frentes A.1 (evidencia) y A.2 (calibración).
- La **priorización** original (qué quedó para las olas restantes).

Acción sugerida: pegar aquí el informe original o exportarlo a
`docs/` como fuente de verdad; este archivo se actualiza entonces con los
enunciados verbatim y se elimina esta sección.
