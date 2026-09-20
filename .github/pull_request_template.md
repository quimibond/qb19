## Resumen

<!-- Qué cambia y por qué, en 2-4 líneas. -->

## Validación

El CI corre lint y `tools/check_addons.py`; **las pruebas de Odoo NO corren en
CI** — por eso este checklist es obligatorio para cambios en addons:

- [ ] `python3 -m py_compile` de los .py tocados
- [ ] `flake8 addons/` con las excepciones del repo, sin hallazgos
- [ ] XML parseados (todos los tocados)
- [ ] `python3 tools/check_addons.py --base-ref origin/main` → 0 errores, 0 advertencias
- [ ] Versión del manifest subida (o el módulo está en `tools/no_bump.txt` con motivo)
- [ ] **Tests de Odoo corridos en el shell de Odoo.sh** para los módulos tocados
      (`odoo-bin ... --test-tags /quimibond_sgi --stop-after-init` o equivalente),
      o justificado aquí por qué no aplica:

## Notas de despliegue

<!-- Migraciones, datos noupdate, pasos manuales post-update, riesgos. -->
