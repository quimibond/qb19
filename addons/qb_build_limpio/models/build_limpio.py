# -*- coding: utf-8 -*-
"""Build limpio: corrige en la base lo que pinta de naranja el build de Odoo.sh.

Cada aviso que atiende este módulo nació en el log de un build real
(qbtesting, 17-sep-2026). Ninguno viene del código del repo: vienen de campos
y vistas creados con Odoo Studio, y de columnas a las que Odoo no pudo poner
NOT NULL en alguna actualización. Como la base de cada rama es copia de
producción, se repiten en todas las ramas hasta que alguien los corrige en la
base. Este módulo lo hace al final de cada instalación/actualización
(``_register_hook``, cuando ya está cargado todo el registro), y a mano desde
Ajustes → Técnico → Build limpio.

Todo es idempotente y va en savepoints: un tropiezo en un punto no tumba el
build ni deja el resto sin hacer.
"""
import logging
import re
from collections import defaultdict

from lxml import etree

from odoo import api, models

_logger = logging.getLogger(__name__)

# Módulos "de nadie": lo que está bajo estos nombres se creó en la base
# (Studio, importaciones), no en un addon; es lo único que se toca.
MODULOS_DE_LA_BASE = ('studio_customization', '__export__', '__import__', '__cloc_exclude__')

RE_GROUPS_ATTR = re.compile(r'groups="([^"]*)"')
RE_GROUPS_NODE = re.compile(r'<attribute name="groups">([^<]*)</attribute>')


class QbBuildLimpio(models.AbstractModel):
    _name = 'qb.build.limpio'
    _description = 'Build limpio: corrección de avisos del build'

    # ------------------------------------------------------------------
    # Disparo
    # ------------------------------------------------------------------
    def _register_hook(self):
        super()._register_hook()
        # Odoo llama _register_hook una sola vez al terminar de cargar el
        # registro. Solo interesa cuando la carga instaló o actualizó módulos
        # (un build de Odoo.sh, un `odoo-update`): ahí es donde se emiten los
        # avisos y ahí ya están cargados todos los modelos. En un arranque
        # normal de los workers no hay nada que hacer.
        if not getattr(self.pool, 'updated_modules', None):
            return
        try:
            self.sudo().ejecutar()
        except Exception:  # noqa: BLE001 — jamás tumbar la carga del registro
            _logger.exception("Build limpio: la limpieza falló; el build sigue")

    @api.model
    def ejecutar_y_mostrar(self):
        """Acción de servidor: corre la limpieza y muestra el resultado."""
        informe = self.ejecutar()
        lineas = []
        for clave, valores in informe.items():
            lineas.append('%s: %d' % (clave, len(valores)))
            lineas.extend('  - %s' % v for v in valores[:40])
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': 'Build limpio',
                'message': '\n'.join(lineas) or 'Nada que corregir.',
                'sticky': True,
                'type': 'success' if any(informe.values()) else 'info',
            },
        }

    @api.model
    def ejecutar(self):
        """Corre todas las correcciones. Devuelve {paso: [descripciones]}."""
        informe = {}
        self.env.flush_all()  # los pasos leen con SQL crudo: que vean lo que la ORM tiene pendiente
        for nombre, paso in (
            ('etiquetas_duplicadas', self._etiquetas_duplicadas),
            ('dependencias_no_buscables', self._dependencias_no_buscables),
            ('vistas_studio_invalidas', self._vistas_studio_invalidas),
            ('grupos_inexistentes', self._grupos_inexistentes),
            ('columnas_sin_not_null', self._columnas_sin_not_null),
        ):
            try:
                with self.env.cr.savepoint():
                    informe[nombre] = paso()
            except Exception:  # noqa: BLE001
                _logger.exception("Build limpio: el paso %s falló", nombre)
                informe[nombre] = ['ERROR: ver log']
        self.env.flush_all()
        self.env.registry.clear_cache()
        for nombre, valores in informe.items():
            for v in valores:
                _logger.info("Build limpio [%s]: %s", nombre, v)
        return informe

    # ------------------------------------------------------------------
    # 1. Etiquetas duplicadas
    #    "Two fields (a, b) of model() have the same label: X"
    #    (ir.model.fields._reflect_fields al actualizar un módulo)
    # ------------------------------------------------------------------
    @api.model
    def _etiquetas_duplicadas(self):
        hechos = []
        cr = self.env.cr
        self.env.flush_all()
        for model_name in list(self.env.registry):
            model = self.env[model_name]
            por_etiqueta = defaultdict(list)
            for field in model._fields.values():
                if field.string:
                    por_etiqueta[field.string].append(field)
            duplicadas = {k: v for k, v in por_etiqueta.items() if len(v) > 1}
            if not duplicadas:
                continue
            # Los campos de Studio no tienen módulo: el de menor id es el
            # original y se queda con su nombre; los demás se renombran.
            # Los heredados por _inherits (product.product ← product.template,
            # account.bank.statement.line ← account.move) se corrigen en el
            # modelo que los define: ahí está la etiqueta de verdad.
            cr.execute("SELECT name, id FROM ir_model_fields WHERE model = %s AND state = 'manual'", (model_name,))
            orden = dict(cr.fetchall())
            usadas = set(por_etiqueta)
            for etiqueta, campos in duplicadas.items():
                manuales = sorted((f for f in campos if f.manual and not f.inherited and f.name in orden),
                                  key=lambda f: orden[f.name])
                if not manuales:
                    continue  # dos campos de código: se arregla en el addon, no aquí
                if len(manuales) == len(campos):
                    manuales = manuales[1:]  # todos de Studio: el más viejo conserva la etiqueta
                for field in manuales:
                    n = 2
                    while '%s (%d)' % (etiqueta, n) in usadas:
                        n += 1
                    nueva = '%s (%d)' % (etiqueta, n)
                    usadas.add(nueva)
                    self._renombrar_campo(model_name, field.name, ' (%d)' % n)
                    field.string = nueva  # que una segunda pasada en el mismo registro no vuelva a renombrar
                    hechos.append('%s.%s: "%s" → "%s"' % (model_name, field.name, etiqueta, nueva))
        if hechos:
            self.env['ir.model.fields'].invalidate_model(['field_description'])
        return hechos

    @api.model
    def _renombrar_campo(self, model_name, field_name, sufijo):
        # El sufijo se agrega en todos los idiomas: la etiqueta es jsonb
        # {lang: texto} y el aviso se emite con el texto en en_US.
        self.env.cr.execute("""
            UPDATE ir_model_fields
               SET field_description = (
                    SELECT jsonb_object_agg(t.k, CASE WHEN t.v IS NULL THEN NULL ELSE t.v || %s END)
                      FROM jsonb_each_text(field_description) AS t(k, v))
             WHERE model = %s AND name = %s AND state = 'manual'
               AND jsonb_typeof(field_description) = 'object'
        """, (sufijo, model_name, field_name))

    # ------------------------------------------------------------------
    # 2. Dependencias no buscables
    #    "Field 'X' in dependency of Y should be searchable"
    #    (Field.resolve_depends: un campo intermedio de la ruta no es
    #    almacenado ni tiene búsqueda, así que Odoo no sabe qué registros
    #    recalcular cuando cambia el final de la ruta)
    # ------------------------------------------------------------------
    @api.model
    def _dependencias_no_buscables(self):
        hechos = []
        for model_name in list(self.env.registry):
            model = self.env[model_name]
            for field in model._fields.values():
                if not field.manual or field.inherited or not (field.related or field.compute):
                    continue
                rutas = list(self._depende_de(field))
                sanas = []
                rotas = False
                for ruta in rutas:
                    recorte = self._recortar_ruta(model, ruta.split('.'))
                    if recorte != [ruta]:
                        rotas = True
                    for r in recorte:
                        if r and r not in sanas:
                            sanas.append(r)
                if not rotas:
                    continue
                if field.related:
                    self._relacionado_a_calculado(model_name, field, sanas)
                    hechos.append('%s.%s: relacionado %s → calculado (depende de %s)' % (
                        model_name, field.name, field.related, ', '.join(sanas) or 'nada'))
                else:
                    self.env.cr.execute(
                        "UPDATE ir_model_fields SET depends = %s WHERE model = %s AND name = %s AND state = 'manual'",
                        (', '.join(sanas) or None, model_name, field.name))
                    hechos.append('%s.%s: depende de %s (antes %s)' % (
                        model_name, field.name, ', '.join(sanas) or 'nada', ', '.join(rutas)))
        if hechos:
            self.env['ir.model.fields'].invalidate_model()
        return hechos

    def _depende_de(self, field):
        try:
            return tuple(self.env.registry.field_depends[field])
        except KeyError:
            return ()

    def _recortar_ruta(self, model, partes, profundidad=0):
        """Devuelve las rutas equivalentes a ``partes`` que Odoo sí puede
        seguir: la ruta entera si todos los campos intermedios son buscables;
        si no, el prefijo buscable, o —cuando el primer campo es el que no se
        puede buscar— sus propias dependencias, recortadas igual."""
        prefijo = []
        actual = model
        for i, nombre in enumerate(partes):
            campo = actual._fields.get(nombre)
            if campo is None:
                return ['.'.join(prefijo)] if prefijo else []
            es_ultimo = i == len(partes) - 1
            if not es_ultimo and not campo._description_searchable:
                if prefijo:
                    return ['.'.join(prefijo)]
                if profundidad >= 3:
                    return []
                rutas = []
                for dep in self._depende_de(campo):
                    for r in self._recortar_ruta(actual, dep.split('.'), profundidad + 1):
                        if r and r not in rutas:
                            rutas.append(r)
                return rutas
            prefijo.append(nombre)
            if not es_ultimo:
                actual = self.env[campo.comodel_name]
        return ['.'.join(prefijo)]

    def _relacionado_a_calculado(self, model_name, field, depends):
        partes = field.related.split('.')
        codigo = (
            "for r in self:\n"
            "    v = r\n"
            "    for n in %r:\n"
            "        v = v[n][:1]\n"
            "    r[%r] = v[%r] if v else False\n"
        ) % (partes[:-1], field.name, partes[-1])
        self.env.cr.execute("""
            UPDATE ir_model_fields
               SET related = NULL, compute = %s, depends = %s, readonly = true
             WHERE model = %s AND name = %s AND state = 'manual'
        """, (codigo, ', '.join(depends) or None, model_name, field.name))

    # ------------------------------------------------------------------
    # 3. Vistas por defecto de Studio inválidas
    #    "invalid custom view(s) for model X: ... Default tree view for ..."
    # ------------------------------------------------------------------
    @api.model
    def _vistas_studio_invalidas(self):
        hechos = []
        View = self.env['ir.ui.view']
        self.env.flush_all()
        self.env.cr.execute("""
            SELECT v.id FROM ir_ui_view v
              JOIN ir_model_data md ON md.model = 'ir.ui.view' AND md.res_id = v.id
             WHERE v.active AND md.module = 'studio_customization'
               AND v.name ILIKE '%Default % view for%'
             ORDER BY v.id
        """)
        for (view_id,) in self.env.cr.fetchall():
            view = View.browse(view_id)
            if self._vista_valida(view):
                continue
            arch = view.arch_db or ''
            reparada = arch.replace('quick_add=', 'quick_create=')
            if reparada != arch:
                try:
                    with self.env.cr.savepoint():
                        view.write({'arch_db': reparada})
                    hechos.append('vista %d "%s": quick_add → quick_create' % (view.id, view.name))
                    continue
                except Exception:  # noqa: BLE001
                    pass
            # Sin arreglo trivial: se archiva. Odoo vuelve a generar la vista
            # por defecto del modelo, que es lo que esta vista pretendía ser.
            self.env.cr.execute("UPDATE ir_ui_view SET active = false WHERE id = %s", (view.id,))
            hechos.append('vista %d "%s" (%s): archivada por inválida' % (view.id, view.name, view.model))
        if hechos:
            View.invalidate_model()
            self.env.registry.clear_cache()
            self.env.registry.clear_cache('templates')
        return hechos

    def _vista_valida(self, view):
        try:
            with self.env.cr.savepoint():
                view._check_xml()
            return True
        except Exception:  # noqa: BLE001
            return False

    # ------------------------------------------------------------------
    # 4. Grupos que ya no existen en vistas
    #    "El grupo X que está definido en la vista no existe"
    # ------------------------------------------------------------------
    @api.model
    def _grupos_inexistentes(self):
        hechos = []
        Data = self.env['ir.model.data']
        View = self.env['ir.ui.view']
        cr = self.env.cr
        self.env.flush_all()
        cr.execute("SELECT name FROM ir_module_module WHERE state = 'installed'")
        instalados = {r[0] for r in cr.fetchall()}
        cr.execute("""
            SELECT v.id, md.module
              FROM ir_ui_view v
              LEFT JOIN ir_model_data md ON md.model = 'ir.ui.view' AND md.res_id = v.id
             WHERE v.active AND v.arch_db::text LIKE '%groups%'
             ORDER BY v.id
        """)
        for view_id, modulo in cr.fetchall():
            view = View.browse(view_id)
            arch = view.arch_db or ''
            faltantes = set()
            for grupos in RE_GROUPS_ATTR.findall(arch) + RE_GROUPS_NODE.findall(arch):
                for token in grupos.split(','):
                    nombre = token.strip().lstrip('!')
                    if nombre and '.' in nombre and not Data._xmlid_to_res_id(nombre, raise_if_not_found=False):
                        faltantes.add(nombre)
            if not faltantes:
                continue
            if modulo and modulo not in MODULOS_DE_LA_BASE and modulo in instalados:
                hechos.append('vista %d "%s" (%s): usa %s; es del addon %s, se arregla en código' % (
                    view.id, view.name, view.model, ', '.join(sorted(faltantes)), modulo))
                continue
            try:
                nuevo = self._quitar_grupos(arch, faltantes)
                with self.env.cr.savepoint():
                    view.write({'arch_db': nuevo})
                hechos.append('vista %d "%s" (%s): sin %s' % (view.id, view.name, view.model, ', '.join(sorted(faltantes))))
            except Exception as exc:  # noqa: BLE001
                hechos.append('vista %d "%s": no se pudo corregir (%s)' % (view.id, view.name, exc))
        return hechos

    @staticmethod
    def _quitar_grupos(arch, faltantes):
        root = etree.fromstring(arch.encode('utf-8'))

        def limpiar(valor):
            return ','.join(t for t in (x.strip() for x in valor.split(',')) if t and t.lstrip('!') not in faltantes)

        for node in list(root.iter()):
            if node.get('groups') is not None:
                nuevo = limpiar(node.get('groups'))
                if nuevo:
                    node.set('groups', nuevo)
                elif node is root:
                    del node.attrib['groups']
                else:
                    # Nadie pertenece a un grupo que no existe: el nodo ya no
                    # se mostraba a nadie. Quitarlo deja la vista igual.
                    node.getparent().remove(node)
            elif node.tag == 'attribute' and node.get('name') == 'groups':
                nuevo = limpiar(node.text or '')
                if nuevo:
                    node.text = nuevo
                else:
                    node.getparent().remove(node)
        return etree.tostring(root, encoding='unicode')

    # ------------------------------------------------------------------
    # 5. Columnas requeridas sin NOT NULL
    #    "Missing not-null constraint on model.field"
    #    (Registry.check_null_constraints: el campo es required y la columna
    #    admite nulos porque al agregar la restricción había filas en NULL)
    # ------------------------------------------------------------------
    @api.model
    def _columnas_sin_not_null(self):
        hechos = []
        cr = self.env.cr
        self.env.flush_all()
        cr.execute("""
            SELECT c.relname, a.attname
              FROM pg_attribute a
              JOIN pg_class c ON a.attrelid = c.oid
             WHERE c.relnamespace = current_schema::regnamespace
               AND a.attnotnull AND a.attnum > 0 AND a.attname != 'id'
        """)
        con_not_null = set(cr.fetchall())
        for model_name in list(self.env.registry):
            model = self.env[model_name]
            if not model._auto or model._abstract:
                continue
            for name, field in model._fields.items():
                if name == 'id' or not (field.column_type and field.store and field.required):
                    continue
                if (model._table, name) in con_not_null:
                    continue
                hechos.append(self._poner_not_null(model, field))
        return [h for h in hechos if h]

    def _poner_not_null(self, model, field):
        cr = self.env.cr
        etiqueta = '%s.%s' % (model._name, field.name)
        try:
            with cr.savepoint():
                cr.execute('SELECT id FROM "%s" WHERE "%s" IS NULL' % (model._table, field.name))
                ids = [r[0] for r in cr.fetchall()]
                nulos = len(ids)
                if nulos:
                    default = model.default_get([field.name]).get(field.name)
                    if default in (None, False) and field.type != 'boolean':
                        return '%s: %d filas en NULL y el campo no tiene valor por defecto; revisar a mano' % (etiqueta, nulos)
                    registros = model.browse(ids)
                    # La caché puede traer el valor viejo (o el mismo default) y la
                    # ORM se saltaría el write: se lee de nuevo desde la base.
                    registros.invalidate_recordset([field.name])
                    registros.write({field.name: default})
                    self.env.flush_all()
                cr.execute('ALTER TABLE "%s" ALTER COLUMN "%s" SET NOT NULL' % (model._table, field.name))
                return '%s: NOT NULL puesto (%d filas rellenadas)' % (etiqueta, nulos)
        except Exception as exc:  # noqa: BLE001
            return '%s: no se pudo poner NOT NULL (%s)' % (etiqueta, str(exc).splitlines()[0] if str(exc) else exc)
