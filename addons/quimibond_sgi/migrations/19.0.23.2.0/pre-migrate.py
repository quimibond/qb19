# -*- coding: utf-8 -*-


def migrate(cr, version):
    """El one2many del procedimiento se renombró a procedure_activity_ids
    (activity_ids ahora es el de mail.activity, del mixin heredado). La
    vista heredada VIEJA guardada en BD aún referencia activity_ids con su
    lista embebida (sequence, block, number…), y al actualizar, la vista
    base se valida COMBINADA con esa herencia estale ANTES de que su XML
    nuevo la reemplace — el update reventaba con «El campo sequence no
    existe en el modelo mail.activity». Se elimina aquí; su archivo XML la
    recrea en esta misma actualización."""
    cr.execute("""
        DELETE FROM ir_ui_view
        WHERE id IN (SELECT res_id FROM ir_model_data
                     WHERE module = 'quimibond_sgi'
                       AND name = 'sgi_process_view_form_procedure'
                       AND model = 'ir.ui.view')
    """)
    cr.execute("""
        DELETE FROM ir_model_data
        WHERE module = 'quimibond_sgi'
          AND name = 'sgi_process_view_form_procedure'
          AND model = 'ir.ui.view'
    """)
