"""
Pre-migration: Clean up old models, crons, and views before updating to v30.

This runs BEFORE the module update, while old models still exist in the DB.
Prevents errors from orphaned crons referencing deleted models.
"""
import logging

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return

    _logger.info('Pre-migration: cleaning up old quimibond_intelligence models...')

    # 1. Deactivate all old cron jobs that reference removed models
    cr.execute("""
        UPDATE ir_cron SET active = FALSE
        WHERE id IN (
            SELECT c.id FROM ir_cron c
            JOIN ir_model m ON c.model_id = m.id
            WHERE m.model IN (
                'intelligence.engine',
                'intelligence.config',
                'intelligence.alert',
                'intelligence.action.item',
                'intelligence.briefing',
                'intelligence.score',
                'intelligence.query',
                'intelligence.draft.action'
            )
        )
    """)
    deactivated = cr.rowcount
    _logger.info('Deactivated %d old cron jobs', deactivated)

    # 2. Remove old ir.model.access records for removed models
    cr.execute("""
        DELETE FROM ir_model_access
        WHERE model_id IN (
            SELECT id FROM ir_model
            WHERE model IN (
                'intelligence.engine',
                'intelligence.config',
                'intelligence.alert',
                'intelligence.action.item',
                'intelligence.briefing',
                'intelligence.score',
                'intelligence.query',
                'intelligence.draft.action',
                'res.partner'
            )
            AND id NOT IN (
                SELECT DISTINCT model_id FROM ir_model_access
                WHERE name NOT LIKE 'access_intelligence_%'
                AND name NOT LIKE 'access_quimibond_intelligence_%'
            )
        )
        AND (name LIKE 'access_intelligence_%' OR name LIKE 'access_quimibond_intelligence_%')
    """)
    _logger.info('Cleaned up %d old access records', cr.rowcount)

    # 3. Remove old server actions that reference removed models
    cr.execute("""
        DELETE FROM ir_act_server
        WHERE model_id IN (
            SELECT id FROM ir_model
            WHERE model IN (
                'intelligence.engine',
                'intelligence.config'
            )
        )
    """)
    _logger.info('Cleaned up %d old server actions', cr.rowcount)

    # 4. Remove old menu items that reference removed actions
    cr.execute("""
        DELETE FROM ir_ui_menu
        WHERE id IN (
            SELECT m.id FROM ir_ui_menu m
            LEFT JOIN ir_model_data d ON d.res_id = m.id AND d.model = 'ir.ui.menu'
            WHERE d.module = 'quimibond_intelligence'
            AND d.name LIKE 'menu_intelligence_%'
        )
    """)
    _logger.info('Cleaned up old menu items')

    # 5. Remove old views
    cr.execute("""
        DELETE FROM ir_ui_view
        WHERE id IN (
            SELECT v.id FROM ir_ui_view v
            JOIN ir_model_data d ON d.res_id = v.id AND d.model = 'ir.ui.view'
            WHERE d.module = 'quimibond_intelligence'
            AND v.model IN (
                'intelligence.alert',
                'intelligence.action.item',
                'intelligence.briefing',
                'intelligence.config',
                'intelligence.query',
                'intelligence.draft.action'
            )
        )
    """)
    _logger.info('Cleaned up old views')

    # 6. Drop old tables (only if they exist and have no FK dependencies)
    old_tables = [
        'intelligence_draft_action',
        'intelligence_query',
        'intelligence_score',
        'intelligence_briefing',
        'intelligence_action_item',
        'intelligence_alert',
        'intelligence_config',
        'intelligence_engine',
    ]
    for table in old_tables:
        cr.execute(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)",
            (table,)
        )
        if cr.fetchone()[0]:
            try:
                cr.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')
                _logger.info('Dropped table %s', table)
            except Exception as exc:
                _logger.warning('Could not drop table %s: %s', table, exc)

    _logger.info('Pre-migration completed')
