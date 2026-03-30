"""
Pull intelligence commands from Supabase back to Odoo.

Handles:
1. sync_commands: manual triggers from frontend (e.g., "reprocess emails")
2. New contacts: create in Odoo if they exist in Supabase but not in res.partner
3. Completed actions: close corresponding Odoo activities
"""
import logging
from datetime import datetime

from odoo import api, models

from .supabase_client import SupabaseClient

_logger = logging.getLogger(__name__)


def _get_client(env) -> SupabaseClient | None:
    get = lambda k: env['ir.config_parameter'].sudo().get_param(k) or ''
    url = get('quimibond_intelligence.supabase_url')
    key = get('quimibond_intelligence.supabase_service_key')
    if not url or not key:
        return None
    return SupabaseClient(url, key)


class QuimibondSyncPull(models.TransientModel):
    _name = 'quimibond.sync.pull'
    _description = 'Quimibond Pull from Supabase'

    @api.model
    def pull_from_supabase(self):
        """Main cron entry point: pull commands and new data from Supabase."""
        client = _get_client(self.env)
        if not client:
            return

        _start = datetime.now()
        try:
            commands = self._process_commands(client)
            contacts = self._sync_new_contacts(client)
            actions = self._sync_completed_actions(client)

            summary = f'commands={commands}, contacts={contacts}, actions={actions}'
            _logger.info('✓ Pull from Supabase: %s', summary)
            elapsed = (datetime.now() - _start).total_seconds()
            self.env['quimibond.sync.log'].sudo().create({
                'name': 'Pull completo',
                'direction': 'pull',
                'status': 'success',
                'summary': summary,
                'duration_seconds': round(elapsed, 1),
            })
            self.env.cr.commit()
        except Exception as exc:
            _logger.error('Pull from Supabase failed: %s', exc)
            try:
                self.env['quimibond.sync.log'].sudo().create({
                    'name': 'Pull fallido',
                    'direction': 'pull',
                    'status': 'error',
                    'summary': str(exc)[:500],
                })
                self.env.cr.commit()
            except Exception:
                pass
        finally:
            client.close()

    # ── Process frontend commands ────────────────────────────────────────

    def _process_commands(self, client: SupabaseClient) -> int:
        """Process pending sync_commands from frontend."""
        commands = client.fetch('sync_commands', {
            'status': 'eq.pending',
            'order': 'created_at.asc',
            'limit': '10',
        })
        if not commands:
            return 0

        processed = 0
        for cmd in commands:
            cmd_id = cmd.get('id')
            command = cmd.get('command', '')
            try:
                # Mark as running
                client.patch('sync_commands', f'id=eq.{cmd_id}', {
                    'status': 'running',
                    'started_at': datetime.now().isoformat(),
                })

                result = self._execute_command(command)

                client.patch('sync_commands', f'id=eq.{cmd_id}', {
                    'status': 'completed',
                    'completed_at': datetime.now().isoformat(),
                    'result': result,
                })
                processed += 1
            except Exception as exc:
                _logger.warning('Command %s failed: %s', cmd_id, exc)
                client.patch('sync_commands', f'id=eq.{cmd_id}', {
                    'status': 'error',
                    'completed_at': datetime.now().isoformat(),
                    'result': str(exc)[:500],
                })

        return processed

    def _execute_command(self, command: str) -> str:
        """Execute a sync command. Returns result string."""
        if command == 'force_push':
            self.env['quimibond.sync'].push_to_supabase()
            return 'Push completed'
        elif command == 'sync_contacts':
            client = _get_client(self.env)
            if client:
                n = self.env['quimibond.sync']._push_contacts(client)
                client.close()
                return f'{n} contacts synced'
            return 'No client'
        else:
            return f'Unknown command: {command}'

    # ── Create new contacts in Odoo from Supabase ────────────────────────

    def _sync_new_contacts(self, client: SupabaseClient) -> int:
        """Find contacts in Supabase without odoo_partner_id and create them in Odoo."""
        # Fetch contacts that exist in Supabase but have no Odoo link
        contacts = client.fetch('contacts', {
            'odoo_partner_id': 'is.null',
            'contact_type': 'eq.external',
            'email': 'not.is.null',
            'limit': '50',
            'order': 'created_at.desc',
        })
        if not contacts:
            return 0

        Partner = self.env['res.partner'].sudo()
        created = 0

        for contact in contacts:
            email = (contact.get('email') or '').strip().lower()
            name = contact.get('name') or email
            if not email:
                continue

            # Check if already exists in Odoo by email
            existing = Partner.search([('email', '=ilike', email)], limit=1)
            if existing:
                # Link existing partner back to Supabase
                client.patch('contacts', f'id=eq.{contact["id"]}', {
                    'odoo_partner_id': existing.id,
                })
                continue

            # Create new partner in Odoo
            try:
                vals = {
                    'name': name,
                    'email': email,
                    'is_company': False,
                    'customer_rank': 1,
                }
                # Link to parent company if company_id exists in Supabase
                company_id = contact.get('company_id')
                if company_id:
                    # Look up the company's odoo_partner_id
                    companies = client.fetch('companies', {
                        'id': f'eq.{company_id}',
                        'select': 'odoo_partner_id',
                        'limit': '1',
                    })
                    if companies and companies[0].get('odoo_partner_id'):
                        vals['parent_id'] = companies[0]['odoo_partner_id']

                partner = Partner.create(vals)

                # Update Supabase with the new Odoo ID
                client.patch('contacts', f'id=eq.{contact["id"]}', {
                    'odoo_partner_id': partner.id,
                })
                created += 1
                _logger.info('Created Odoo partner %s for %s', partner.id, email)
            except Exception as exc:
                _logger.warning('Failed to create partner for %s: %s', email, exc)

        return created

    # ── Sync completed actions back to Odoo ──────────────────────────────

    def _sync_completed_actions(self, client: SupabaseClient) -> int:
        """Sync action state changes from Supabase to Odoo activities."""
        # Fetch recently completed/dismissed actions
        actions = client.fetch('action_items', {
            'state': 'in.(completed,dismissed)',
            'updated_at': f'gte.{(datetime.now().replace(hour=0, minute=0)).isoformat()}',
            'select': 'id,state,contact_name,description',
            'limit': '50',
        })
        if not actions:
            return 0

        # For now, just log — full bidirectional sync can be added later
        _logger.info('Found %d completed/dismissed actions in Supabase', len(actions))
        return len(actions)
