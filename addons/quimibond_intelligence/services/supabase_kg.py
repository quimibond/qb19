"""
Quimibond Intelligence — Supabase KG Mixin
Knowledge Graph operations for Supabase.
"""
import logging
from urllib.parse import quote as url_quote

_logger = logging.getLogger(__name__)


class SupabaseKGMixin:
    """Knowledge Graph operations for Supabase."""

    # ── Knowledge Graph — dedup, decay, traversal ─────────────────────────

    def find_duplicate_entities(self) -> list:
        """Return candidate duplicate entities for review/merge."""
        try:
            return self._request(
                '/rest/v1/rpc/find_duplicate_entities', 'POST', {},
            ) or []
        except Exception as exc:
            _logger.warning('find_duplicate_entities: %s', exc)
            return []

    def merge_entities(self, keep_id: int, merge_id: int) -> dict:
        """Merge two entities, keeping keep_id."""
        return self._request(
            '/rest/v1/rpc/merge_entities', 'POST',
            {'p_keep_id': keep_id, 'p_merge_id': merge_id},
        ) or {}

    def auto_deduplicate_entities(self) -> dict:
        """Auto-merge high-confidence duplicates (same_email only)."""
        dupes = self.find_duplicate_entities()
        merged = 0
        skipped = 0
        for d in dupes:
            if d.get('match_reason') != 'same_email':
                continue
            result = self.merge_entities(
                d['entity_a_id'], d['entity_b_id'],
            )
            if result.get('status') == 'ok':
                merged += 1
            else:
                skipped += 1
        if merged:
            self.log_event('auto_dedup', source='pipeline',
                           payload={'merged': merged, 'skipped': skipped})
        return {'merged': merged, 'skipped': skipped,
                'candidates': len(dupes)}

    def decay_fact_confidence(self) -> dict:
        """Run temporal decay on unverified facts."""
        try:
            return self._request(
                '/rest/v1/rpc/decay_fact_confidence', 'POST', {},
            ) or {}
        except Exception as exc:
            _logger.warning('decay_fact_confidence: %s', exc)
            return {}

    def verify_fact(self, fact_id: int, source: str = 'cross_reference') -> dict:
        """Mark a fact as verified, boosting confidence."""
        return self._request(
            '/rest/v1/rpc/verify_fact', 'POST',
            {'p_fact_id': fact_id, 'p_source': source},
        ) or {}

    def get_entity_network(self, entity_id: int, depth: int = 2,
                           min_strength: float = 0.0) -> dict:
        """BFS traversal of entity relationships graph."""
        try:
            return self._request(
                '/rest/v1/rpc/get_entity_network', 'POST',
                {'p_entity_id': entity_id, 'p_depth': depth,
                 'p_min_strength': min_strength},
            ) or {}
        except Exception as exc:
            _logger.warning('get_entity_network: %s', exc)
            return {}

    # ── Knowledge Graph ──────────────────────────────────────────────────────

    def upsert_entity(self, entity):
        """Inserta o actualiza una entidad en el knowledge graph."""
        canonical = entity.get('name', '').lower().strip()
        data = {
            'entity_type': entity.get('type', 'person'),
            'name': entity.get('name', ''),
            'canonical_name': canonical,
            'email': entity.get('email'),
            'attributes': entity.get('attributes', {}),
            'last_seen': entity.get('date', None),
        }
        return self._request(
            '/rest/v1/entities?on_conflict=entity_type,canonical_name',
            'POST', data, {
                'Prefer': 'resolution=merge-duplicates,return=representation',
            })

    def save_fact(self, fact):
        """Guarda un hecho extraido (con dedup por fact_hash).

        Computes fact_hash if missing: md5(entity_id + fact_type + fact_text).
        """
        if not fact.get('fact_hash'):
            import hashlib
            raw = f"{fact.get('entity_id', '')}|{fact.get('fact_type', '')}|{fact.get('fact_text', '')}"
            fact['fact_hash'] = hashlib.md5(raw.encode()).hexdigest()
        return self._request(
            '/rest/v1/facts?on_conflict=fact_hash',
            'POST', fact,
            extra_headers={
                'Prefer': 'resolution=ignore-duplicates,return=representation',
            },
        )

    def save_relationship(self, rel):
        """Guarda o actualiza una relacion entre entidades."""
        return self._request(
            '/rest/v1/entity_relationships'
            '?on_conflict=entity_a_id,entity_b_id,relationship_type',
            'POST', rel, {
                'Prefer': 'resolution=merge-duplicates,return=representation',
            })

    def batch_save_facts(self, facts: list):
        """Guarda hechos en batch (ignora duplicados por fact_hash)."""
        if not facts:
            return
        import hashlib
        for fact in facts:
            if not fact.get('fact_hash'):
                raw = (f"{fact.get('entity_id', '')}|"
                       f"{fact.get('fact_type', '')}|"
                       f"{fact.get('fact_text', '')}")
                fact['fact_hash'] = hashlib.md5(raw.encode()).hexdigest()
        try:
            self._upsert_batch(
                '/rest/v1/facts?on_conflict=fact_hash',
                facts, 'ignore-duplicates',
            )
        except Exception as exc:
            _logger.warning('batch_save_facts (%d): %s', len(facts), exc)

    def batch_save_relationships(self, rels: list):
        """Guarda relaciones en batch (merge duplicados)."""
        if not rels:
            return
        try:
            self._upsert_batch(
                '/rest/v1/entity_relationships'
                '?on_conflict=entity_a_id,entity_b_id,relationship_type',
                rels, 'merge-duplicates',
            )
        except Exception as exc:
            _logger.warning('batch_save_relationships (%d): %s',
                            len(rels), exc)

    def get_entity_by_name(self, name):
        """Busca una entidad por nombre."""
        canonical = name.lower().strip()
        result = self._request(
            '/rest/v1/entities?canonical_name=eq.' + url_quote(canonical, safe='') + '&limit=1',
        )
        return result[0] if result else None

    def get_entity_intelligence(self, name=None, email=None,
                                entity_type=None, odoo_id=None):
        """Llama al RPC get_entity_intelligence (4-param version).

        Uses the 4-param overload to avoid PostgREST HTTP 300
        'Multiple Choices' when both 2-param and 4-param versions exist.
        """
        return self._request(
            '/rest/v1/rpc/get_entity_intelligence',
            'POST',
            {
                'p_entity_type': entity_type,
                'p_name': name,
                'p_email': email,
                'p_odoo_id': odoo_id,
            },
            extra_headers={
                'Accept': 'application/json',
                'Content-Profile': 'public',
            },
        )
