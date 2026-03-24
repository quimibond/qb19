"""
Quimibond Intelligence — Supabase Utilities
Funciones utilitarias compartidas por los mixins de Supabase.
"""


def _postgrest_in_list(values: list) -> str:
    """Construye la lista `in.(...)` para filtros PostgREST."""
    parts = []
    for s in values:
        if not s:
            continue
        esc = str(s).replace('\\', '\\\\').replace('"', '\\"')
        parts.append(f'"{esc}"')
    return ','.join(parts)
