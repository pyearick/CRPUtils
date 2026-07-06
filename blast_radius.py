"""
blast_radius.py - second-order cross-project impact mapping.

ProjectAnalyzer's cross-reference knows, per shared table, who reads and who
writes it; and per project, who it imports from. That is enough to build a
project dependency GRAPH and walk it outward from whatever project a Start Fix
is touching - capturing not just its direct neighbors but the neighbors of
those neighbors (the "blast radius").

This is inherently a FULL-ROSTER computation: it needs every project's reads,
writes, and imports in one place. The per-item Start Fix run is single-project
and cannot produce it - so this must run during the full-roster analysis and be
persisted for Start Fix to embed.

Edge semantics (direction matters for blast effect):
  - downstream : X WRITES a table that Y READS  -> changing X can break Y
  - upstream   : X READS a table that Y WRITES  -> changing Y can break X
  - shared_read: X and Y both READ the same table (looser coupling)
  - import     : one project imports code from the other
"""

from collections import defaultdict


def _invert(table_to_projects):
    """table -> [projects]  ==>  project -> set(tables)."""
    out = defaultdict(set)
    for table, projects in table_to_projects.items():
        for p in projects:
            out[p].add(table)
    return out


def _edges_between(a, b, reads, writes, import_graph):
    """
    Describe how project `a` is coupled to project `b`, from a's perspective.
    Returns a dict of non-empty edge kinds, or {} if a and b don't touch.
    """
    a_reads, a_writes = reads.get(a, set()), writes.get(a, set())
    b_reads, b_writes = reads.get(b, set()), writes.get(b, set())

    rel = {}
    downstream = sorted(a_writes & b_reads)      # a writes, b reads
    upstream = sorted(a_reads & b_writes)        # a reads, b writes
    shared_read = sorted((a_reads & b_reads) - set(downstream) - set(upstream))

    if downstream:
        rel["downstream"] = downstream
    if upstream:
        rel["upstream"] = upstream
    if shared_read:
        rel["shared_read"] = shared_read
    if b in import_graph.get(a, []):
        rel["a_imports_b"] = True
    if a in import_graph.get(b, []):
        rel["b_imports_a"] = True
    return rel


def compute_blast_radius(table_readers, table_writers, import_graph,
                         project, max_hops=2, max_table_breadth=6):
    """
    Walk the project dependency graph outward from `project`.

    Args:
        table_readers : {table -> [projects]}   (xref.table_readers)
        table_writers : {table -> [projects]}   (xref.table_writers)
        import_graph  : {project -> [projects]} (xref.import_graph)
        project       : the project being worked on
        max_hops      : how far to walk (2 = direct neighbors + their neighbors)
        max_table_breadth : a table touched by more than this many projects is
                       treated as shared infrastructure (a warehouse dimension /
                       reference table like ProductMaster or Suppliers) and is
                       excluded from coupling edges - otherwise every project that
                       merely reads a common dimension looks coupled to every
                       other. Such tables still appear in the cross-reference
                       workbook; they're just not blast-path signal. Set very
                       high to disable.

    Returns a dict:
        {
          'project': str,
          'hops': {
             1: { other_proj: {edge dict}, ... },   # directly coupled to project
             2: { other_proj: {'via': [hop1 projs], 'edges_via': {via: edge dict}} }
          },
          'common_tables': [tables excluded as too-widely-shared],
        }
    Hop-1 entries describe coupling to `project` directly. Hop-2 entries describe
    projects reachable only THROUGH a hop-1 project (the ripple's next step),
    annotated with which hop-1 project(s) connect them and how.
    """
    # Breadth = how many distinct projects touch each table. Tables above the
    # threshold are reference/dimension tables, not coupling signal.
    breadth = {}
    for tbl, projs in table_readers.items():
        breadth.setdefault(tbl, set()).update(projs)
    for tbl, projs in table_writers.items():
        breadth.setdefault(tbl, set()).update(projs)
    common = {t for t, ps in breadth.items() if len(ps) > max_table_breadth}

    def _without_common(table_map):
        return {t: ps for t, ps in table_map.items() if t not in common}

    reads = _invert(_without_common(table_readers))
    writes = _invert(_without_common(table_writers))

    all_projects = set(reads) | set(writes) | set(import_graph)
    for deps in import_graph.values():
        all_projects.update(deps)
    all_projects.discard(project)

    # --- Hop 1: everything directly coupled to `project` ---
    hop1 = {}
    for other in all_projects:
        rel = _edges_between(project, other, reads, writes, import_graph)
        if rel:
            hop1[other] = rel

    hops = {1: hop1}
    seen = {project} | set(hop1)

    # --- Hop 2..N: neighbors of the previous frontier, not already seen ---
    frontier = set(hop1)
    hop_n = 2
    while hop_n <= max_hops and frontier:
        layer = {}
        next_frontier = set()
        for near in frontier:                      # a hop-(n-1) project
            for other in all_projects:
                if other in seen or other == near:
                    continue
                rel = _edges_between(near, other, reads, writes, import_graph)
                if not rel:
                    continue
                entry = layer.setdefault(other, {"via": [], "edges_via": {}})
                if near not in entry["via"]:
                    entry["via"].append(near)
                entry["edges_via"][near] = rel
                next_frontier.add(other)
        if not layer:
            break
        hops[hop_n] = layer
        seen |= set(layer)
        frontier = next_frontier
        hop_n += 1

    return {"project": project, "hops": hops, "common_tables": sorted(common)}


def _fmt_edges(rel, subject):
    """Render an edge dict into short human phrases from `subject`'s side."""
    parts = []
    if "downstream" in rel:
        parts.append(f"writes {', '.join(rel['downstream'])} (consumed downstream)")
    if "upstream" in rel:
        parts.append(f"depends on {', '.join(rel['upstream'])} (written upstream)")
    if "shared_read" in rel:
        parts.append(f"shares read of {', '.join(rel['shared_read'])}")
    if rel.get("a_imports_b"):
        parts.append("imports its code")
    if rel.get("b_imports_a"):
        parts.append("is imported by it")
    return "; ".join(parts)


def render_blast_radius(blast, max_listed=40):
    """
    Render the blast-radius dict to compact markdown for embedding in a brief.
    Hop-1 is sorted to put DOWNSTREAM consumers first - those are the projects
    a change here can actually break.
    """
    project = blast["project"]
    hop1 = blast["hops"].get(1, {})
    lines = [f"## Blast Radius: {project}", ""]

    if not hop1:
        lines.append("No cross-project coupling detected. Changes here are "
                     "contained to this project.")
        return "\n".join(lines)

    lines.append(f"Changing **{project}** can ripple outward through shared "
                 "tables and code imports. Account for this before altering "
                 "anything it writes or anything other projects import from it.")
    lines.append("")

    # Hop 1 - downstream consumers first (highest break risk)
    def hop1_sort_key(item):
        _, rel = item
        return (0 if "downstream" in rel else
                1 if "b_imports_a" in rel else 2)

    lines.append("### Directly coupled (hop 1)")
    for other, rel in sorted(hop1.items(), key=hop1_sort_key):
        lines.append(f"- **{other}** — {project} {_fmt_edges(rel, project)}")
    lines.append("")

    # Hop 2 - where ripples continue
    hop2 = blast["hops"].get(2, {})
    if hop2:
        lines.append("### Second-order (hop 2 - reached through the above)")
        shown = 0
        for other in sorted(hop2):
            if shown >= max_listed:
                lines.append(f"- ...and {len(hop2) - shown} more "
                             "second-order projects (see the workbook).")
                break
            entry = hop2[other]
            via = ", ".join(entry["via"])
            lines.append(f"- **{other}** — via {via}")
            shown += 1
        lines.append("")

    # Transparency: note the reference tables deliberately excluded from edges.
    common = blast.get("common_tables", [])
    if common:
        shown = ", ".join(common[:12])
        more = f" (+{len(common) - 12} more)" if len(common) > 12 else ""
        lines.append(f"_Excluded as shared reference tables (read too broadly to "
                     f"be coupling signal): {shown}{more}._")

    return "\n".join(lines).strip()