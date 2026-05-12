"""
Canonical built-in internal supplier aliases for double-counting Rule 1 (CTS source + internal provider).

These strings are the historical INTERNAL_SUPPLIERS baseline: permanent offline-safe protection
for manual uploads, external files, non-CCC procurement, legacy sources, batch jobs, and CLI runs.

This module MUST NOT import Flask or pandas. Consumers merge these aliases with DB / cache / seed
tokens — never replace them.
"""

from __future__ import annotations

# Original INTERNAL_SUPPLIERS list (canonical names and known variants). Kept as a tuple for immutability.
BUILTIN_INTERNAL_SUPPLIER_ALIASES: tuple[str, ...] = (
    "Nordic EPOD",
    "Nordicepod AS",
    "Nordicepod",
    "NEP Switchboards",
    "G. T Nordics",
    "G. T Nordics AS",
    "G.T Nordics As",
    "Gapit",
    "Gapit AS",
    "Gapit As",
    "Gapit Nordics As",
    "DC Piping",
    "MC Prefab",
    "MC Prefab Nordics AS",
    "Mc Prefab Nordics AS",
    "Velox Electro Nordics AS",
    "Velox Electro Nordics OY",
    "Mecwide Nordics Finland OY",
    "Mecwide Nordics AS",
    "Mecwide Nordics Denmark ApS",
    "Mecwide Nordics`",
    "Comissioning Services",
    "Comissioning Services AS",
    "Qec Nordics AS",
    "Qec Nordics",
    "PORVELOX Electro Europe Lda",
    "MC Prefab Sweden AB",
    "Nordic Crane AS",
    "Commissioning Services Nordics AS",
    "Mecwide Nordics Sweden AB",
    "CTS-VDC Services LTD",
    "CTS NORDICS AS",
    "Velox Electro Nordics AB",
    "CS Nordics",
    "Fortica Sweden AB",
    "101",
    "102",
    "103",
    "104",
    "105",
    "106",
    "107",
    "108",
    "109",
    "110",
    "111",
    "112",
    "113",
    "114",
    "115",
    "116",
    "117",
)
