"""Legacy authorization cards -> unified policy boundary (P1.7).

The legacy card shape (``methods``/``stream_scope``) may only be interpreted
here, once, then handed to ``policy.authorize`` as ordinary policy inputs:

- the card's ``methods`` become the principal's ``capabilities`` set;
- ``stream_scope=None`` means "any stream" (``scope_all``), an explicit empty
  scope never covers anything, and a non-empty scope is the ``resource_scope``;
- an expired card is never adapted.

Consumers (tool visibility, M5 execution) call ``authorize_with_cards`` and
never loop over cards themselves. A principal that already carries a valid
``authority_ceiling`` never falls back to cards: its unified inputs are the
single source of truth.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable, Mapping
from typing import Any

from .capability_table import (
    AUTHORITY_WORKSPACE_WRITE,
    is_valid_authority,
)
from .policy import (
    REASON_AUTHORITY_INSUFFICIENT,
    REASON_AUTHORITY_REQUIRED,
    Decision,
    authorize,
    principal_field,
)

Cards = Iterable[Any] | Callable[[], Iterable[Any]] | None


def _card_scope(card: object) -> tuple[str, ...] | None:
    """Return the card scope tuple, or None when the scope is explicit-empty."""
    raw = getattr(card, "stream_scope", None)
    if raw is None:
        return None
    try:
        scope = tuple(raw)
    except TypeError:
        return ()
    return scope


def adapt_card(card: object, now: float) -> Mapping[str, Any] | None:
    """Adapt one active, unexpired legacy card to policy input; else ``None``."""
    expires_at = getattr(card, "expires_at", None)
    if expires_at is not None:
        try:
            if float(expires_at) <= now:
                return None
        except (TypeError, ValueError):
            return None
    scope = _card_scope(card)
    if scope == ():
        return None
    try:
        capabilities = frozenset(getattr(card, "methods", None) or ())
    except TypeError:
        return None
    label = (
        getattr(card, "label", None)
        or getattr(card, "principal_label", None)
        or getattr(card, "principal", None)
    )
    return {
        "principal": label,
        "label": label,
        "is_root": False,
        "authority_ceiling": AUTHORITY_WORKSPACE_WRITE,
        "resource_scope": scope if scope is not None else (),
        "scope_all": scope is None,
        "capabilities": capabilities,
    }


def _iter_cards(cards: Cards) -> Iterable[Any]:
    if cards is None:
        return ()
    try:
        resolved = cards() if callable(cards) else cards
        return resolved or ()
    except Exception:  # fail closed: an unreadable card source grants nothing
        return ()


def authorize_with_cards(
    principal: object,
    action: object,
    resource: object = None,
    *,
    cards: Cards = None,
    now: float | None = None,
) -> Decision:
    """One policy decision for a modern principal or a legacy card holder."""
    decision = authorize(principal, action, resource)
    if decision.allow:
        return decision
    if decision.reason_code not in (
        REASON_AUTHORITY_REQUIRED,
        REASON_AUTHORITY_INSUFFICIENT,
    ):
        return decision
    authority = principal_field(principal, "authority_ceiling")
    if isinstance(authority, str) and is_valid_authority(authority):
        return decision
    if authority not in (None, ""):
        return decision
    if cards is None:
        return decision
    when = time.time() if now is None else now
    fallback = decision
    for card in _iter_cards(cards):
        adapted = adapt_card(card, when)
        if adapted is None:
            continue
        candidate = authorize(adapted, action, resource)
        if candidate.allow:
            return candidate
        fallback = candidate
    return fallback
