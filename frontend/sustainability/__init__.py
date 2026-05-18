"""CTS Group sustainability methodology engine (embedded workbook logic)."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from flask import Flask


def register_sustainability(app: Flask) -> None:
    """Register blueprint after app module is fully initialized."""
    from frontend.sustainability.routes import bp

    app.register_blueprint(bp)


__all__ = ["register_sustainability"]
