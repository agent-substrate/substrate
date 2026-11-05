"""Unit tests for UI components, pastel gradients, and ASCII widgets."""

import pytest
from rich.text import Text
from substrate_onboarding.theme import get_gradient_color, apply_pastel_gradient
from substrate_onboarding.widgets.ascii_art import get_rendered_logo, get_compact_logo


def test_gradient_color_interpolation():
    c0 = get_gradient_color(0.0)
    c1 = get_gradient_color(1.0)
    cmid = get_gradient_color(0.5)

    assert isinstance(c0, tuple) and len(c0) == 3
    assert isinstance(c1, tuple) and len(c1) == 3
    assert isinstance(cmid, tuple) and len(cmid) == 3


def test_rendered_logo():
    logo = get_rendered_logo()
    assert isinstance(logo, Text)
    assert len(logo.plain) > 0
    assert "S U B S T R A T E" in logo.plain or "SUBSTRATE" in logo.plain


def test_compact_logo():
    compact = get_compact_logo()
    assert isinstance(compact, Text)
    assert "SUBSTRATE" in compact.plain
