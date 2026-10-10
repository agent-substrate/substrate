"""ASCII Art assets and Google Material 3 4-color gradient rendering."""

from __future__ import annotations
from rich.text import Text
from substrate_onboarding.theme import apply_google_gradient


LOGO_ASCII_SUBSTRATE = [
    r"   ____  _   _ ____  ____ _____ ____     _  _____ _____ ",
    r"  / ___|| | | | __ )/ ___|_   _|  _ \   / \|_   _| ____|",
    r"  \___ \| | | |  _ \\___ \ | | | |_) | / _ \ | | |  _|  ",
    r"   ___) | |_| | |_) |___) || | |  _ < / ___ \| | | |___ ",
    r"  |____/ \___/|____/|____/ |_| |_| \_\_/   \_\_| |_____|",
    r"                                                        ",
    r"             ⚡ A G E N T   S U B S T R A T E ⚡          ",
]


def get_rendered_logo() -> Text:
    """Generate the full styled logo with Google 4-color gradient coloration."""
    return apply_google_gradient(LOGO_ASCII_SUBSTRATE)


def get_compact_logo() -> Text:
    """Generate a single-line or compact logo."""
    t = Text("⚡ SUBSTRATE ", style="bold #8ab4f8")
    t.append("AGENT ONBOARDING", style="bold #a8c7fa")
    return t
