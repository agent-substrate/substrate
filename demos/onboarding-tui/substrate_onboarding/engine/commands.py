"""Slash Command Interpreter and Registry."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, List, Optional


@dataclass
class SlashCommand:
    name: str
    aliases: List[str]
    description: str
    usage: str
    action_key: str


class CommandRegistry:
    """Registry and parser for global slash commands."""

    COMMANDS: List[SlashCommand] = [
        SlashCommand(
            name="/help",
            aliases=["/?", "/h"],
            description="Show interactive command overlay & keyboard shortcuts",
            usage="/help",
            action_key="help",
        ),
        SlashCommand(
            name="/skip",
            aliases=["/s", "/next"],
            description="Skip current question/step using recommended defaults",
            usage="/skip",
            action_key="skip",
        ),
        SlashCommand(
            name="/back",
            aliases=["/prev", "/b"],
            description="Return to previous onboarding screen",
            usage="/back",
            action_key="back",
        ),
        SlashCommand(
            name="/doctor",
            aliases=["/check", "/preflight"],
            description="Run or inspect environment pre-flight diagnostics",
            usage="/doctor",
            action_key="doctor",
        ),
        SlashCommand(
            name="/exit",
            aliases=["/quit", "/q"],
            description="Pause onboarding and exit cleanly",
            usage="/exit",
            action_key="exit",
        ),
    ]

    @classmethod
    def is_slash_command(cls, text: str) -> bool:
        """Check if input text begins with a slash command trigger."""
        stripped = text.strip()
        return stripped.startswith("/")

    @classmethod
    def parse_command(cls, text: str) -> Optional[SlashCommand]:
        """Parse raw user input to find a matching slash command."""
        tokens = text.strip().split()
        if not tokens:
            return None
        cmd_token = tokens[0].lower()

        for cmd in cls.COMMANDS:
            if cmd.name == cmd_token or cmd_token in cmd.aliases:
                return cmd
        return None

    @classmethod
    def get_command_list(cls) -> List[SlashCommand]:
        return list(cls.COMMANDS)
