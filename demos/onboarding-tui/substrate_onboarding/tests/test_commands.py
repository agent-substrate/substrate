"""Unit tests for slash commands parser and registry."""

from substrate_onboarding.engine.commands import CommandRegistry


def test_is_slash_command():
    assert CommandRegistry.is_slash_command("/help") is True
    assert CommandRegistry.is_slash_command("  /skip  ") is True
    assert CommandRegistry.is_slash_command("help") is False
    assert CommandRegistry.is_slash_command("") is False


def test_parse_slash_commands():
    cmd_help = CommandRegistry.parse_command("/help")
    assert cmd_help is not None
    assert cmd_help.action_key == "help"

    cmd_skip = CommandRegistry.parse_command("/skip")
    assert cmd_skip is not None
    assert cmd_skip.action_key == "skip"

    cmd_alias = CommandRegistry.parse_command("/s")
    assert cmd_alias is not None
    assert cmd_alias.action_key == "skip"

    cmd_exit = CommandRegistry.parse_command("/exit")
    assert cmd_exit is not None
    assert cmd_exit.action_key == "exit"

    cmd_doctor = CommandRegistry.parse_command("/doctor")
    assert cmd_doctor is not None
    assert cmd_doctor.action_key == "doctor"

    assert CommandRegistry.parse_command("/unknown_command") is None
