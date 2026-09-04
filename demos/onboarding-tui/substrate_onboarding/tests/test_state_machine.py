"""Unit tests for OnboardingStateMachine with 7-step sequence."""

import pytest
from substrate_onboarding.config import OnboardingStep, UserSetupState
from substrate_onboarding.engine.state_machine import OnboardingStateMachine


def test_state_machine_initialization():
    sm = OnboardingStateMachine()
    assert sm.current_step == OnboardingStep.WELCOME
    assert sm.step_number() == 0
    assert not sm.state.is_complete


def test_state_machine_sequential_transitions():
    sm = OnboardingStateMachine()
    transitions = []

    sm.add_listener(lambda old_s, new_s: transitions.append((old_s, new_s)))

    expected_steps = [
        OnboardingStep.CHECK_SETUP,
        OnboardingStep.CONNECT_CLUSTER,
        OnboardingStep.TURN_ON_SUBSTRATE,
        OnboardingStep.COMPATIBLE_NODEPOOL,
        OnboardingStep.CONFIG_AUTOSCALING,
        OnboardingStep.DEPLOY_WORKERPOOL,
        OnboardingStep.COMPLETE,
    ]

    for expected in expected_steps:
        step = sm.next_step()
        assert step == expected

    assert sm.state.is_complete
    assert len(transitions) == len(expected_steps)


def test_state_machine_previous_step():
    sm = OnboardingStateMachine()
    sm.next_step()  # To CHECK_SETUP
    sm.next_step()  # To CONNECT_CLUSTER

    assert sm.current_step == OnboardingStep.CONNECT_CLUSTER
    prev = sm.previous_step()
    assert prev == OnboardingStep.CHECK_SETUP
    assert sm.current_step == OnboardingStep.CHECK_SETUP

    prev = sm.previous_step()
    assert prev == OnboardingStep.WELCOME
    assert sm.current_step == OnboardingStep.WELCOME


def test_state_machine_direct_transition():
    sm = OnboardingStateMachine()
    success = sm.transition_to(OnboardingStep.COMPATIBLE_NODEPOOL)
    assert success is True
    assert sm.current_step == OnboardingStep.COMPATIBLE_NODEPOOL
