"""State Machine for Onboarding Workflow with 7-step progressive disclosure architecture."""

from __future__ import annotations

from typing import Callable, List, Optional
from substrate_onboarding.config import OnboardingStep, UserSetupState


class OnboardingStateMachine:
    """Manages consecutive onboarding state transitions with history and rollback support.
    
    Sequence:
      0. Welcome & Setup Track (WELCOME)
      1. Check your setup (CHECK_SETUP)
      2. Connect cluster & Region & GKE Agreement (CONNECT_CLUSTER)
      3. Turn on Substrate (TURN_ON_SUBSTRATE)
      4. Compatible Node Pool (COMPATIBLE_NODEPOOL)
      5. Configure Autoscaling (CONFIG_AUTOSCALING)
      6. Deploy WorkerPool (DEPLOY_WORKERPOOL)
      7. Installation Complete & Next Steps (COMPLETE)
    """

    STEPS_ORDER: List[OnboardingStep] = [
        OnboardingStep.WELCOME,
        OnboardingStep.CHECK_SETUP,
        OnboardingStep.CONNECT_CLUSTER,
        OnboardingStep.TURN_ON_SUBSTRATE,
        OnboardingStep.COMPATIBLE_NODEPOOL,
        OnboardingStep.CONFIG_AUTOSCALING,
        OnboardingStep.DEPLOY_WORKERPOOL,
        OnboardingStep.COMPLETE,
    ]

    def __init__(self, state: Optional[UserSetupState] = None):
        self.state = state or UserSetupState()
        self.history: List[OnboardingStep] = []
        self._listeners: List[Callable[[OnboardingStep, OnboardingStep], None]] = []

    @property
    def current_step(self) -> OnboardingStep:
        return self.state.current_step

    def add_listener(self, listener: Callable[[OnboardingStep, OnboardingStep], None]) -> None:
        """Register a callback when state transitions occur."""
        self._listeners.append(listener)

    def transition_to(self, new_step: OnboardingStep) -> bool:
        """Transition to a specific step, updating history and notifying listeners."""
        old_step = self.state.current_step
        if old_step == new_step:
            return False

        self.history.append(old_step)
        self.state.current_step = new_step

        if new_step == OnboardingStep.COMPLETE:
            self.state.is_complete = True

        for listener in self._listeners:
            listener(old_step, new_step)

        return True

    def next_step(self) -> Optional[OnboardingStep]:
        """Advance to the next logical step in the predefined sequence."""
        try:
            current_idx = self.STEPS_ORDER.index(self.current_step)
            if current_idx < len(self.STEPS_ORDER) - 1:
                next_step = self.STEPS_ORDER[current_idx + 1]
                self.transition_to(next_step)
                return next_step
        except ValueError:
            pass
        return None

    def previous_step(self) -> Optional[OnboardingStep]:
        """Rollback to the previous step in history, or the preceding logical step."""
        if self.history:
            prev_step = self.history.pop()
            old_step = self.state.current_step
            self.state.current_step = prev_step
            for listener in self._listeners:
                listener(old_step, prev_step)
            return prev_step
        else:
            try:
                current_idx = self.STEPS_ORDER.index(self.current_step)
                if current_idx > 0:
                    prev_step = self.STEPS_ORDER[current_idx - 1]
                    self.transition_to(prev_step)
                    return prev_step
            except ValueError:
                pass
        return None

    def step_number(self) -> int:
        """Return the current step number (0-based, matching STEPS_ORDER index)."""
        try:
            return self.STEPS_ORDER.index(self.current_step)
        except ValueError:
            return 0

    def total_steps(self) -> int:
        """Total number of interactive steps (excluding Welcome Step 0)."""
        return len(self.STEPS_ORDER) - 1
