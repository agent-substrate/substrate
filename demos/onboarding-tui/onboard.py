#!/usr/bin/env python3
"""Convenience executable script to launch the Substrate Onboarding TUI Prototype."""

import os
import sys

# Ensure demo directory is on sys.path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from substrate_onboarding.__main__ import main

if __name__ == "__main__":
    main()
