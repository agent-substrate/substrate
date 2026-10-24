---
name: tui-ux-guidelines
description: Comprehensive guidelines, design principles, and best practices for building elite Terminal User Interfaces (TUIs) and delightful command-line experiences.
---

# Elite Terminal User Interface (TUI) UX/UI Guidelines

This skill provides comprehensive design principles, architectural patterns, and implementation best practices for crafting modern, accessible, and high-taste Terminal User Interfaces (TUIs).

---

## 🎨 1. Visual Design & Typography

| Best Practice | Implementation Detail | Why It Matters |
| :--- | :--- | :--- |
| **Adopt a "Light-Theme Safe" Palette** | Use standard ANSI colors (e.g., green, cyan, yellow) or semantic color variables instead of hardcoded dark hex values. | Hardcoding dark colors will render the TUI completely invisible or unreadable for users with light terminal backgrounds. |
| **Enforce Strict Line Wrapping** | Always calculate terminal width dynamically using system hooks (e.g., `os.get_terminal_size()` or framework container layouts) and wrap text gracefully. | Prevents text from clipping or spilling over, which breaks Unicode box-drawing borders and degrades readability. |
| **Leverage Unicode & Braille Patterns** | Use standard Unicode box-drawing characters (`┌─┐`, `│`, `└─┘`) for structural panels and Braille patterns (`⠋`, `⠙`, `⠹`, `⠸`, `⠼`, `⠴`, `⠦`, `⠧`, `⠇`, `⠏`) for smooth spinners. | Instantly elevates the interface from a retro "MS-DOS" block style to a sleek, modern command-line aesthetic. |
| **Maintain Generous Whitespace** | Keep a strict visual hierarchy. Ensure ample padding (`1-2` cells) inside containers, distinct row gaps, and spacing between icons and titles (`icon + "  " + title`). | Prevents "terminal claustrophobia" and allows the user to scan the layout and options effortlessly. |

---

## ⌨️ 2. Interaction & Navigation

- **Keyboard-First, Mouse-Friendly**:
  - Every single action must be fully bindable to simple, standard keys (`↑`/`↓`, `j`/`k`, `Tab`/`Shift+Tab`, `Enter`, `Space`, `b` for back).
  - Enable mouse hover, clicking, and scrolling wherever supported by the runtime to make the tool accessible to beginners.
- **Implement Global Escape Hatches**:
  - The user should never feel trapped. Ensure `Ctrl+C`, `Ctrl+D`, or `Esc` triggers a clean, safe exit confirmation dialog (e.g., `"Exit setup? (y/n)"`) preserving existing state rather than abruptly crashing.
- **Always-Visible Keyboard Legend & Status Bar**:
  - Display a persistent, single-line status bar at the bottom of the terminal showing active shortcuts:
    ```text
    💡 Contextual Tip Here...    [↑/↓] Select  [Enter] Confirm  [/help] Shortcuts  [Ctrl+C] Exit
    ```
- **Non-Blocking Async Input Handling**:
  - Execute background tasks (network requests, API calls, Docker probes, CLI verifications) on separate async workers.
  - Keep the main UI event loop active so animations, cursor movement, and keypresses remain fluid during background I/O.

---

## 🔄 3. State & Dynamic Updates

- **Dynamic Resizing Recovery**:
  - Bind to terminal resize signals (`SIGWINCH` on Unix).
  - When the terminal window is resized, reflow text and re-render layout grids dynamically without throwing exceptions or corrupting text buffers.
- **Prefer Inline Rewrites over Terminal Flooding**:
  - In non-fullscreen CLI tools, use carriage returns (`\r`), ANSI cursor-up codes, or dynamic live displays rather than repeatedly printing new lines.
  - Prevents polluting the user's terminal scrollback history.
- **Modal Overlay Handling**:
  - When displaying errors, slash command palettes, or confirmation dialogs, dim the background panels and render a centered, floating Unicode modal box.

---

## 💡 4. Subtle Polish: The "Delight" Factor

- **Micro-Animations & Typing Cadence**:
  - Use frame-by-frame Braille spinners, subtle color pulsing, or typewriter text reveals for hero intros rather than static `"Loading..."` text.
  - Provides clear, continuous visual confirmation that the application is actively processing.
- **The "Doctor" Self-Healing Checkup**:
  - On startup or diagnostic steps, run silent, non-blocking environment validation checks.
  - **Never crash with raw stack traces**. If a prerequisite is missing, display a formatted warning badge accompanied by the exact copy-paste remediation command (e.g., `brew install ...` or `gcloud auth login`).
- **Slash Commands & Power Shortcuts**:
  - Support power-user slash commands (`/help`, `/skip`, `/doctor`, `/back`, `/exit`) for rapid jumping across onboarding states.

---

## 📋 TUI Review Checklist

When designing or reviewing terminal interfaces, verify the following:

- [ ] **Contrast & Readability**: All text is readable against both dark and light terminal backgrounds.
- [ ] **Border Integrity**: No text overflows outside Unicode borders or buttons across varying terminal widths.
- [ ] **Tab & Step Navigation**: Top stepper tabs indicate active vs. completed vs. upcoming states.
- [ ] **Doctor Diagnostics**: System checks provide actionable copy-paste remedy commands when warnings/failures occur.
- [ ] **Input Masking & Validation**: Passwords and tokens are masked by default (`sb-l********5d3`) with show/hide toggle support.
- [ ] **Zero Uncaught Exceptions**: All CLI failures, network timeouts, and cancellation signals are cleanly handled.
