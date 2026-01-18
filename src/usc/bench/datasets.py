def toy_agent_log() -> str:
    """
    A tiny fake "agent memory log" we can compress.
    We'll make it repetitive on purpose so compression is obvious.
    """
    return (
        "Project: USC\n"
        "Goal: build a unified state codec.\n"
        "Decision: start with USC-MEM v0.\n"
        "Next: implement skeleton + witnesses + residual patches.\n"
        "Note: always keep ROADMAP, FILEMAP, MASTER_HANDOFF, CHANGES updated.\n"
        "\n"
        "Session recap:\n"
        "- Created repo scaffold.\n"
        "- Fixed pyenv python version.\n"
        "- Installed usc editable.\n"
        "- Installed pytest.\n"
        "- Ran tests successfully.\n"
        "\n"
        "Reminder: compress memories safely using anchors and residuals.\n"
        "Reminder: compress memories safely using anchors and residuals.\n"
        "Reminder: compress memories safely using anchors and residuals.\n"
    )
