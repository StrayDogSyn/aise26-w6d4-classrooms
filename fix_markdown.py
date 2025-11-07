#!/usr/bin/env python3
"""Fix remaining markdown linting issues."""
import re
from pathlib import Path

def fix_fenced_blocks(content: str) -> str:
    """Add 'text' language to fenced blocks without language specification."""
    # Find code blocks that are just ``` without a language
    # Look for ``` at start of line followed by newline (not followed by any language identifier)
    pattern = r'^```\s*$'
    replacement = '```text'
    content = re.sub(pattern, replacement, content, flags=re.MULTILINE)
    return content

def fix_bold_headings(content: str) -> str:
    """Convert bold text used as headings to proper headings."""
    # Common patterns: **Text:** or **Text** at start of line followed by newline
    # Only if it's clearly meant as a section header
    
    # Pattern 1: **Text:** on its own line
    pattern1 = r'^(\*\*([^*]+)\*\*)\s*$'
    
    def replace_heading(match):
        text = match.group(2)
        # Check if this looks like a heading (short, title-case, etc.)
        if len(text) < 50 and not text.endswith('.'):
            return f"#### {text}"
        return match.group(0)
    
    content = re.sub(pattern1, replace_heading, content, flags=re.MULTILINE)
    return content

def process_file(filepath: Path):
    """Process a single markdown file."""
    print(f"Processing {filepath}...")
    
    content = filepath.read_text(encoding='utf-8')
    original = content
    
    # Apply fixes
    content = fix_fenced_blocks(content)
    content = fix_bold_headings(content)
    
    if content != original:
        filepath.write_text(content, encoding='utf-8')
        print(f"  ✓ Fixed {filepath.name}")
    else:
        print(f"  - No changes needed for {filepath.name}")

def main():
    """Fix all markdown files."""
    base = Path(__file__).parent
    
    files = [
        base / "breakouts" / "01_backpressure_dlq.md",
        base / "breakouts" / "02_partitions_hotkeys.md",
        base / "COMPLETION_SUMMARY.md",
        base / "QUICK_REFERENCE.md",
        base / "README.md",
    ]
    
    for filepath in files:
        if filepath.exists():
            process_file(filepath)
        else:
            print(f"⚠ File not found: {filepath}")

if __name__ == "__main__":
    main()
