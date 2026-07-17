import os
import re
import sys
from urllib.parse import urlparse

# Regular expression to match markdown links: [text](link)
# We exclude embedded images ![text](link) but can include them or match both.
LINK_RE = re.compile(r'\[([^\]]+)\]\(([^)]+)\)')

def verify_links():
    workspace_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    agent_docs_dir = os.path.join(workspace_dir, "agent_docs")
    
    broken_links = 0
    checked_count = 0
    
    print(f"Scanning markdown files in {agent_docs_dir}...")
    
    for root, dirs, files in os.walk(agent_docs_dir):
        for file in files:
            if not file.endswith(".md"):
                continue
                
            filepath = os.path.join(root, file)
            with open(filepath, "r", encoding="utf-8") as f:
                content = f.read()
                
            # Find all links in the file
            for match in LINK_RE.finditer(content):
                link_text = match.group(1)
                link_target = match.group(2)
                
                # Split off potential anchors (e.g. #L123 or #headers)
                target_path = link_target.split("#")[0]
                
                if not target_path:
                    # Anchor-only link, skip
                    continue
                    
                # Skip external links
                if target_path.startswith(("http://", "https://", "mailto:")):
                    continue
                    
                checked_count += 1
                
                # Check absolute file:/// links
                if target_path.startswith("file:///"):
                    # Extract absolute path
                    abs_path = target_path.replace("file://", "")
                    if not os.path.exists(abs_path):
                        print(f"Broken Link in {filepath}: '{link_target}' (File does not exist: {abs_path})")
                        broken_links += 1
                else:
                    # Check relative links
                    # Resolve relative to the current file directory
                    rel_dir = os.path.dirname(filepath)
                    abs_path = os.path.abspath(os.path.join(rel_dir, target_path))
                    if not os.path.exists(abs_path):
                        print(f"Broken Link in {filepath}: '{link_target}' (Resolved path does not exist: {abs_path})")
                        broken_links += 1

    print(f"\nLink Verification Summary:")
    print(f"  Checked: {checked_count} links")
    print(f"  Broken:  {broken_links} links")
    
    if broken_links > 0:
        sys.exit(1)
    else:
        sys.exit(0)

if __name__ == "__main__":
    verify_links()
