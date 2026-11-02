#!/usr/bin/env bash
""":"
exec .venv/bin/python "$0" "$@"
"""
"""
scripts/generate_notebooks.py
Converts Python module scripts with marked blocks into interactive Jupyter Notebooks.
"""

import os
import glob
import nbformat as nbf

def convert_script_to_notebook(py_path: str, nb_path: str):
    with open(py_path, "r", encoding="utf-8") as f:
        content = f.read()

    nb = nbf.v4.new_notebook()
    nb.metadata["kernelspec"] = {
        "display_name": "Python 3",
        "language": "python",
        "name": "python3"
    }

    # Extract header docstring if present
    cells = []
    if content.startswith('"""'):
        end_header = content.find('"""', 3)
        header_md = content[3:end_header].strip()
        cells.append(nbf.v4.new_markdown_cell(header_md))
        rest = content[end_header + 3:]
    else:
        rest = content

    # Extract trailing docstring (Expected Output / Exercises) if present
    trailing_md = ""
    if '"""' in rest:
        last_idx = rest.rfind('"""')
        prev_idx = rest.rfind('"""', 0, last_idx)
        if prev_idx != -1 and ("Expected Output" in rest[prev_idx:] or "Try It Yourself" in rest[prev_idx:]):
            trailing_md = rest[prev_idx + 3:last_idx].strip()
            code_body = rest[:prev_idx].strip()
        else:
            code_body = rest.strip()
    else:
        code_body = rest.strip()

    # Split code body into cells by '# %%' if present, or create code cells
    if "# %%" in code_body:
        sections = code_body.split("# %%")
        for sec in sections:
            sec = sec.strip()
            if not sec:
                continue
            if sec.startswith("[markdown]"):
                md_text = sec.replace("[markdown]", "", 1).strip()
                cells.append(nbf.v4.new_markdown_cell(md_text))
            else:
                cells.append(nbf.v4.new_code_cell(sec))
    else:
        cells.append(nbf.v4.new_code_cell(code_body))

    if trailing_md:
        cells.append(nbf.v4.new_markdown_cell(trailing_md))

    nb.cells = cells
    os.makedirs(os.path.dirname(nb_path), exist_ok=True)
    with open(nb_path, "w", encoding="utf-8") as f:
        nbf.write(nb, f)
    print(f"[✓] Converted {py_path} -> {nb_path}")

def main():
    py_files = glob.glob("scripts/part*/*.py")
    for py_file in py_files:
        rel = os.path.relpath(py_file, "scripts")
        nb_file = os.path.join("notebooks", rel.replace(".py", ".ipynb"))
        convert_script_to_notebook(py_file, nb_file)

if __name__ == "__main__":
    main()
