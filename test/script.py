from pathlib import Path

current_dir = Path.cwd()
current_file = Path(__file__).name

print(f"Files in the {current_dir} are:")

for filepath in current_dir.iterdir():
    if filepath.name == current_file:
        continue
    
    print(f" -  {filepath.name}")

    if filepath.is_file():
        content = filepath.read_text(encoding='utf-8').strip()
        print(f" content: {content}")
