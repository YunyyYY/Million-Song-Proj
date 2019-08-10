from utils import get_all_files

paths = get_all_files('A')
with open('dir.txt', 'w') as f:
    for i in paths:
        f.write(i+"\n")