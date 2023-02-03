import os
import sys
from tqdm import trange

def split_tsv(tsv_file, num_lines_per_split):
    f = open(tsv_file, 'r')
    lines = f.readlines()
    num_splits = len(lines) // num_lines_per_split + 1
    for i in trange(num_splits):
        out_file = f"tsv/{tsv_file}.{i}"
        if os.path.exists(out_file):
            continue
        with open(out_file, 'w') as out_f:
            for line in lines[i*num_lines_per_split:(i+1)*num_lines_per_split]:
                out_f.write(line)


if __name__ == "__main__":
    split_tsv(sys.argv[1], int(sys.argv[2]))
