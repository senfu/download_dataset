import os
import sys
import urllib.request
import hashlib
from tqdm.contrib.concurrent import process_map
from tqdm import tqdm
import zipfile
import json
TARGET_FOLDER = "junyan_cci01:/gpfs/u/home/LMCG/LMCGljnn/scratch/datasets/raw/cc3m/"


def process_single_tsv(tsv_file):
    tqdm.write(f"Processing {tsv_file}...")
    failed_url = []
    index = int(tsv_file.split(".")[-1])
    zip_file = f"images_{index}.zip"
    success_num = 0
    if os.path.exists(zip_file):
        return
    with zipfile.ZipFile(zip_file, "w") as zf:
        with open("tsv/" + tsv_file, "r") as f:
            lines = f.readlines()
        for line in lines:
            line = line.strip()
            caption, url = line.split("\t")
            filename = "cc3m_" + hashlib.md5(url.encode()).hexdigest() + ".jpg"
            if os.path.exists("images/" + filename):
                continue
            try:
                urllib.request.urlretrieve(url, "images/" + filename)
                success_num += 1
            except:
                failed_url.append(url)
                # tqdm.write(f"Failed to download {url}")
                continue
            zf.write("images/" + filename, filename)
            os.remove("images/" + filename)
    tqdm.write(f"Finished {tsv_file}. {success_num} images downloaded.")
    os.system(f"scp {zip_file} {TARGET_FOLDER} > /dev/null 2>&1")
    os.remove(zip_file)
    with open("status/" + zip_file + ".json", "w") as f:
        json.dump(failed_url, f)


if __name__ == "__main__":
    tsv_files = sorted(os.listdir("tsv"), key=lambda x: int(x.split(".")[-1]))
    os.makedirs("images", exist_ok=True)
    os.makedirs("status", exist_ok=True)
    failed_urls = process_map(
        process_single_tsv, tsv_files,
        max_workers=8, chunksize=1,
    )
    with open("failed_urls.json", "w") as f:
        json.dump(failed_urls, f)
