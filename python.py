import csv
import random
import time
import os
from pathlib import Path

# ==== 設定 ====
TOTAL_RECORDS = 100_000_000
RECORDS_PER_FILE = 1_000_000
OUTPUT_DIR = Path("csv_output")
SENTENCES_FILE = "sentences.txt"

last_names = ["山田", "佐藤", "高橋", "田中", "伊藤", "中村", "小林", "加藤", "吉田", "斎藤"]
first_names = ["太郎", "花子", "一郎", "美咲", "健太", "優子", "翔", "玲奈", "直樹", "舞"]

def load_sentences(path):
    with open(path, encoding='utf-8') as f:
        return [line.strip() for line in f if line.strip()]

def random_name():
    return random.choice(last_names) + random.choice(first_names)

def generate_csv_stream(start_id, count, file_index, sentences):
    file_name = OUTPUT_DIR / f"output_{file_index:05d}.csv"
    with open(file_name, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "memo"])

        for i in range(count):
            row_id = start_id + i
            name = random_name()
            memo = random.choice(sentences)
            writer.writerow([row_id, name, memo])

def main():
    start = time.time()
    print(f"📦 全{TOTAL_RECORDS:,}件 → {TOTAL_RECORDS // RECORDS_PER_FILE}ファイルで生成")
    OUTPUT_DIR.mkdir(exist_ok=True)

    sentences = load_sentences(SENTENCES_FILE)
    if not sentences:
        raise RuntimeError("sentences.txt が空です")

    file_count = TOTAL_RECORDS // RECORDS_PER_FILE
    for file_index in range(file_count):
        start_id = file_index * RECORDS_PER_FILE + 1
        t0 = time.time()
        generate_csv_stream(start_id, RECORDS_PER_FILE, file_index + 1, sentences)
        print(f"✅ output_{file_index+1:05d}.csv 完了（{RECORDS_PER_FILE:,}件）[{time.time() - t0:.2f}s]")

    print(f"\n🎉 全ファイル完了！所要時間: {time.time() - start:.2f}秒")

if __name__ == "__main__":
    main()
