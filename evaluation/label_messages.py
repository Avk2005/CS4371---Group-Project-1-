"""
Label the hand-check messages in the terminal, one at a time (Part 2.3b).
Saves after every answer, so you can quit (q) and come back later.

  python evaluation/label_messages.py output/batch/hand_labels_to_fill.csv
"""
import csv
import sys
import textwrap

KEYS = {"1": "World", "2": "Sports", "3": "Business", "4": "Sci/Tech", "5": "None"}


def save(path, header, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)
        w.writerow(header)
        w.writerows(rows)


def main():
    if len(sys.argv) != 2:
        sys.exit("Usage: python evaluation/label_messages.py <hand_labels_to_fill.csv>")
    path = sys.argv[1]
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        rows = [r for r in reader]
    col = header.index("hand_label")
    total = len(rows)

    for i, r in enumerate(rows):
        if r[col].strip():
            continue  # already labelled
        print("\n" + "=" * 78)
        print(f"Message {i + 1} of {total}   (labelled so far: {sum(1 for x in rows if x[col].strip())})\n")
        print(textwrap.fill(r[header.index("text")], width=78))
        print("\n  1 World   2 Sports   3 Business   4 Sci/Tech   5 None of these   s skip   q quit")
        while True:
            ans = input("  label> ").strip().lower()
            if ans in KEYS:
                r[col] = KEYS[ans]
                save(path, header, rows)
                break
            if ans == "s":
                break
            if ans == "q":
                save(path, header, rows)
                print("Saved. Run again to continue.")
                return
            print("  Type 1-5, s or q.")

    left = sum(1 for x in rows if not x[col].strip())
    print(f"\nDone. {total - left} of {total} labelled" + (f", {left} skipped." if left else "."))


if __name__ == "__main__":
    main()
