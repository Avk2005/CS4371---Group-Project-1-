"""
CS4371 Group Project 1 - Part 2.3b
Compare your 50 hand labels with the model's predictions.

Run AFTER filling the hand_label column of hand_labels_to_fill.csv with one of:
  World, Sports, Business, Sci/Tech, None   ("None" = None of these)

Outputs (in --out-dir):
  hand_labels_vs_model.csv  - id, text, hand_label, predicted_class, probability, match
  hand_check_summary.md     - counts, accuracy, confusion table, every mismatch (for the error summary)

Example:
  python evaluation/score_hand_labels.py --labels output/batch/hand_labels_to_fill.csv \
      --predictions output/batch/hand_sample_predictions.csv --out-dir output/batch
"""
import argparse
import os
import sys

import pandas as pd

CLASSES = ["World", "Sports", "Business", "Sci/Tech"]
ALIASES = {"world": "World", "sports": "Sports", "sport": "Sports", "business": "Business",
           "sci/tech": "Sci/Tech", "scitech": "Sci/Tech", "sci tech": "Sci/Tech", "tech": "Sci/Tech",
           "none": "None", "none of these": "None"}


def parse_args():
    p = argparse.ArgumentParser(description="GP1 Part 2.3b: score hand labels against predictions")
    p.add_argument("--labels", required=True, help="hand_labels_to_fill.csv with hand_label filled in")
    p.add_argument("--predictions", required=True, help="hand_sample_predictions.csv from batch_predict.py")
    p.add_argument("--out-dir", required=True, help="Folder for the output files")
    p.add_argument("--all-predictions", default=None,
                   help="predictions.csv from batch_predict.py (default: next to --predictions); "
                        "used to match rows by text when spreadsheet software changed the ids")
    return p.parse_args()


def norm(text):
    return " ".join(str(text).split())


def main():
    args = parse_args()
    labels = pd.read_csv(args.labels, dtype={"hand_label": str}, keep_default_na=False)  # keep the word "None"
    preds = pd.read_csv(args.predictions)
    labels["hand_label"] = labels["hand_label"].fillna("").str.strip().str.lower().map(ALIASES)

    bad = labels[labels["hand_label"].isna()]
    if len(bad):
        print(f"{len(bad)} rows have a missing or invalid hand_label (use World, Sports, Business, Sci/Tech, None):")
        print(bad[["id"]].to_string(index=False))
        sys.exit(1)

    # Match on id first; spreadsheet apps can rewrite long ids (1.51E+12), so fall back to the text.
    labels["id"] = labels["id"].astype(str)
    preds["id"] = preds["id"].astype(str)
    df = labels.merge(preds, on="id", how="inner")
    if len(df) < len(labels):
        allp_path = args.all_predictions or os.path.join(os.path.dirname(args.predictions), "predictions.csv")
        allp = pd.read_csv(allp_path, keep_default_na=False)
        allp["key"] = allp["text"].map(norm)
        labels["key"] = labels["text"].map(norm)
        df = labels.drop(columns=["id"]).merge(
            allp[["key", "id", "predicted_class", "probability"]], on="key", how="inner").drop(columns=["key"])
        print(f"Matched rows by message text ({len(df)} of {len(labels)}); ids had been changed.")
    if len(df) < len(labels):
        print(f"WARNING: only {len(df)} of {len(labels)} labelled rows could be matched to predictions.")
    df["match"] = df["hand_label"] == df["predicted_class"]
    fits = df[df["hand_label"] != "None"]
    n, n_fit, n_none = len(df), len(fits), int((df["hand_label"] == "None").sum())
    correct = int(fits["match"].sum())
    acc = correct / n_fit if n_fit else 0.0

    os.makedirs(args.out_dir, exist_ok=True)
    df[["id", "text", "hand_label", "predicted_class", "probability", "match"]].to_csv(
        os.path.join(args.out_dir, "hand_labels_vs_model.csv"), index=False)

    conf = pd.crosstab(df["hand_label"], df["predicted_class"]).reindex(
        index=CLASSES + ["None"], columns=CLASSES, fill_value=0)
    md = ["# Part 2.3b hand check", "",
          f"- Messages hand-labelled: {n}",
          f"- Fit one of the four classes: {n_fit}",
          f"- None of these: {n_none}",
          f"- Model correct on the {n_fit} that fit: {correct} (accuracy {acc:.1%})", "",
          "## Hand label (rows) vs model prediction (columns)", "",
          "| Hand label | " + " | ".join(CLASSES) + " |", "| --- |" + " --- |" * len(CLASSES)]
    for lab, row in conf.iterrows():
        md.append(f"| {lab} | " + " | ".join(str(int(v)) for v in row) + " |")
    md += ["", "## Every mismatch (use these for the error summary)", "",
           "| Hand label | Predicted | Prob. | Message |", "| --- | --- | --- | --- |"]
    for _, r in df[~df["match"]].iterrows():
        text = " ".join(str(r["text"]).split())[:180].replace("|", "/")
        md.append(f"| {r['hand_label']} | {r['predicted_class']} | {r['probability']:.3f} | {text} |")
    with open(os.path.join(args.out_dir, "hand_check_summary.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(md) + "\n")

    print(f"Hand-labelled: {n}  |  fit a class: {n_fit}  |  None of these: {n_none}")
    print(f"Accuracy on the {n_fit} that fit: {correct}/{n_fit} = {acc:.1%}")
    print(f"Saved hand_labels_vs_model.csv and hand_check_summary.md to {args.out_dir}")


if __name__ == "__main__":
    main()
