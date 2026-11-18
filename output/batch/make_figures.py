"""
CS4371 GP1 - report figures from the Part 2 output files.

Reads the files batch_predict.py and score_hand_labels.py already wrote, so nothing
has to be re-run. Needs: pip install matplotlib pandas

  python make_figures.py --batch-dir output/batch --out-dir report_figures

Makes:
  fig3_class_distribution.png  messages per predicted class (2.3a)
  fig4_confusion.png           hand label vs model prediction, 50 messages (2.3b)
  fig6_confidence.png          model confidence per predicted class (optional, for 4.4)
"""
import argparse
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

CLASSES = ["World", "Sports", "Business", "Sci/Tech"]
BLUE = "#3b6fb6"
GREY = "#9aa0a6"


def parse_args():
    p = argparse.ArgumentParser(description="Make report figures from GP1 Part 2 outputs")
    p.add_argument("--batch-dir", required=True,
                   help="Folder with predictions.csv and hand_labels_vs_model.csv")
    p.add_argument("--out-dir", required=True, help="Folder to save the PNG figures")
    return p.parse_args()


def style(ax):
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    ax.tick_params(length=0)


def fig_class_distribution(pred, out):
    counts = pred["predicted_class"].value_counts().reindex(CLASSES, fill_value=0)
    counts = counts.sort_values()
    total = counts.sum()
    fig, ax = plt.subplots(figsize=(7, 3.2), dpi=200)
    bars = ax.barh(counts.index, counts.values, color=BLUE)
    for b, v in zip(bars, counts.values):
        ax.text(b.get_width() + total * 0.01, b.get_y() + b.get_height() / 2,
                f"{v:,}  ({100 * v / total:.1f}%)", va="center", fontsize=9)
    ax.set_xlim(0, counts.max() * 1.25)
    ax.set_xlabel("Messages")
    ax.set_title(f"Predicted topic of {total:,} streamed Hacker News messages", loc="left", fontsize=11)
    style(ax)
    fig.tight_layout()
    fig.savefig(out)
    plt.close(fig)


def fig_confusion(hand, out):
    rows = CLASSES + ["None"]
    m = pd.crosstab(hand["hand_label"], hand["predicted_class"]).reindex(
        index=rows, columns=CLASSES, fill_value=0)
    fig, ax = plt.subplots(figsize=(6.5, 4.6), dpi=200)
    ax.imshow(m.values, cmap="Blues", vmin=0)
    ax.set_xticks(range(len(CLASSES)), CLASSES)
    ax.set_yticks(range(len(rows)), [r if r != "None" else "None of these" for r in rows])
    vmax = max(m.values.max(), 1)
    for i in range(len(rows)):
        for j in range(len(CLASSES)):
            v = int(m.values[i, j])
            ax.text(j, i, v, ha="center", va="center", fontsize=10,
                    color="white" if v > vmax * 0.6 else "black")
    # separate the "None" row: these can never be correct
    ax.axhline(len(CLASSES) - 0.5, color="black", linewidth=1)
    fits = hand[hand["hand_label"] != "None"]
    acc = (fits["hand_label"] == fits["predicted_class"]).mean() if len(fits) else 0
    ax.set_xlabel("Model prediction")
    ax.set_ylabel("Our hand label")
    correct = int((fits["hand_label"] == fits["predicted_class"]).sum())
    ax.set_title(f"Hand check: {correct}/{len(fits)} correct ({acc:.0%})\n"
                 f"{len(hand) - len(fits)} of {len(hand)} messages fit no class",
                 loc="left", fontsize=10)
    # outline the diagonal = correct predictions
    for k in range(len(CLASSES)):
        ax.add_patch(plt.Rectangle((k - 0.5, k - 0.5), 1, 1, fill=False, edgecolor="black", linewidth=1.5))
    ax.tick_params(length=0)
    fig.tight_layout()
    fig.savefig(out)
    plt.close(fig)


def fig_confidence(pred, out):
    present = [c for c in CLASSES if (pred["predicted_class"] == c).any()]
    data = [pred.loc[pred["predicted_class"] == c, "probability"] for c in present]
    fig, ax = plt.subplots(figsize=(7, 3.2), dpi=200)
    # drawn by hand (not ax.boxplot) so it works on any matplotlib version
    for y, s in enumerate(data):
        q10, q25, q50, q75, q90 = s.quantile([0.1, 0.25, 0.5, 0.75, 0.9])
        ax.hlines(y, q10, q90, color=GREY, linewidth=1)                 # middle 80%
        ax.hlines(y, q25, q75, color=BLUE, linewidth=10, alpha=0.45)    # middle 50%
        ax.plot(q50, y, "o", color="black", markersize=4)               # median
        ax.text(1.02, y, f"median {q50:.2f}", va="center", ha="left", fontsize=8)
    ax.set_yticks(range(len(present)), present)
    ax.set_ylim(-0.6, len(present) - 0.4)
    ax.axvline(0.25, color=GREY, linestyle="--", linewidth=1)
    ax.text(0.255, len(present) - 0.55, "random guess (0.25)", fontsize=8, color=GREY)
    ax.set_xlim(0.2, 1.0)
    ax.set_xticks([0.25, 0.5, 0.75, 1.0])
    ax.set_xlabel("Probability of the predicted class  (bar = middle 50%, line = middle 80%, dot = median)")
    ax.set_title("How confident the model is, by predicted class", loc="left", fontsize=11)
    style(ax)
    fig.tight_layout()
    fig.savefig(out)
    plt.close(fig)


def main():
    args = parse_args()
    os.makedirs(args.out_dir, exist_ok=True)

    pred = pd.read_csv(os.path.join(args.batch_dir, "predictions.csv"), keep_default_na=False)
    fig_class_distribution(pred, os.path.join(args.out_dir, "fig3_class_distribution.png"))
    fig_confidence(pred, os.path.join(args.out_dir, "fig6_confidence.png"))
    print("Saved fig3_class_distribution.png and fig6_confidence.png")

    hand_path = os.path.join(args.batch_dir, "hand_labels_vs_model.csv")
    if os.path.exists(hand_path):
        hand = pd.read_csv(hand_path, keep_default_na=False)  # keep the word "None"
        fig_confusion(hand, os.path.join(args.out_dir, "fig4_confusion.png"))
        print("Saved fig4_confusion.png")
    else:
        print(f"Skipped fig4: {hand_path} not found (run score_hand_labels.py first)")


if __name__ == "__main__":
    main()
