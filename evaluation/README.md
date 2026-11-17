# evaluation: Batch predictions and hand check (Part 2.3a, 2.3b)

## 1. Batch predictions (2.3a)
```bash
python evaluation/batch_predict.py --model models/agnews_lr \
    --messages output/cleaned_messages --out-dir output/batch
```
Loads the saved PipelineModel and predicts a class for every cleaned message. Writes to `--out-dir`:

| File | Contents |
|---|---|
| `predictions.csv` | Every message: `id`, `text`, `predicted_class`, `probability` |
| `class_counts.csv` | Messages per predicted class and share |
| `examples_per_class.md` | 5 random messages per class with their probability |
| `hand_labels_to_fill.csv` | 50 random messages with an empty `hand_label` column (no predictions shown) |
| `hand_sample_predictions.csv` | The model's predictions for those 50 (kept separate so labelling is blind) |

## 2. Label the 50 messages by hand
Open `hand_labels_to_fill.csv` (Excel, Google Sheets or VS Code) and fill `hand_label` with one of:
`World`, `Sports`, `Business`, `Sci/Tech`, `None` (None = none of these). Save as CSV.

## 3. Score (2.3b)
```bash
python evaluation/score_hand_labels.py --labels output/batch/hand_labels_to_fill.csv \
    --predictions output/batch/hand_sample_predictions.csv --out-dir output/batch
```
Writes `hand_labels_vs_model.csv` (hand labels next to predictions) and `hand_check_summary.md`
(counts, accuracy on messages that fit a class, confusion table, every mismatch for the error summary).

Options: `--sample-size` (default 50), `--examples` (default 5), `--seed` (default 13).
