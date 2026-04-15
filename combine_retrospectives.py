import pandas as pd
from pathlib import Path

retro_dir = Path("retrospectives")
files = sorted(retro_dir.glob("*_retrospective_full_results.xlsx"))

dfs = []
for f in files:
    df = pd.read_excel(f, sheet_name="student_compare")
    df.insert(0, "survey", f.stem.replace("_retrospective_full_results", ""))
    dfs.append(df)

combined = pd.concat(dfs, ignore_index=True)
# combined.to_excel(retro_dir / "all_student_compare.xlsx", index=False)
# print(f"Wrote {len(combined)} rows from {len(files)} files to all_student_compare.xlsx")

from scipy.stats import wilcoxon
from class_analytics_utils import wilcoxon_signed_rank_test
combined.dropna(inplace=True)
pre = combined['score_pre']
post = combined['score_post']

# Perform the Wilcoxon signed-rank test
print(wilcoxon_signed_rank_test(pre, post))

def normalized_change(pre: int, post: int, min_score: int = 1, max_score: int = 7) -> float:
    """
    Compute normalized change for a bounded score.

    Returns:
        A value in [-1, 1]:
        - positive for improvement
        - 0 for no change
        - negative for decline
    """
    if pre < min_score or pre > max_score or post < min_score or post > max_score:
        raise ValueError("pre and post must be within the score bounds")

    if post > pre:
        return (post - pre) / (max_score - pre)
    elif post < pre:
        return (post - pre) / (pre - min_score)
    else:
        return 0.0
    
normalized_changes = combined.apply(lambda row: normalized_change(row['score_pre'], row['score_post']), axis=1)
# wilcoxon(
#             (post - pre),
#             zero_method="wilcox",
#             alternative="greater",
#             correction=False,
#             method="approx",
w = wilcoxon(post-pre, zero_method="wilcox", alternative="greater", correction=False, method="approx")
