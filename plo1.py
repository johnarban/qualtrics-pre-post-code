

import json
import os
import time
from functools import partial

from importlib import reload
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from astropy.table import Table
from IPython.display import HTML, display
import class_analytics_utils as cau
import qualtrics_data_fixes as qdf
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill
from openpyxl.utils.dataframe import dataframe_to_rows

# dont output warnings
import warnings
warnings.filterwarnings("ignore")

pd.set_option("display.max_colwidth", None)

def pprint(pandas_dataframe: pd.DataFrame, max_width=1000, **kwargs):
    return HTML(
        "\n".join(
            Table.from_pandas(pandas_dataframe).pformat(max_width=max_width, **kwargs)
        )
    )

OVERWRITE = True


id_column = "Intro information_1"


def process_survey_data(
        data_pre, description_pre, header_pre,
        data_post, description_post, header_post,
        combined_questions_filename = "2025_combined_questions.xlsx",
        retrospective_full_filename = "2025_retrospective_full_results.xlsx",
    ):

    try:
        data_pre = qdf.drop_unfinished_rows(data_pre)
        data_post = qdf.drop_unfinished_rows(data_post)
        qdf.modify_pre_data(data_pre)
        qdf.modify_post_data(data_post)
        qdf.fix_student_codes(data_pre, data_post)
    except ImportError:
        print("No qualtrics_data_fixes.py file found.  Applying no fixes.")
        pass



    id_column = description_post[header_post.index("Intro information_1")]
    instructor_column = "CosmicDS Pre-Survey - Instructor's Last Name"
    section_column = 'CosmicDS Pre-Survey - Course/Section'


    df_pre, df_post = cau.create_initial_dataframes(
        header_pre,
        description_pre,
        data_pre,
        header_post,
        description_post,
        data_post,
        id_column,
    )
    assert df_pre[id_column].is_unique, "Duplicate values found in id_column of df_pre"
    assert df_post[id_column].is_unique, "Duplicate values found in id_column of df_post"


    def process_likert_columns(df, questions):
        # convert the likert columns to numeric
        for col in df.columns:
            if col in questions["question"].values:
                if questions.loc[questions["question"] == col]["is_likert"].values[0]:
                    df[col] = df[col].apply(lambda x: cau.convert_likert_to_numeric(x))
        return df


    pre_column_filter = lambda arr: [p for p in arr if (p in df_pre.columns)]
    post_column_filter = lambda arr: [p for p in arr if (p in df_post.columns)]


    def drop_columns(columns):
        df_pre.drop(columns=pre_column_filter(columns), inplace=True)
        df_post.drop(columns=post_column_filter(columns), inplace=True)


    questions = cau.create_question_dataframe(
        df_pre, df_post, header_pre, description_pre, header_post, description_post
    )




    mlo_questions = list(filter(lambda x: "MLO" in x, questions["question"].unique()))
    mlos = questions[questions["question"].isin(mlo_questions)]



    # qeustion questions that say MLO in the question
    mlo_questions = list(filter(lambda x: "MLO" in x, questions["question"].unique()))
    questions = questions[~questions["question"].isin(mlo_questions)]
    df_pre.drop(columns=pre_column_filter(mlo_questions), inplace=True)
    df_post.drop(columns=post_column_filter(mlo_questions), inplace=True)

    cau.add_likert_columns(questions, df_pre, df_post)
    cau.add_retrospective_columns(questions)
    df_pre = process_likert_columns(df_pre, questions)
    df_post = process_likert_columns(df_post, questions)

    removed_questions = []

    questions, removed = cau.process_parent_questions(questions, df_pre, df_post)
    removed_questions.extend(removed)
    drop_columns(removed_questions)

    questions, removed = cau.process_race_questions(questions, df_pre, df_post)
    removed_questions.extend(removed)
    drop_columns(removed_questions)

    questions, removed = cau.process_gender_questions(questions, df_pre, df_post)
    removed_questions.extend(removed)
    drop_columns(removed_questions)

    questions, removed = cau.process_school_questions(questions, df_pre, df_post)
    removed_questions.extend(removed)
    drop_columns(removed_questions)

    questions, removed = cau.process_esl_questions(questions, df_pre, df_post)
    removed_questions.extend(removed)
    drop_columns(removed_questions)

    questions, removed = cau.process_activity_questions(questions, df_pre, df_post)
    removed_questions.extend(removed)
    drop_columns(removed_questions)

    questions, removed = cau.process_confused_questions(questions, df_pre, df_post)
    removed_questions.extend(removed)
    drop_columns(removed_questions)

    cau.add_content_question_meta(questions, description_pre, description_post)
    cau.add_likert_meta(questions)

    # Here we are going to add in the meta data for the questions
    # While df_pre and df_post have the questions as columns
    # this step performs a "melt" taking the wide format and making it long
    # So that each student entry now has a row for each question & it's response
    # the responses are under the column "response" and the question is under "question"
    id_columns = list(questions.groupby("question_category").get_group("intro")["question"]) + ['Finished', 'Progress']


    pre_only_questions = [
        "Parent 1 Education",
        "Parent 1 Gender",
        "Parent 2 Education",
        "Parent 2 Gender",
        "Student Race",
        "Student Gender",
        "Student School Level",
        "Student Grade Level",
        "Student ESL",
    ]

    df_pre_merged, df_post_merged = cau.merge_questions_into_dataframes(
        df_pre, df_post, questions, id_column, id_columns, pre_only_questions
    )




    print("Pre-test merged records: ", len(df_pre_merged))
    # display(pprint(df_pre_merged.head(5), html=True))
    print("Post-test merged records: ", len(df_post_merged))
    # display(pprint(df_post_merged.head(5), html=True))

    # This line counts the number of questions in each (question_category, group)
    # combination by exploding the 'groups' column and grouping by 'question_category' and 'groups'.
    # print("Number of questions in each question qategory and group:")
    # questions.explode("groups").groupby(["question_category", "groups"]).count().rename(
    #     {"question": "count"}, axis=1
    # )["count"]


    # In[12]:

    matched_ids = list(
        set(df_pre_merged[id_column]).intersection(df_post_merged[id_column])
    )
    orphraned_pre = list(
        set(df_pre_merged[id_column]) - set(df_post_merged[id_column])
    )
    orphraned_post = list(
        set(df_post_merged[id_column]) - set(df_pre_merged[id_column])
    )

    print("Number of orphaned pre ids:", len(orphraned_pre))
    print("Number of orphaned post ids:", len(orphraned_post))

    matched_idx = df_pre_merged[df_pre_merged[id_column].isin(matched_ids)].index
    matched_idx_post = df_post_merged[df_post_merged[id_column].isin(matched_ids)].index
    print("Number of matched ids:", len(matched_ids))

    # merging on the id_column and the questions
    # By doing an outer join,
    df_combined = pd.merge(
        df_pre_merged,
        df_post_merged,
        on=[id_column] + questions.columns.tolist(),
        how="outer",  # inner join to keep only matching rows
        suffixes=("_pre", "_post"),
    )

    def notnull(value):
        return value not in ['(empty)', '', None, np.nan]
    def has_pre_and_post_respose(row):
        return notnull(row["response_pre"]) and notnull(row["response_post"])



    df_combined["matched"] = df_combined[id_column].apply(
        lambda x: x in matched_ids
    )

    df_combined["orphaned_pre"] = df_combined[id_column].apply(
        lambda x: x in orphraned_pre
    )
    df_combined["orphaned_post"] = df_combined[id_column].apply(
        lambda x: x in orphraned_post
    )

    c = list(
        [id_column] + pre_only_questions
        + ['matched', 'orphaned_pre', 'orphaned_post']
        + questions.columns.tolist()
        + ["response_pre", "correct_pre", "response_post", "correct_post"]
        + [c + "_pre" for c in id_columns[1:]]
        + [c + "_post" for c in id_columns[1:]]
    )

    df_combined = df_combined[c]
    # df_combined = df_combined.rename(columns={"question": "question"})


    # get the appropriate classroom data
    df_combined = cau.get_class_info(df_combined, id_column, skip_db=True)

    print(f"There are {len(df_combined)} rows in the combined dataframe")
    print(f"There are {len(df_combined[df_combined['matched']])} rows with matched ids")


    # In[13]:


    # some basic stats
    # print(f"Unique IDs in combined: {len(df_combined[id_column].unique())}")
    # print(f"Unique IDs in pre-survey: {len(df_pre[id_column].unique())}")
    # print(f"Unique IDs in post-survey: {len(df_post[id_column].unique())}")
    # print(f"Unique IDs in merged pre-survey: {len(df_pre_merged[id_column].unique())}")
    # print(f"Unique IDs in merged post-survey: {len(df_post_merged[id_column].unique())}")

    missing_in_pre = set(df_post_merged[id_column]) - set(df_pre_merged[id_column])
    missing_in_post = set(df_pre_merged[id_column]) - set(df_post_merged[id_column])

    # print(f"IDs in post but not in pre: {len(missing_in_pre)}")
    # print(f"IDs in pre but not in post: {len(missing_in_post)}")
    # print("List of IDs in post but not in pre:", missing_in_pre)
    # print("List of IDs in pre but not in post:", missing_in_post)


    student_states = cau.get_student_states(df_combined, id_column, skip_db=True)
    # student_states.drop(columns=["free_responses", "mc_scoring"])


    def place_column_beside(df, column_name, beside_column, to_right=True):
        """
        Place a column beside another column in a DataFrame.
        If to_right is True, place it to the right of the beside_column.
        If to_right is False, place it to the left of the beside_column.
        """
        cols = list(df.columns)
        idx = cols.index(beside_column)
        if column_name in cols:
            cols.remove(column_name)
        if to_right:
            cols.insert(idx + 1, column_name)
        else:
            cols.insert(idx, column_name)
        return df[cols]




    df_copy = df_combined.copy() #.groupby(['matched', 'both']).get_group((True, True))
    df_copy["Student ESL"] = df_copy["Student ESL"].apply(cau.clean_esl)
    df_copy["student_esl_value_y1_n0"] = df_copy["Student ESL"].map({'Yes': 1, 'No': 0}).astype(pd.Int64Dtype())
    df_copy["Student Gender"] = df_copy["Student Gender"].apply(cau.clean_gender)
    df_copy["Student_Gender_Value_M0_F1_X2"] = df_copy["Student Gender"].map({'Male':0, 'Female':1, 'Other': 2}).astype(pd.Int64Dtype())
    df_copy = place_column_beside(
        df_copy,
        "Student_Gender_Value_M0_F1_X2",
        "Student Gender")

    df_copy["Student Race"] = df_copy["Student Race"].apply(cau.clean_race)
    df_copy["student_race_value_W1_B2_A3_NHPI4_Mixed5"] = df_copy["Student Race"].map({
        'White': 1,
        'Black or African American': 2,
        'Asian': 3,
        'Native Hawaiian or Other Pacific Islander': 4,
        'Multi-ethnic': 5,
        }).astype(pd.Int64Dtype())
    df_copy = place_column_beside(
        df_copy,
        "student_race_value_W1_B2_A3_NHPI4_Mixed5",
        "Student Race",
    )


    # using the questions dataframe create a dictionary to map {'question': 'short_question'}
    question_map = {}
    for _, row in questions.iterrows():
        question_map[row["question"]] = row["short_question"]

    def get_question_label(question):
        mapped = question_map.get(question, question)
        if pd.isna(mapped) or str(mapped).strip() == "":
            return question
        return mapped

    df_copy['question'] = df_copy['question'].map(get_question_label)
    def invert_likert(row, col="correct_pre"):
        val = row[col]
        if row["is_likert"]:
            if row["likert_negate"]:
                return 5 - val
        return val
    def norm(row, col = "correct_pre", negate=False):
        if row["is_likert"]:
            return 0.2 * row[col]
        return row[col]



    df_copy["correct_pre"] = df_copy.apply(
        partial(invert_likert, col="correct_pre"), axis=1
    )
    df_copy["correct_post"] = df_copy.apply(
        partial(invert_likert, col="correct_post"), axis=1
    )

    df_copy["norm"] = df_copy.apply(norm, axis=1)
    df_copy["normed_correct_pre"] = df_copy.apply(
        partial(norm, col="correct_pre"), axis=1
    )
    df_copy["normed_correct_post"] = df_copy.apply(
        partial(norm, col="correct_post"), axis=1
    )



    education_map = {
        "": 0,
        "Less than High School": 1,
        "High School Diploma/GED": 2,
        "Some College/ Associate Degree": 3,
        "Bachelor’s Degree": 4,
        "Master’s Degree or higher": 5,
    }
    reversed_education_map = {v: k for k, v in education_map.items()}
    # Map education levels to integers
    p1_edu = df_copy["Parent 1 Education"].map(education_map)
    p2_edu = df_copy["Parent 2 Education"].map(education_map)
    # get the highest of the two parents' education levels
    df_copy["Parent Education"] = (p1_edu
                                .combine(p2_edu, func=lambda x, y: max(x, y))
                                .map(
                                    reversed_education_map
                                    )
                                    )
    df_copy["parent_education_value"] = df_copy["Parent Education"].map(education_map).astype(pd.Int64Dtype())
    df_copy = place_column_beside(
        df_copy,
        "parent_education_value",
        "Parent Education"
    )

    questions_both = df_copy.pivot(
        index=id_column,
        columns=["question"],
        values=["correct_pre", "correct_post", "normed_correct_pre", "normed_correct_post",],
        # aggfunc="first"
    )
    # questions_both.columns = ['++'.join(map(str, col)).strip() for col in questions_both.columns]
    # questions_both.columns = pd.Index(
    #     [c[0] for c in questions_both.columns]
    # )

    # Merge back the rest of the non-pre/post q columns (demographics, etc)
    questions_both = pd.merge(
        cau.to_multindex(df_copy.drop_duplicates(subset=[id_column]).set_index(id_column)),
        questions_both,
        on=id_column,
        how="left"
    )

    drop = [
    'question',
    'both',
    'in_pre',
    'in_post',
    'tag_pre',
    'tag_post',
    'question_category',
    'is_likert',
    'is_retrospective',
    'groups',
    'answer',
    'short_question',
    'likert_group',
    'likert_negate',
    "CosmicDS Pre-Survey - Instructor's Last Name_pre",
    'CosmicDS Pre-Survey - Course/Section_pre',
    "CosmicDS Pre-Survey - Instructor's Last Name_post",
    'CosmicDS Pre-Survey - Course/Section_post',
    'Student School Level'
    #  'normed_correct_pre',
    #  'normed_correct_post',
    ]
    questions_both.drop(columns=drop, inplace=True)



    questions_both['student_id'] = questions_both['student_id'].astype(str)
    questions_both = cau.to_multindex(questions_both)


    questions_both["Pre Score"] = questions_both["normed_correct_pre"].sum(axis=1) # type: ignore
    questions_both["Post Score"] = questions_both["normed_correct_post"].sum(axis=1) # type: ignore
    questions_both['score_gain'] = questions_both["Post Score"] - questions_both["Pre Score"]



    def score_questions(selected_questions, name):
        def safe_mean(row):
            N_q = len(row)
            N_nan = row.isna().sum()
            if N_nan == N_q:
                return np.nan
            elif N_nan == 0:
                return row.sum() / N_q
            else:
                return row.sum() / (N_q - N_nan)
            
        questions_both[name + "_score_pre"] = questions_both['correct_pre'][selected_questions].apply(safe_mean, axis=1).astype(float)
        questions_both[name + "_score_post"] = questions_both['correct_post'][selected_questions].apply(safe_mean, axis=1).astype(float)
        questions_both[name + "_score_gain"] = questions_both[name + "_score_post"] - questions_both[name + "_score_pre"].astype(float)

    score_questions(
        questions.groupby("question_category").get_group("science")['short_question'].to_list(),
        "content"
    )


    score_questions(
        questions.groupby("question_category").get_group("likert")['short_question'].to_list(), 
        "likert"
    )

    score_questions(
        questions.groupby("question_category").get_group("retrospective")['short_question'].to_list(),
        "retrospective"
    )

    for qtype in ['science', 'likert']:
        groups = questions.groupby('question_category').get_group(qtype)['groups'].explode().unique()
        for group in groups:
            if pd.isna(group):
                continue
            selected_group = (questions
                            .groupby('question_category')
                            .get_group(qtype)
                            .explode('groups')
                            .groupby('groups')
                            .get_group(group)['short_question'].unique().tolist())
            score_questions(
                selected_questions=selected_group,
                name = group
            )
            

    # replace (empty) with pd.na, and other empty strings with pd.NA
    questions_both.replace(
        to_replace=['(empty)', '', None],
        value=pd.NA,
        inplace=True
    )



    # reorder the rows so that they go
    # matched, orphaned_pre, orphaned_post
    questions_both = questions_both.sort_values(
        by=[('matched', ''), ('orphaned_pre', ''), ('orphaned_post', '')], # type: ignore
        ascending=[False, False, False]
    ) 
    questions_both.reset_index(inplace=True)
    questions_both.dropna(axis='columns', how='all', inplace=True)

    # drop normed columns


    # questions_both.drop(columns=["normed_correct_pre", "normed_correct_post"]).to_excel(
    #     "2025_combined_questions_row.xlsx",
    #     index=True,
    #     engine="openpyxl",
    #     sheet_name="2025_combined"
    # )

    # questions_both.groupby('matched').get_group(True)[
    #     list(x for x in filter(lambda x: 'score_gain' in x[0].lower(), questions_both.columns)) + [('retrospective_score_post','')]
    # ]

    with cau.check_if_exists("2025_student_free_responses.xlsx", overwrite=OVERWRITE, error=True) as filename:
        if filename:
            pd.concat(
                [student_states,
                pd.DataFrame(student_states['free_responses'].to_dict()).T],
                axis=1
            ).drop(columns=['free_responses', 'mc_scoring']).to_excel(
                filename,
                index=True,
                engine="openpyxl",
                sheet_name="2025_student_states"
            )

    student_states['student_id'] = student_states['student_id'].astype(str)

    questions_both = cau.to_multindex(pd.merge(
        (questions_both.reset_index()),
        cau.to_multindex(student_states.drop(columns=['mc_scoring', 'free_responses']).reset_index()),
        left_on='student_id',
        right_on='student_id',
        how="left"
    ))

    def clean_columns(df):
        columns = ['_'.join(col.split()) for col in df.columns]
        df.columns = columns
        return df

    output_drop_columns = ["normed_correct_pre", "normed_correct_post", "response_pre", "response_post", "correct_pre", "correct_post"]
    with cau.check_if_exists(combined_questions_filename, overwrite=OVERWRITE, error=True) as filename:
        if filename:
            clean_columns(cau.to_flatindex(questions_both.drop(columns=output_drop_columns), string=True)).to_excel(
                filename,
                index=True,
                engine="openpyxl",
                sheet_name="2025_combined"
            )


    # In[17]:


    import statsmodels.api as sm

    df = questions_both.copy().reset_index()
    # flatten the multi-index columns
    df.columns = ['_'.join(map(str, col)).strip().strip('_') for col in df.columns]
    # Create outcome variable
    df["score_gain"] = df["Post Score"] - df["Pre Score"]


    # Same for Student Race

    # Drop or fill missing values
    df = df.dropna(subset=[
        "score_gain", 
        "Student Gender", 
        'Student_Gender_Value_M0_F1_X2', 
        "Student Race", 
        'student_race_value_W1_B2_A3_NHPI4_Mixed5',
        "Parent Education",
        'parent_education_value',
        "Educator", 
        "Pre Score", 
        "Post Score"])

    # Select predictors
    demographics = df[["Student Gender", "Student Race", "Parent Education", "Student ESL", "Educator"]]

    # One-hot encode the categorical variables
    demographics_encoded = pd.get_dummies(demographics, drop_first=True)
    
    # 
    if False:
        # Create X and y
        X = demographics_encoded
        y = df["score_gain"]  # or df["Post Score"] if you prefer

        # Add constant for intercept
        # X = sm.add_constant(X).astype(bool)

        # Fit linear regression model
        model = sm.OLS(y, X).fit()

        # View results
        print(model.summary())


    # In[18]:


    print("Response Summary")
    response_summary = (
        df_combined.groupby("both")
        .get_group(True)
        .drop_duplicates(subset=[id_column])
        .groupby(["class_name", "Educator", "class_id"])
        .agg(
            {
                "response_pre": lambda x: x.notnull().sum(),
                "response_post": lambda x: x.notnull().sum(),
                "matched": lambda x: x.sum(),
                "orphaned_pre": lambda x: x.sum(),
                "orphaned_post": lambda x: x.sum(),
            }
        )
        .assign(post_response_rate=lambda df: df["matched"] / (df["response_pre"]))
        .reset_index()
    )

    print(f"Overall response rate: {response_summary['post_response_rate'].mean() * 100:.2f}%")



    # In[19]:


    # print("Matched totals")
    # (df_combined
    # .groupby('Educator')
    # .apply(
    #     lambda group: group.drop_duplicates(subset=[id_column])["matched"].sum()
    #     )
    #     )
    
    with open("retrospective_questions.json", "r") as f:
        retro_qs = json.load(f)

    retro = df_combined[df_combined["question"].isin([q["question"] for q in retro_qs])].copy()
    pre_retro = [q["question"] for q in retro_qs if q["which"] == "pre"]
    post_retro = [q["question"] for q in retro_qs if q["which"] == "post"]

    cols = [
        "student_id",
        "Educator",
        "class_name",
        "class_id",
        "question_category",
        "question",
        "tag_post",
        "short_question",
        "response_post",
        "correct_post",
    ]

    qid2 = retro[retro["question"].isin(pre_retro)][cols].copy()
    qid3 = retro[retro["question"].isin(post_retro)][cols].copy()

    qid2 = qid2.rename(
        columns={
            "question": "question_pre",
            "tag_post": "tag_pre",
            "short_question": "short_question_pre",
            "response_post": "response_pre",
            "correct_post": "score_pre",
        }
    )

    qid3 = qid3.rename(
        columns={
            "question": "question_post",
            "short_question": "short_question_post",
            "correct_post": "score_post",
        }
    )

    compare = pd.merge(
        qid2,
        qid3,
        on=["student_id", "Educator", "class_name", "class_id", "question_category"],
        how="outer",
    )

    compare["response_change"] = compare.apply(
        lambda row: f"{row['response_pre']} -> {row['response_post']}"
        if pd.notna(row["response_pre"]) or pd.notna(row["response_post"])
        else pd.NA, # type: ignore
        axis=1,
    ) 
    compare["score_change"] = compare["score_post"] - compare["score_pre"]
    # compare = compare.sort_values(["Educator", "class_name", "student_id"], na_position="last")
    # check that there are 2 of every unique student_id
    student_counts = compare["student_id"].value_counts()
    if not all(student_counts == 1):
        print("Warning: Not all student_ids have 1 entry in the compare dataframe")
        print(student_counts[student_counts != 1])

    # In[46]:


    with pd.ExcelWriter(retrospective_full_filename, engine="openpyxl") as writer:
        qid2.sort_values(["Educator", "class_name", "student_id"], na_position="last").to_excel(
            writer, sheet_name="QID2_pre", index=False
        )
        qid3.sort_values(["Educator", "class_name", "student_id"], na_position="last").to_excel(
            writer, sheet_name="QID3_post", index=False
        )
        compare.to_excel(writer, sheet_name="student_compare", index=False)
