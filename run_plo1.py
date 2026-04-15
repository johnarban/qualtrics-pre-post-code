import pandas as pd
from astropy.table import Table
from pathlib import Path



import os


import class_analytics_utils as cau


pd.set_option("display.max_colwidth", None)


USE_FRESH_DATA = False
OVERWRITE = True

import plo1 as plo1


# import all keys from qualtrics_keys
from qualtrics_keys import (
    survey_id_dict
    )

id_column = "Intro information_1"


for which_survey in survey_id_dict.keys():
    print(f"Processing survey: {which_survey}")

    # pre_response = cau.load_pre_data(from_api=False)
    file_exists = os.path.exists(survey_id_dict[which_survey]["output_name"]+'.csv')

    pre_response = cau.load_data(
        from_api=USE_FRESH_DATA,
        survey_id=survey_id_dict[which_survey]["pre"],
        # filename="2025_pre_response.csv",
        filename=Path("./surveys") / (survey_id_dict[which_survey]["output_name"]+'_pre.csv'),
    )

    post_response = cau.load_data(
        from_api=USE_FRESH_DATA,
        survey_id=survey_id_dict[which_survey]["post"],
        # filename="2025_post_response.csv",
        filename=Path("./surveys") / (survey_id_dict[which_survey]["output_name"]+'_post.csv'),
    )
        
        
    header_pre, description_pre, data_pre = cau.parse_response(pre_response)

    header_post, description_post, data_post = cau.parse_response(post_response)


    # updates some columns to match properly, and drop columns in the list
    drop_columns = [
        "CosmicDS Pre-Survey - Click to write Form Field 4",
        'Intro information_5',
        "The CosmicDS team has my permission to use my anonymized survey data in their evaluation process."
        ]
    data_pre, data_post = cau.column_cleanup(
        header_pre,
        description_pre,
        data_pre,
        header_post,
        description_post,
        data_post,
        drop_columns=drop_columns,
    )

    for question in description_post:
        # makes question tags match between matching questions
        # probably don't need to do this since we do everything based on the question text
        cau.get_pre_header_for_post_question(
            question,
            header_pre,
            description_pre,
            header_post,
            description_post,
            verbose=False,
        )
        
    plo1.process_survey_data(
    data_pre, description_pre, header_pre,
    data_post, description_post, header_post,
    combined_questions_filename = (which_survey+"_combined_questions.xlsx"),
    retrospective_full_filename = 
        str(Path("./retrospectives") / (which_survey+"_retrospective_full_results.xlsx"))
)




    # print out the number of records
    print("\n\n\n Number of records in Pre/Post survey")
    print("Pre-survey records: ", len(data_pre))
    print("Post-survey records: ", len(data_post))