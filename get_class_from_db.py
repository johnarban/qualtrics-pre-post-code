from collections import Counter
from typing import cast, Optional
from qualtrics_keys import database_keys

import pandas as pd

import mysql.connector


def connect_to_db(database_dict=None):
    # connect to the database
    if database_dict is None:
        database_dict = {
            "host": "localhost",
            "user": "user",
            "password": "password",
            "database": "database",
        }
    mydb = mysql.connector.connect(
        host=database_dict["host"],
        user=database_dict["user"],
        passwd=database_dict["passwd"],
        database=database_dict["database"],
    )
    return mydb


# function to run an sql query and return the results as a pandas dataframe
def run_sql_query(sql_query, database_dict=None, return_df=True):
    # connect to the database
    mydb = connect_to_db(database_dict=database_dict)
    # run the query
    mycursor = mydb.cursor()
    mycursor.execute(sql_query)
    # get the results
    results = mycursor.fetchall()
    # get the column names
    col_names = [desc[0] for desc in mycursor.description]  # type: ignore
    # close the connection
    mydb.close()
    # return the results as a pandas dataframe
    if return_df:
        return pd.DataFrame(results, columns=col_names, dtype=object)
    else:
        return [{col_names[i]: res[i] for i in range(len(res))} for res in results]


sql_query = """
SELECT  
    Educators.first_name, 
    Educators.last_name,
    Classes.id, 
    Classes.name
FROM Classes
JOIN StudentsClasses on StudentsClasses.class_id = Classes.id
JOIN Educators on Educators.id = Classes.educator_id
WHERE StudentsClasses.student_id={student_id}
"""


def get_student_class_info(student_id):
    try:
        out = cast(
            pd.DataFrame,
            run_sql_query(
                sql_query.format(student_id=student_id), database_keys, return_df=True
            ),
        )
        return out
    except Exception as e:
        print(f"Error: {e}")
        return None


# Function to get class information for a list of student IDs
def get_students_classes_info(student_ids: list):
    try:
        # Modify the SQL query to use IN clause for multiple student IDs
        sql_query_multiple = """
        SELECT  
            Educators.first_name, 
            Educators.last_name,
            Classes.id, 
            Classes.name,
            StudentsClasses.student_id
        FROM Classes
        JOIN StudentsClasses on StudentsClasses.class_id = Classes.id
        JOIN Educators on Educators.id = Classes.educator_id
        WHERE StudentsClasses.student_id IN ({student_ids})
        """
        # Format the query with the list of student IDs
        formatted_query = sql_query_multiple.format(
            student_ids=",".join(map(str, student_ids))
        )
        # Run the query and return the results
        out = cast(
            pd.DataFrame, run_sql_query(formatted_query, database_keys, return_df=True)
        )
        return out
    except Exception as e:
        print(f"Error: {e}")
        return None
