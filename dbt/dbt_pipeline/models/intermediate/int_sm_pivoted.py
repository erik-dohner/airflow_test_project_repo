import pandas as pd

def model(dbt, session):
    
    # Reference the model --> convert to pandas df
    df = dbt.ref('int_sm_joined').toPandas()
    
    # Pivot the df so that each question has its own column
    pivoted_df = df.pivot(
        index='respondent_id',
        columns='question_text',
        values='answer_text'
        )

    # Reset the pivot tables index to return to regular tabular format
    pivoted_df = pivoted_df.reset_index()
    
    # List of new column names --> need to do here, as bigquery won't accept crazy column names
    new_column_names = [
        'respondent_id',
        'hispanic_origin',
        'recommend_production',
        'recommend_lyric',
        'race',
        'experience_rating',
        'additional_thoughts',
        'negative_comments',
        'positive_comments',
        'age'
    ]

    # Rename columns by index position
    pivoted_df.columns = new_column_names
    
    return pivoted_df