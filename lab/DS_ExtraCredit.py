#Used Chat GPT as a Guide and also worked with Ananya Kumar 
import os
import pandas as pd
import numpy as np
import pyarrow

var_list = ['year', 'marital', 'reliten', 'happy', 'age']
output_dir = '/Users/jaithajasti/Downloads/DS 3001'
os.makedirs(output_dir, exist_ok=True)

output_filename = os.path.join(output_dir, 'selected_gss_data.csv')

phase = 0

#1. Data Selection and Description
#For this analysis, I selected five variables from the General Social Survey (GSS) dataset:
#a. Year (year): The survey year allows us to consider potential changes over time, although for this analysis we focus on aggregate trends rather than year-specific effects.
#b. Marital Status (marital): Categorizes respondents as married, never married, divorced, separated, or widowed. This variable is used to explore the relationship between marital status and general happiness.
#c. Religious Intensity (reliten): Measures the strength of religious identification on a scale that includes “no religion,” “not very strong,” “somewhat strong,” and “strong.” This variable helps investigate how religiosity correlates with happiness.
#d. Happiness (happy): Respondents rate their general happiness as “not too happy,” “pretty happy,” or “very happy.” This is the primary dependent variable in the analysis.
#e. Age (age): Respondent’s age, used to control for life-cycle effects and explore trends in happiness across different stages of life.
#These variables were chosen because they allow us to examine how personal and social factors—marital status, religious involvement, and age—relate to general happiness, a topic that is widely studied in social science.

for k in range(3):
    url = (
        'https://github.com/DS3001/project_gss/raw/main/'
        f'gss_chunk_{k+1}.parquet'
    )
    print(f"Loading chunk {k+1}...")
    
    df = pd.read_parquet(url)
    
    # Replace GSS missing codes
    df.replace([-9, -8, -7, -6, -5], np.nan, inplace=True)
    
    # Restrict to adults
    df = df[(df['age'] >= 18) & (df['age'] <= 89)]
    
    if phase == 0:
        df[var_list].to_csv(
            output_filename,
            mode='w',
            header=True,
            index=False
        )
        phase = 1
    else:
        df[var_list].to_csv(
            output_filename,
            mode='a',
            header=False,
            index=False
        )

print("Done! File saved.")
