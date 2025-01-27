import pandas as pd
import re
import numpy as np
import pytest
from pandas.testing import assert_frame_equal


@pytest.fixture
def file_paths():
    return {
        'transform_person_ids': "carrottransform/examples/test/test_output/person_ids.tsv",
        'expected_person_ids': "carrottransform/examples/test/expected_outputs/person_ids.tsv",
        'transform_output': "carrottransform/examples/test/test_output/condition_occurrence.tsv", 
        'expected_output': "carrottransform/examples/test/expected_outputs/condition_occurrence.tsv"
    }

@pytest.fixture
def combined_df(file_paths):
    df1 = pd.read_csv(file_paths['transform_person_ids'], sep='\t')
    df2 = pd.read_csv(file_paths['expected_person_ids'], sep='\t')
    df2[['TARGET_SUBJECT', 'SOURCE_SUBJECT']] = df2[['SOURCE_SUBJECT', 'TARGET_SUBJECT']]
    combined_df = pd.merge(df1, df2, on='SOURCE_SUBJECT')
    return combined_df

@pytest.fixture
def expected_df(combined_df, file_paths):
    df = pd.read_csv(file_paths['expected_output'], sep='\t')
    cdm_ids = df['person_id']
    for i, cdm_id in enumerate(cdm_ids):
        transform_id = get_transform_id(combined_df, cdm_id)
        df.at[df.index[i], 'person_id'] = transform_id
    return df

@pytest.fixture 
def transform_df(file_paths):
    return pd.read_csv(file_paths['transform_output'], sep='\t')

def get_transform_id(combined_df, CDM_id):
    row = combined_df.loc[combined_df['TARGET_SUBJECT_y'] == CDM_id]
    return row['TARGET_SUBJECT_x'].iloc[0]

def get_CDM_id(combined_df, transform_id):
    row = combined_df.loc[combined_df['TARGET_SUBJECT_x'] == transform_id]
    return row['TARGET_SUBJECT_y'].iloc[0]

def remove_datetime(df):
    pattern = ".+datetime"
    for column in df.columns:
        match = re.search(pattern, column)
        if match:
            records = df[column].unique()
            for record in records:
                newRecord = record.split(' ')[0]
                df[column] = df[column].replace(record, newRecord)

def remove_zero_columns(df):
    for column in df.columns:
        if (df[column] == 0).all():
            df[column] = df[column].replace(0, np.nan)

def test_frames_equal(expected_df, transform_df):
    """Assert that two dataframes are equal, ignoring ordering of columns"""
    # Remove datetime portions and zero columns
    remove_datetime(expected_df)
    remove_datetime(transform_df)
    remove_zero_columns(expected_df)
    remove_zero_columns(transform_df)

    # Drop columns that are all NaN or 0
    expected_df = expected_df.dropna(axis=1, how='all')
    transform_df = transform_df.dropna(axis=1, how='all')
    expected_df = expected_df.loc[:, ~(expected_df == 0).all()]
    transform_df = transform_df.loc[:, ~(transform_df == 0).all()]

    # Sort both dataframes
    expected_df = expected_df.sort_values(by='person_id')
    transform_df = transform_df.sort_values(by='person_id')

    # Sort both dataframes by all columns to ensure consistent ordering
    expected_sorted = expected_df.sort_values(by=list(expected_df.columns)).sort_index(axis=1)
    transform_sorted = transform_df.sort_values(by=list(transform_df.columns)).sort_index(axis=1)
    
    assert_frame_equal(
        expected_sorted,
        transform_sorted,
        check_names=True
    )

def test_compare_tsv_headers(file_paths):
    df1 = pd.read_csv(file_paths['transform_output'], sep='\t')
    df2 = pd.read_csv(file_paths['expected_output'], sep='\t')

    header_set = set(df1.columns).union(set(df2.columns))
    for header in header_set:
        if header not in df1.columns:
            print(header, ' not in ', file_paths['transform_output'])
            if np.isnan(df2[header]).all():
                print ('    but column is all NaN - dropping')
                df2 = df2.drop(columns = header)
                continue
        if header not in df2.columns:
            print(header, ' not in ', file_paths['expected_output'])
            if np.isnan(df1[header]).all():
                print ('    but column is all NaN - dropping')
                df1 = df1.drop(columns = header)
                continue
        assert header in df1.columns
        assert header in df2.columns