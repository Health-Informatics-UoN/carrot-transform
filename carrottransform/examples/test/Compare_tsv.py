import pandas as pd
import re
import numpy as np
from pandas.testing import assert_frame_equal
### start with person_ids.tsv, because we need to create a lookup - the replacement int ids aren't the same.

def set_person_id_df(transform_person_ids, cdm_person_ids):
    df1 = pd.read_csv(transform_person_ids, sep='\t')
    df2 = pd.read_csv(cdm_person_ids, sep='\t')
    df2[['TARGET_SUBJECT', 'SOURCE_SUBJECT']] = df2[['SOURCE_SUBJECT', 'TARGET_SUBJECT']]
    combined_df = pd.merge(df1, df2, on='SOURCE_SUBJECT')
    ## here, target_subject_x is from df1, i.e., transform ids, and target_subject_y is from cdm

    return combined_df

def get_transform_id(combined_df, CDM_id):
    row = combined_df.loc[combined_df['TARGET_SUBJECT_y'] == CDM_id]
    return row['TARGET_SUBJECT_x'].iloc[0]

def get_CDM_id(combined_df, transform_id):
    row = combined_df.loc[combined_df['TARGET_SUBJECT_x'] == transform_id]
    return row['TARGET_SUBJECT_y'].iloc[0]


def convert_CDM_to_transform_ids(combined_df, cdm_file):
    df = pd.read_csv(cdm_file, sep='\t')
    cdm_ids = df['person_id']
    for i, cdm_id in enumerate(cdm_ids):
        transform_id = get_transform_id(combined_df, cdm_id)
        #df['person_id'].iloc[i] = transform_id
        df.at[df.index[i], 'person_id'] = transform_id
        #df['person_id'] = df['person_id'].replace(cdm_id, transform_id)
        #df.loc[df['person_id'] == cdm_id, 'person_id'] = transform_id
    return df

def remove_datetime(df):
    pattern = ".+datetime"
    for column in df.columns:
        match = re.search(pattern, column)
        if match: ## this is teh column with the datetime in, so now we need to strip out 00:00:00.000000
            records = df[column].unique()
            #for record in df[column]:
            for record in records:
                newRecord = record.split(' ')[0]
                df[column] = df[column].replace(record, newRecord)

                #df.loc[df[column] == record, column] = newRecord

    pass
def remove_zero_columns(df):
    for column in df.columns:
        if (df[column] == 0).all():
            df[column] = df[column].replace(0, np.nan)


def test_compare_tsv_headers(file1, file2, order = False):
    df1 = pd.read_csv(file1, sep='\t')
    df2 = pd.read_csv(file1, sep='\t')

    ### check headers are the same. Since the set is the union of both headers, all possible headers are in it
    if order:
        assert df1 == df2

    set1 = set(df1.columns)
    set2 = set(df2.columns)
    assert set1 == set2

    ### shouldn't need to do this if the sets are equal, but check if not - it'll fail, but we'll know what is missing from which file
    header_set = set(df1.columns).union(set(df2.columns))
    for header in header_set:
        if header not in df1.columns:
            pass
        if header not in df2.columns:
            pass
        assert header in df1.columns
        assert header in df2.columns

    # syntax reminder
    # 'person_id' in df1.columns

def convert_person_ids(transform_person_ids_file, expected_output_person_ids_file, expected_output_file):
    ## convert a file's CDM person_ids to transform person_ids (might be worth doing it as a whole column, rather than one at a time)
    combined_df = set_person_id_df(transform_person_ids_file, expected_output_person_ids_file)
    expected = convert_CDM_to_transform_ids(combined_df, expected_output_file)
    return expected

def test_frames_equal(df1, df2, **kwargs ):
    """ Assert that two dataframes are equal, ignoring ordering of columns"""
    #from pandas.util.testing import assert_frame_equal
    return assert_frame_equal(df1.sort_index(axis=1), df2.sort_index(axis=1), check_names=True, **kwargs )


if __name__ == "__main__":
    #assert test_compare_tsv_headers("test_output/person.tsv", "expected_outputs/person.tsv")

    transform_person_ids_file = "test_output/person_ids.tsv"
    expected_output_person_ids_file =  "expected_outputs/person_ids.tsv"
    transform_output_file = "test_output/measurement.tsv"
    expected_output_file = "expected_outputs/measurement.tsv"

    combined_df = set_person_id_df(transform_person_ids_file, expected_output_person_ids_file)

################## remove datetime from hte df
    ## set the person_ids to the same integer scheme
    expected_df = convert_CDM_to_transform_ids(combined_df, expected_output_file)
    expected_df2 = pd.read_csv(expected_output_file, sep='\t')
    transform_df = pd.read_csv(transform_output_file, sep='\t')

# remove the time portion of datetime columns - note that this will remove all time portions, not just where it is 00:00:00
    remove_datetime(expected_df)
    remove_datetime(transform_df)

# Remove columns that are all 0
    remove_zero_columns(expected_df)
    remove_zero_columns(transform_df)
###############################################

## sort by new id, then compare - should be easiest this way, as every row should be equal.
    expected_df = expected_df.sort_values(by='person_id')
    transform_df = transform_df.sort_values(by='person_id')

    test_frames_equal(expected_df, transform_df)
    pass