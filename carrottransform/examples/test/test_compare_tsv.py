import pandas as pd
import re
import numpy as np
import pytest
from pandas.testing import assert_frame_equal


class TestCompareOutput:
    transform_person_ids_file = "carrottransform/examples/test/test_output/person_ids.tsv"
    expected_output_person_ids_file = "carrottransform/examples/test/expected_outputs/person_ids.tsv"
    transform_output_file = "carrottransform/examples/test/test_output/condition_occurrence.tsv"
    expected_output_file = "carrottransform/examples/test/expected_outputs/condition_occurrence.tsv"

    #combined_df = set_person_id_df(transform_person_ids_file, expected_output_person_ids_file)




    ### start with person_ids.tsv, because we need to create a lookup - the replacement int ids aren't the same.

    def set_person_id_df(self, transform_person_ids, cdm_person_ids):
        df1 = pd.read_csv(transform_person_ids, sep='\t')
        df2 = pd.read_csv(cdm_person_ids, sep='\t')
        df2[['TARGET_SUBJECT', 'SOURCE_SUBJECT']] = df2[['SOURCE_SUBJECT', 'TARGET_SUBJECT']]
        combined_df = pd.merge(df1, df2, on='SOURCE_SUBJECT')
        ## here, target_subject_x is from df1, i.e., transform ids, and target_subject_y is from cdm

        return combined_df

    def get_transform_id(self, combined_df, CDM_id):
        row = combined_df.loc[combined_df['TARGET_SUBJECT_y'] == CDM_id]
        return row['TARGET_SUBJECT_x'].iloc[0]

    def get_CDM_id(self, combined_df, transform_id):
        row = combined_df.loc[combined_df['TARGET_SUBJECT_x'] == transform_id]
        return row['TARGET_SUBJECT_y'].iloc[0]


    def convert_CDM_to_transform_ids(self, combined_df, cdm_file):
        df = pd.read_csv(cdm_file, sep='\t')
        cdm_ids = df['person_id']
        for i, cdm_id in enumerate(cdm_ids):
            transform_id = self.get_transform_id(combined_df, cdm_id)
            #df['person_id'].iloc[i] = transform_id
            df.at[df.index[i], 'person_id'] = transform_id
            #df['person_id'] = df['person_id'].replace(cdm_id, transform_id)
            #df.loc[df['person_id'] == cdm_id, 'person_id'] = transform_id
        return df

    def remove_datetime(self, df):
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
    def remove_zero_columns(self, df):
        for column in df.columns:
            if (df[column] == 0).all():
                df[column] = df[column].replace(0, np.nan)




    def convert_person_ids(self, transform_person_ids_file, expected_output_person_ids_file, expected_output_file):
        ## convert a file's CDM person_ids to transform person_ids (might be worth doing it as a whole column, rather than one at a time)
        combined_df = self.set_person_id_df(transform_person_ids_file, expected_output_person_ids_file)
        expected = self.convert_CDM_to_transform_ids(combined_df, expected_output_file)
        return expected

    def align_person_ids(self):
        ## set the person_ids to the same integer scheme
        self.expected_df = self.convert_CDM_to_transform_ids(self.combined_df, self.expected_output_file)
        self.expected_df2 = pd.read_csv(self.expected_output_file, sep='\t')
        self.transform_df = pd.read_csv(self.transform_output_file, sep='\t')


    def remove_zeros(self):


        # remove the time portion of datetime columns - note that this will remove all time portions, not just where it is 00:00:00
        self.remove_datetime(self.expected_df)
        self.remove_datetime(self.transform_df)

        # Remove columns that are all 0
        self.remove_zero_columns(self.expected_df)
        self.remove_zero_columns(self.transform_df)

    ###############################################
    def sort_dfs(self):
        ## sort by new id, then compare - should be easiest this way, as every row should be equal.
        self.expected_df = self.expected_df.sort_values(by='person_id')
        self.transform_df = self.transform_df.sort_values(by='person_id')


    # def test_frames_equal(self, df1, df2, **kwargs ):
    #     """ Assert that two dataframes are equal, ignoring ordering of columns"""
    #     #from pandas.util.testing import assert_frame_equal
    #     return assert_frame_equal(df1.sort_index(axis=1), df2.sort_index(axis=1), check_names=True, **kwargs )
    #
    def test_frames_equal(self):
        """ Assert that two dataframes are equal, ignoring ordering of columns"""
        #from pandas.util.testing import assert_frame_equal

        import os
        print (os.getcwd())
        self.combined_df = self.set_person_id_df(self.transform_person_ids_file, self.expected_output_person_ids_file)
        self.align_person_ids()
        self.remove_zeros()

        return assert_frame_equal(self.expected_df.sort_index(axis=1), self.transform_df.sort_index(axis=1), check_names=True )

    def test_compare_tsv_headers(self):
        file1 = self.transform_output_file
        file2 = self.expected_output_file

        import os
        print (os.getcwd())

        df1 = pd.read_csv(file1, sep='\t')
        df2 = pd.read_csv(file2, sep='\t')

        ### check headers are the same. Since the set is the union of both headers, all possible headers are in it

        set1 = set(df1.columns)
        set2 = set(df2.columns)
        #assert set1 == set2

        ### shouldn't need to do this if the sets are equal, but check if not - it'll fail, but we'll know what is missing from which file
        header_set = set(df1.columns).union(set(df2.columns))
        for header in header_set:
            if header not in df1.columns:
                print(header, ' not in ', file1)
                if np.isnan(df2[header]).all():
                    print ('    but column is all NaN - dropping')
                    df2 = df2.drop(columns = header)
                    continue
            if header not in df2.columns:
                print(header, ' not in ', file2)
                if np.isnan(df1[header]).all():
                    print ('    but column is all NaN - dropping')
                    df1 = df1.drop(columns = header)
                    continue
            #if assertion:
            assert header in df1.columns
            assert header in df2.columns
        return df1, df2
        # syntax reminder
        # 'person_id' in df1.columns



if __name__ == "__main__":


## sort by new id, then compare - should be easiest this way, as every row should be equal.
#     expected_df = expected_df.sort_values(by='person_id')
#     transform_df = transform_df.sort_values(by='person_id')
#
#     print (expected_output_file, expected_df.shape)
#     print (transform_output_file, transform_df.shape)
#
#
# ### need to implement the preprocessing of this to drop nan columns
#     test_compare_tsv_headers(transform_output_file, expected_output_file, assertion=True)

    # test_frames_equal(df1, df2)

    tests = TestCompareOutput()
    tests.test_frames_equal()
    tests.test_compare_tsv_headers()

    pass