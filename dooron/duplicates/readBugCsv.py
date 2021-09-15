import pandas as pd
from bson import ObjectId

from constants.mongoConnectDooron import duplicate_props_collection


def get_property_id_from_link(property_link):
    if type(property_link) == str:
        foundProperty = property_link.find('property/')
        foundEdit = property_link.find('edit/')
        if (foundProperty < 0) & (foundEdit < 0):
            return None
        elif foundProperty > -1:
            start_loc = foundProperty + len('property/')
        elif foundEdit > -1:
            start_loc = foundEdit + len('/edit')
        end_loc = start_loc + 24
        property_id = property_link[start_loc: end_loc]
        return ObjectId(property_id)
    return None


def get_duplicates_from_db(duplicates_reported_list):
    list1 = list(duplicate_props_collection.find({
        'candidatesTuple': {'$in': duplicates_reported_list},
    }, {'similarityScore': 1, 'state': 1, 'candidatesTuple': 1, 'kind': 1}))
    duplicateDf = pd.DataFrame(list1)
    duplicateDf['candidatesTuple'] = duplicateDf['candidatesTuple'].apply(lambda x: list(x))
    return duplicateDf


def is_candidates_were_in_db(dup_df_db, dup_df_row):
    first_combination = [dup_df_row['propertyOne'], dup_df_row['propertyTwo']]
    second_combination = [dup_df_row['propertyTwo'], dup_df_row['propertyOne']]
    dup_df_db['combinationFound'] = dup_df_db['candidatesTuple'].apply(
        lambda x: True if (x == first_combination) | (x == second_combination) else False)
    dup_df_db_combination = dup_df_db[dup_df_db['combinationFound']]
    if len(dup_df_db_combination.index) > 0:
        return True
    else:
        return False


dup_df_reported = pd.read_csv('/Users/asaflev/Downloads/Bug Tracker - Dooron - Bugs Reports.csv')
dup_df_reported = dup_df_reported[dup_df_reported['Bug Type'] == 'Duplicate Property']
dup_df_reported['propertyOne'] = dup_df_reported['Link'].apply(lambda x: get_property_id_from_link(x))
dup_df_reported['propertyTwo'] = dup_df_reported['Link 2'].apply(lambda x: get_property_id_from_link(x))
dup_df_reported.dropna(subset=['propertyOne'], inplace=True)
dup_df_reported.dropna(subset=['propertyTwo'], inplace=True)
dup_df_db = get_duplicates_from_db(dup_df_reported['propertyOne'].tolist())
dup_df_reported['isCandidatesWereInDb'] = dup_df_reported.apply(lambda row: is_candidates_were_in_db(dup_df_db, row),
                                                                axis=1)
