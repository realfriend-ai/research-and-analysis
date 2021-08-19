import pandas as pd

from constants.importantDates import last_month
from constants.mongoConnectDooron import duplicate_props_collection
from dooron.duplicates.currentState import get_property_kind


def get_duplicates_df(is_manual):
    query = {
        'similarityScore': {'$exists': True},
        'createdAt': {'$gte': last_month}
    }
    if is_manual:
        query.update({'state': {'$ne': 'PENDING'},
                      'kind': 'MANUAL'})
    else:
        query.update({'kind': 'AUTO', 'similarityScore': {'$gte': 0.07}})
    fields_needed = {'similarityScore': 1, 'state': 1, 'candidatesTuple': 1,
                     'kind': 1}
    list1 = list(duplicate_props_collection.find(query, fields_needed).limit(5000))
    duplicateDf = pd.DataFrame(list1)
    duplicateDf['propertyOne'] = duplicateDf['candidatesTuple'].apply(lambda x: x[0])
    duplicateDf['propertyOneLink'] = duplicateDf['propertyOne'].apply(
        lambda x: f'https://adooron.realfriend.ai/bot/property/{x}')
    duplicateDf['propertyTwo'] = duplicateDf['candidatesTuple'].apply(lambda x: x[1])
    duplicateDf['propertyTwoLink'] = duplicateDf['propertyTwo'].apply(
        lambda x: f'https://adooron.realfriend.ai/bot/property/{x}')
    return duplicateDf


def shuffle_and_save(df, path_name):
    df['wightsForShuffle'] = df['similarityScore'].apply(lambda x: 2 if x > 0.3 else 1)
    df_shuffled = df.sample(n=1200, weights='wightsForShuffle')
    df_shuffled.sort_values(by='similarityScore', inplace=True, ignore_index=True)
    df_shuffled.to_csv(f'{path_name}.csv')


def main_func():
    dup_df_man = get_duplicates_df(is_manual=True)
    dup_df_auto = get_duplicates_df(is_manual=False)
    dup_df = pd.concat([dup_df_man, dup_df_auto], ignore_index=True)
    property_list = dup_df['propertyOne'].tolist()
    prop_df = get_property_kind(property_list)
    merged_df = pd.merge(dup_df, prop_df, how='inner', left_on='propertyOne', right_on='propertyOne')
    duplicates_rent_df = merged_df[merged_df['category'] == 'APT_RENT']
    shuffle_and_save(duplicates_rent_df, 'APT_RENT_MAN_CHECK')
    duplicates_sale_df = merged_df[merged_df['category'] == 'APT_SALE']
    shuffle_and_save(duplicates_sale_df, 'APT_SALE_MAN_CHECK')
    return duplicates_rent_df, duplicates_sale_df


main_func()
