import pandas as pd

from constants.importantDates import last_month
from constants.mongoConnectDooron import duplicate_props_collection, properties_collection_dooron


def get_duplicates_df():
    list1 = list(duplicate_props_collection.find({
        'similarityScore': {'$lte': 0.75, '$gte': 0.45},
        'state': {'$ne': 'PENDING'},
        'kind': {'$ne': 'RULE'},
        'createdAt': {'$gte': last_month}
    }, {'similarityScore': 1, 'state': 1, 'candidatesTuple': 1, 'kind': 1}))
    duplicateDf = pd.DataFrame(list1)
    duplicateDf['propertyOne'] = duplicateDf['candidatesTuple'].apply(lambda x: x[0])
    duplicateDf['propertyOneLink'] = duplicateDf['propertyOne'].apply(
        lambda x: f'https://adooron.realfriend.ai/bot/property/{x}')
    duplicateDf['propertyTwo'] = duplicateDf['candidatesTuple'].apply(lambda x: x[0])
    duplicateDf['propertyTwoLink'] = duplicateDf['propertyTwo'].apply(
        lambda x: f'https://adooron.realfriend.ai/bot/property/{x}')
    return duplicateDf


def get_property_kind(property_list_ids):
    list1 = list(properties_collection_dooron.find({
        '_id': {'$in': property_list_ids},
    }, {'category': 1}))
    property_df = pd.DataFrame(list1)
    property_df.rename(columns={'_id': 'propertyOne'}, inplace=True)
    return property_df


def get_metrics_per_score(score: float, duplication_df):
    positive_values_num = duplication_df['state'].value_counts()['YES']
    duplication_df_score = duplication_df[duplication_df['similarityScore'] > score]
    val_counts_real = dict(duplication_df_score['state'].value_counts())
    positive_values_num_in_score = val_counts_real.get('YES', 0)
    val_counts_pred = dict(duplication_df_score['state'].value_counts())
    chosen_by_model = val_counts_pred.get('YES', 0)
    tp = duplication_df_score[(duplication_df_score['state'] == 'YES')]
    tpNum = len(tp.index)
    fp = duplication_df_score[(duplication_df_score['state'] == 'NO')]
    fpNum = len(fp.index)
    percent_of_total = len(tp.index) / positive_values_num
    return {f'score': score, 'totalPositives': positive_values_num, 'canChooseInScore': positive_values_num_in_score,
            'numOfChosen': chosen_by_model, 'correctChosenByModel': len(tp.index),
            'wronglyChosenByModel': len(fp.index), 'chosenOutOfOptional': percent_of_total,
            'precision': tpNum / (tpNum + fpNum),
            }


dup_df = get_duplicates_df()
property_list = dup_df['propertyOne'].tolist()
prop_df = get_property_kind(property_list)
print(f'NUM OF DUPLICATIONS PER DAY {len(prop_df.index) / 30}')
merged_df = pd.merge(dup_df, prop_df, how='inner', left_on='propertyOne', right_on='propertyOne')
print(len(merged_df.index))
merged_df_sale = merged_df[merged_df['category'] == 'APT_SALE']
merged_df_rent = merged_df[merged_df['category'] == 'APT_RENT']
merged_df_rent.to_csv('duplicates_rent.csv')
merged_df_sale.to_csv('duplicates_sale.csv')

# merged_df_rent_for_man = merged_df_rent[
#     (merged_df_rent['similarityScore'] > 0.65) & (merged_df_rent['similarityScore'] < 0.89)]
# print(len(merged_df_rent_for_man.index))
# print(len(merged_df_sale.index))
#
# # merged_df_rent_for_man.to_clipboard()
# # final_df_list_sale = []
# final_df_list_rent = []
# for score in [0.45, 0.47, 0.5, 0.55, 0.6, .65, 0.7, .75, .8, .85, 0.86, 0.87, 0.88, 0.885, .886, 0.887, 0.888, 0.89, .9,
#               .91,
#               .92, .95]:
#     # final_df_list_sale.append(get_metrics_per_score(score, merged_df_sale))
#     final_df_list_rent.append(get_metrics_per_score(score, merged_df_rent))
# # final_df = pd.DataFrame(final_df_list_sale)
# final_df_rent = pd.DataFrame(final_df_list_rent)
# # # def get_property_from_property_list():


# def get_duplicates_white_list_for_sale():
#     list1 = list(duplicate_props_collection.find({
#         'state': 'NO',
#         'kind': 'AUTO',
#         'similarityScore': {'$lte': 0.05},
#         'createdAt': {'$gte': last_month}
#     }, {'similarityScore': 1, 'state': 1, 'candidatesTuple': 1}).limit(10000))
#     duplicateDf = pd.DataFrame(list1)
#     duplicateDf['propertyOne'] = duplicateDf['candidatesTuple'].apply(lambda x: x[0])
#     duplicateDf['propertyOneLink'] = duplicateDf['propertyOne'].apply(lambda x: f'https://adooron.realfriend.ai/bot/property/{x}')
#     duplicateDf['propertyTwo'] = duplicateDf['candidatesTuple'].apply(lambda x: x[0])
#     duplicateDf['propertyTwoLink'] = duplicateDf['propertyTwo'].apply(lambda x: f'https://adooron.realfriend.ai/bot/property/{x}')
#     return duplicateDf
#
#
# auto_white = get_duplicates_white_list_for_sale()
# property_list = auto_white['propertyOne'].tolist()
# prop_df = get_property_kind(property_list)
# merged_df = pd.merge(auto_white, prop_df, how='inner', left_on='propertyOne', right_on='propertyOne')
# merged_df_sale = merged_df[merged_df['category'] == 'APT_SALE']
# merged_df_sale.sort_values(by='similarityScore', ascending=False, inplace=True, ignore_index=True)
