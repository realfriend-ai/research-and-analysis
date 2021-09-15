import pandas as pd

from constants.importantDates import last_month, last_day
from constants.mongoConnectDooron import duplicate_props_collection, properties_collection_dooron


def get_duplicates_df(is_manual, is_rule, state):
    query = {
        'similarityScore': {'$exists': True},
        'createdAt': {'$gte': last_day},
        'state': state
    }
    if is_manual:
        query.update({
            'kind': 'MANUAL'})
    if is_rule:
        query.update({
            'kind': 'RULE'})
    else:
        query.update({'kind': 'AUTO', 'similarityScore': {'$gte': 0.07}})
    fields_needed = {'similarityScore': 1, 'state': 1, 'candidatesTuple': 1, 'createdAt': 1,
                     'kind': 1}
    list1 = list(duplicate_props_collection.find(query, fields_needed).limit(10000))
    duplicateDf = pd.DataFrame(list1)
    duplicateDf['propertyOne'] = duplicateDf['candidatesTuple'].apply(lambda x: x[0])
    duplicateDf['propertyOneLink'] = duplicateDf['propertyOne'].apply(
        lambda x: f'https://adooron.realfriend.ai/bot/property/{x}')
    duplicateDf['propertyTwo'] = duplicateDf['candidatesTuple'].apply(lambda x: x[1])
    duplicateDf['propertyTwoLink'] = duplicateDf['propertyTwo'].apply(
        lambda x: f'https://adooron.realfriend.ai/bot/property/{x}')
    return duplicateDf


def get_price_by_category(row):
    if row['category'] == 'APT_SALE':
        if type(row['saleData']) == dict:
            return row['saleData'].get('APT_SALE', 0)
    else:
        return row['rentPrice']


def get_property_features_for_rule_check(property_list_ids):
    list1 = list(properties_collection_dooron.find({
        '_id': {'$in': property_list_ids},
    }, {'content': 1, 'rentPrice': 1, 'saleData': 1, 'numOfRooms': 1, 'category': 1}))
    property_df = pd.DataFrame(list1)
    property_df.dropna(subset=['content'], inplace=True)
    property_df['contentLength'] = property_df['content'].apply(lambda x: len(x.split()))
    property_df['price'] = property_df.apply(lambda row: get_price_by_category(row), axis=1)
    return property_df


def get_property_features_from_property_df(row, properties_df, propertyNum):
    property_id = row[propertyNum]
    index_of_property = properties_df.index[properties_df['_id'] == property_id].tolist()
    columnsNeeded = ['_id', 'price', 'numOfRooms', 'content', 'contentLength']
    result = {}
    if len(index_of_property) > 0:
        for col in columnsNeeded:
            result.update({f'{propertyNum}_{col}': properties_df.at[index_of_property[0], col]})
    else:
        for col in columnsNeeded:
            result.update({f'{propertyNum}_{col}': None})
    return result


def transform_series_to_df(series_input):
    list_input = series_input.to_list()
    return pd.DataFrame(list_input)


def get_duplicates_for_man_rule_check(is_manual, is_rule, state):
    dup_df = get_duplicates_df(is_manual, is_rule, state)
    print(f'STATE: {state}, NUM OF DUPLICATES GOT FROM QUERY: {len(dup_df.index)}')
    property_list = dup_df['propertyOne'].tolist() + dup_df['propertyTwo'].tolist()
    property_df = get_property_features_for_rule_check(property_list)
    firstPropertyFeatures = dup_df.apply(
        lambda row: get_property_features_from_property_df(row, property_df, 'propertyOne'), axis=1)
    firstPropertyFeatures = transform_series_to_df(firstPropertyFeatures)
    secondPropertyFeatures = dup_df.apply(
        lambda row: get_property_features_from_property_df(row, property_df, 'propertyTwo'), axis=1)
    secondPropertyFeatures = transform_series_to_df(secondPropertyFeatures)
    merged_df = pd.merge(dup_df, firstPropertyFeatures, how='inner', left_on='propertyOne', right_on='propertyOne__id')
    merged_df = pd.merge(merged_df, secondPropertyFeatures, how='inner', left_on='propertyTwo',
                         right_on='propertyTwo__id')
    sameContent = merged_df['propertyOne_content'] == merged_df['propertyTwo_content']
    sameNumOfRooms = merged_df['propertyOne_numOfRooms'] == merged_df['propertyTwo_numOfRooms']
    samePrice = merged_df['propertyOne_price'] == merged_df['propertyTwo_price']
    conditions = sameContent & sameNumOfRooms & samePrice
    result = merged_df[conditions]
    print(f'STATE: {state}, NUM OF DUPLICATES AFTER CONDITIONS: {len(result.index)}')
    return result


rule_df_yes = get_duplicates_for_man_rule_check(is_manual=True, is_rule=False, state='YES')

# for i in range(1, 100, 3):
#     man_df_temp = rule_df_yes[rule_df_yes['propertyOne_contentLength'] <= i]
#     print(f'Number: {i}')
#     print(man_df_temp['state'].value_counts(normalize=True))
#     print(man_df_temp['state'].value_counts(normalize=False))
