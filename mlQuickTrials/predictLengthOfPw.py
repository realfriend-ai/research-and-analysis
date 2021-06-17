from datetime import datetime

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from bson import ObjectId
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, confusion_matrix, precision_score, recall_score

from constants.lukeFbUserIds import luke_fb_user_ids
from constants.mongoConnectLuke import fb_users_collection, property_collection, page_view_collection, \
    property_requests_collection, feed_collection


def get_buyer_list():
    query = {
        'isAgent': {'$ne': True},
        'tags': {'$ne': 'ADMIN'},
        'requestsKind': 'APT_SALE',
        '_id': {'$nin': luke_fb_user_ids},
        'lastPageViewAt': {'$exists': True}
    }
    projection = {'_id': 1}
    buyer_list = list(fb_users_collection.find(query, projection))
    return list(map(lambda x: x.get('_id'), buyer_list))


def get_user_pws_length(buyer_list):
    pageViewsList = list(page_view_collection.find(
        {
            'pageName': 'PROPERTY_MORE_DETAILS',
            'duration': {'$exists': True},
            'adminUserId': {'$exists': False},
            'fbUserId': {'$in': buyer_list},
            'params': {'$exists': True},
        }, {'createdAt': 1, 'fbUserId': 1, 'duration': 1, 'params': 1}
    ))
    pageViewsDf = pd.DataFrame(pageViewsList)
    print(f'Length of pws df {len(pageViewsDf.index)}')
    pageViewsDf['propertyId'] = pageViewsDf['params'].apply(lambda x: ObjectId(x.get('propertyId')))
    pageViewsDf['duration'] = pageViewsDf['duration'].apply(lambda x: x if x <= 180 else 120)
    pageViewsDf.rename(columns={'createdAt': 'pageViewCreatedAt'}, inplace=True)
    pageViewsDf.dropna(subset=['duration'], inplace=True)
    return pageViewsDf


def get_property_id_from_propertyId_list(property_id_list):
    if type(property_id_list) == list:
        if len(property_id_list) > 0:
            return property_id_list[0]
    return None


def get_feed_to_connect_page_views_with_property_request(buyer_list):
    start = datetime.now()
    feedList = list(feed_collection.find(
        {
            'fbUserId': {'$in': buyer_list},
            'propertyRequestIds': {'$exists': True}
        }, {'propertyId': 1, 'propertyRequestIds': 1, 'fbUserId': 1}
    ))
    feedDf = pd.DataFrame(feedList)
    print(f'Time for getting feed data {datetime.now() - start}')
    feedDf['propertyRequestId'] = feedDf['propertyRequestIds'].apply(
        lambda x: get_property_id_from_propertyId_list(x))
    feedDf.dropna(subset=['propertyRequestId'], inplace=True)
    return feedDf


def get_max_from_from_to_var(from_to_var):
    if pd.isna(from_to_var):
        return None
    var_to = from_to_var.get('to', False)
    if var_to:
        return var_to
    var_from = from_to_var.get('from', False)
    if var_from:
        return var_from
    else:
        return None


def get_user_request_details(buyer_list):
    userReqDfList = list(property_requests_collection.find(
        {
            'fbUserId': {'$in': buyer_list},
            'category': 'APT_SALE'
        }, {'createdAt': 1, 'fbUserId': 1, 'salePrice': 1, 'numOfBeds': 1}
    ))
    userReqDf = pd.DataFrame(userReqDfList)
    userReqDf['maxSalePrice'] = userReqDf['salePrice'].apply(lambda x: get_max_from_from_to_var(x))
    userReqDf.drop(columns='salePrice', inplace=True)
    userReqDf['max_req_beds'] = userReqDf['numOfBeds'].apply(lambda x: get_max_from_from_to_var(x))
    userReqDf.drop(columns='numOfBeds', inplace=True)
    userReqDf.rename(columns={'_id': 'propertyRequestId'}, inplace=True)
    return userReqDf


def change_yes_no_vars_to_dummy(y_n_var):
    if y_n_var == 'YES':
        return 1
    elif y_n_var == 'NO':
        return -1
    else:
        return 0


def get_features_of_property(property_df):
    property_df['isRenovated'] = property_df['isRenovated'].apply(lambda x: change_yes_no_vars_to_dummy(x))
    property_df['hasNaturalLight'] = property_df['hasNaturalLight'].apply(
        lambda x: change_yes_no_vars_to_dummy(x))
    property_df['ourRank'] = property_df['ourRank'].fillna(value=3)
    property_df['isOpenKitchen'] = property_df['kitchen'].apply(
        lambda x: change_yes_no_vars_to_dummy(x.get('isOpenKitchen')))
    property_df['isRenovatedKitchen'] = property_df['kitchen'].apply(
        lambda x: change_yes_no_vars_to_dummy(x.get('isRenovated')))
    property_df['hasBathtub'] = property_df['bathroom'].apply(
        lambda x: change_yes_no_vars_to_dummy(x.get('hasBathtub')))
    property_df['isBathroomRenovated'] = property_df['bathroom'].apply(
        lambda x: change_yes_no_vars_to_dummy(x.get('isBathroomRenovated')))
    property_df['contentLength'] = property_df['content'].apply(lambda x: len(x))
    return property_df


def get_property_details_df(property_ids):
    propertyList = list(property_collection.find(
        {
            '_id': {'$in': property_ids},
            'category': 'APT_SALE'
        }, {'createdAt': 1, 'ourRank': 1, 'salePrice': 1, 'isRenovated': 1, 'hasNaturalLight': 1, 'numOfBeds': 1,
            'content': 1, 'kitchen': 1, 'bathroom': 1}
    ))
    propertyDetailsDf = pd.DataFrame(propertyList)
    propertyDetailsDf = get_features_of_property(propertyDetailsDf)
    propertyDetailsDf.rename(columns={'_id': 'propertyId', 'createdAt': 'propertyCreatedAt'}, inplace=True)
    return propertyDetailsDf


def get_time_in_market(merged_df):
    merged_df['timeOnMarket'] = merged_df.apply(lambda row: row['pageViewCreatedAt'] - row['propertyCreatedAt'], axis=1)
    merged_df['timeOnMarketInDays'] = merged_df['timeOnMarket'].apply(lambda x: x.days)
    return merged_df


def _get_data_set():
    users_list = get_buyer_list()
    pw_df = get_user_pws_length(users_list)
    feed_df = get_feed_to_connect_page_views_with_property_request(users_list)
    user_requests = get_user_request_details(users_list)
    property_df = get_property_details_df(pw_df['propertyId'].tolist())
    pw_feed_merged = pd.merge(feed_df, pw_df, how='inner', left_on=['propertyId', 'fbUserId'],
                              right_on=['propertyId', 'fbUserId'])
    pw_feed_property_merged = pd.merge(pw_feed_merged, property_df, how='inner', left_on='propertyId',
                                       right_on='propertyId')
    merged_df = pd.merge(pw_feed_property_merged, user_requests, how='inner', left_on='propertyRequestId',
                         right_on='propertyRequestId')
    merged_df['salePriceRatio'] = merged_df['maxSalePrice'] / merged_df['salePrice']
    merged_df['bedsRatio'] = merged_df['numOfBeds'] / merged_df['max_req_beds']
    return merged_df


def split_to_train_and_test(data):
    shuffle_df = data.sample(frac=1)
    train_size = int(0.9 * len(data))
    train_set = shuffle_df[:train_size]
    test_set = shuffle_df[train_size:]
    return train_set, test_set


def train_model(df, threshold):
    train_df, test_df = split_to_train_and_test(df)
    X_train = train_df.drop(columns=['duration'])
    y_train = train_df['duration']
    X_test = test_df.drop(columns=['duration'])
    clf = RandomForestClassifier(max_depth=15, n_estimators=200)
    clf.fit(X_train, y_train)
    test_df['predict'] = None
    y_pred = clf.predict_proba(X_test)
    y_pred = (y_pred[:, 1] >= threshold).astype('int')
    labels = [0, 1]
    fig = plt.figure()
    ax = fig.add_subplot(111)
    conf_mat = confusion_matrix(test_df['duration'], y_pred, labels)
    sns.heatmap(conf_mat, annot=True, ax=ax, fmt='g')
    # labels, title and ticks
    ax.set_xlabel('Predicted labels')
    ax.set_ylabel('True labels')
    ax.set_title('Confusion Matrix')
    ax.xaxis.set_ticklabels(['0', '1'])
    ax.yaxis.set_ticklabels(['0', '1'])
    plt.show()
    acc = accuracy_score(test_df['duration'], y_pred)
    precision = precision_score(test_df['duration'], y_pred)
    recall = recall_score(test_df['duration'], y_pred)
    print("Accuracy:", acc)
    print("Precision:", precision)
    print("Recall:", recall)
    importances = clf.feature_importances_
    indices = np.argsort(importances)[::-1]
    # Rearrange feature names so they match the sorted feature importances
    names = [X_train.columns[i] for i in indices]
    # Create plot
    plt.figure()
    # Create plot title
    plt.title("Feature Importance")
    # Add bars
    plt.bar(range(X_train.shape[1]), importances[indices])
    plt.gcf().set_size_inches(20, 8)
    # Add feature names as x-axis labels
    plt.xticks(range(X_train.shape[1]), names, rotation=90)
    # Show plot
    plt.show()
    return {'score': threshold, 'acc': acc, 'precision': precision, 'recall': recall}


def main_func():
    df = _get_data_set()
    df = get_time_in_market(df)
    df = df[['isRenovatedKitchen', 'isOpenKitchen', 'hasBathtub', 'isBathroomRenovated', 'salePriceRatio', 'bedsRatio',
             'ourRank', 'isRenovated', 'hasNaturalLight', 'duration', 'contentLength', 'timeOnMarketInDays']]
    df.describe(percentiles=[.1, .2, .3, .4, .5, .6, .7, .8, .9])
    df.to_pickle('./df.pkl')
    df.dropna(inplace=True)
    df['duration'] = df['duration'].apply(lambda x: 0 if x <= 40 else 1)
    results_df = []
    for score in [0.55, 0.60, 0.65, 0.7, 0.75]:
        res = train_model(df, score)
        results_df.append(res)
    results_df = pd.DataFrame(results_df)
    return results_df


main_func()
# todo:  1. first picture room, 2. private outdoor space, 3.beat his average or not
