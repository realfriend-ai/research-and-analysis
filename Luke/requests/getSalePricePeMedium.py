import pandas as pd

from Luke.acquisition.getNumOfUsersCreatedByMediumByPeriod import get_users_created_by_medium_and_date
from constants.mongoConnectLuke import property_requests_collection


def get_max_sale_price(salePrice):
    if type(salePrice) == dict:
        if salePrice.get('to', False):
            return salePrice.get('to')
        elif salePrice.get('from', False):
            return salePrice.get('from')
        else:
            return None
    else:
        return None


def get_median_sale_price(mediumUserDf: pd.DataFrame):
    fbUserList = mediumUserDf['fbUserId'].tolist()
    fbUserList = list(filter(lambda x: pd.isna(x) == False, fbUserList))
    propertyRequestsList = list(property_requests_collection.find({
        'fbUserId': {'$in': fbUserList},
        'category': 'APT_SALE',
        'isActive': True,
        'salePrice': {'$exists': True},
    }, {'salePrice': 1, 'fbUserId': 1, 'createdAt': 1}
    ))
    property_req_df = pd.DataFrame(propertyRequestsList)
    property_req_df['max_salePrice'] = property_req_df['salePrice'].apply(
        lambda x: get_max_sale_price(x))
    property_req_df.dropna(subset=['max_salePrice'], inplace=True)
    print(f'Max sale Price stats: {property_req_df["max_salePrice"].describe()}')
    merged_df = pd.merge(mediumUserDf, property_req_df[['max_salePrice', 'fbUserId']], how='inner',
                         left_on='fbUserId',
                         right_on='fbUserId')
    return merged_df


def get_median_sale_price_per_medium(start, end):
    """Summary: get stats of sale price requested by users created by medium between dates

     Parameters:
             start (date): the beginning date -  we want to find user created after
             end (date): the final date - we want to find users created before

     """
    users_created_by_dates = get_users_created_by_medium_and_date(start, end, only_user_did_pw=True)
    users_created_by_dates.rename(columns={'_id': 'fbUserId'}, inplace=True)
    mediums = ['app', 'imessage', 'phone']
    for medium in mediums:
        medium_users_df = users_created_by_dates[users_created_by_dates['preferredMedium'] == medium]
        print(f'Medium sale price stats: {medium}')
        get_median_sale_price(medium_users_df)


