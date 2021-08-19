import pandas as pd
from bson import ObjectId

from Luke.acquisition.getFbUserIdsWeShouldIgnore import get_user_we_should_ignore_when_counting
from Luke.acquisition.getGroupesToIgnore import find_groups_with_user_created_between_date_and_user_member_before, \
    groups_created_between_dates
from constants.lukeFbUserIds import luke_fb_user_ids, bug_users_ids
from constants.mongoConnectLuke import fb_users_collection, lead_collection


def get_preferred_medium_by_user_cleaning_groups(mediums):
    preferred = mediums.get('preferredMedium')
    if preferred == 'group':
        return mediums.get('group').get('preferredMedium')
    else:
        return preferred


def remove_fb_user_we_should_ignore_on_counting(start, end, fbUserDf):
    groups_df = groups_created_between_dates(start, end)
    user_ids_we_should_ignore_in_counting = get_user_we_should_ignore_when_counting(groups_df, fbUserDf)
    fbUserDf['isIgnoreUser'] = fbUserDf['fbUserId'].apply(
        lambda x: True if x in user_ids_we_should_ignore_in_counting else False)
    fbUserDf = fbUserDf[fbUserDf['isIgnoreUser'] == False]
    return fbUserDf


def get_only_user_who_sent_lead(fbUserDf):
    user_ids = fbUserDf['fbUserId'].tolist()
    user_ids_str = list(map(lambda x: str(x), user_ids))
    query = {
        'fbUserId': {'$in': user_ids_str},
    }
    fbUserSentLead = list(lead_collection.find(query, {'fbUserId': 1}))
    fbUserIdsSentLead = list(map(lambda x: ObjectId(x.get('fbUserId')), fbUserSentLead))
    fbUserDf['shouldKeepUser'] = fbUserDf['fbUserId'].apply(lambda x: True if x in fbUserIdsSentLead else False)
    fbUserDf = fbUserDf[fbUserDf['shouldKeepUser']]
    return fbUserDf


def get_users_created_by_medium_and_date(start, end, medium, only_user_did_pw, only_user_sent_lead, for_action_insights):
    """Summary: get users created by medium between dates

     Parameters:
             start (date): the beginning date -  we want to find user created after
             end (date): the final date - we want to find users created before
             medium (str): subsetting by specific medium we are looking for in line 71
             only_user_did_pw(bool) - for checking only user who did pw
             for_action_insights(bool): if its for action insights we wont want to delete the duplicates of user and groups but we'll merge them after

     """
    groups_ids_with_member_created_before_start_date = find_groups_with_user_created_between_date_and_user_member_before(start, end)
    query = {
        'isAgent': {'$ne': True},
        'tags': {'$ne': 'ADMIN'},
        'requestsKind': 'APT_SALE',
        '_id': {'$nin': luke_fb_user_ids + groups_ids_with_member_created_before_start_date + bug_users_ids},
        'createdAt': {'$gte': start, '$lte': end},
    }
    projection = {'_id': 1, 'createdAt': 1, 'mediums': 1, 'requestsKind': 1, 'initialRequest': 1, 'tags': 1}
    if only_user_did_pw:
        query.update({'lastPageViewAt': {'$exists': True}})
    fbUsersList = list(fb_users_collection.find(query, projection))
    fbUserDf = pd.DataFrame(fbUsersList)
    fbUserDf.dropna(subset=['mediums'], inplace=True)
    fbUserDf.rename(columns={'_id': 'fbUserId'}, inplace=True)
    if for_action_insights is not True:
        fbUserDf = remove_fb_user_we_should_ignore_on_counting(start, end, fbUserDf)
    fbUserDf['preferredMedium'] = fbUserDf['mediums'].apply(lambda x: get_preferred_medium_by_user_cleaning_groups(x))
    print(fbUserDf['preferredMedium'].value_counts())
    fbUserDf = fbUserDf[fbUserDf['preferredMedium'] == medium]
    if only_user_sent_lead:
        fbUserDf = get_only_user_who_sent_lead(fbUserDf)
    return fbUserDf.reset_index()
