from Luke.acquisition.getNumOfUsersCreatedByMediumByPeriod import get_users_created_by_medium_and_date
from constants.importantDates import first_of_jun_21, first_of_july_21
from mongoConnect import fb_users_collection


def get_app_users_created_between_dates_and_approved_notifications(start, end):
    app_users_df = get_users_created_by_medium_and_date(start=start, end=end, medium='app',
                                                        for_action=False, only_user_sent_lead=False,
                                                        only_user_did_pw=False)
    fb_users_ids = app_users_df['fbUserId'].tolist()
    query = {
        '_id': {'$in': fb_users_ids},
        'mediums': {
            '$exists': True
        }
    }
    projection = {'_id': 1, 'mediums': 1}
    fbUsersList = list(fb_users_collection.find(query, projection))
    print(f'Num of user created and approved token is: {len(fbUsersList)}')
    return fb_users_ids


app_user_list = get_app_users_created_between_dates_and_approved_notifications(start=first_of_jun_21,
                                                                               end=first_of_july_21)
