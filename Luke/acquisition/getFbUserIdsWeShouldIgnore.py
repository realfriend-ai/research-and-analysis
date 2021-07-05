import pandas as pd


def find_member_in_user_list(fb_user_list, members):
    for member in members:
        if member.get('fbUserId') in fb_user_list:
            return member.get('fbUserId')
    return False


def get_user_we_should_ignore_when_counting(groups_df, fb_users_df):
    """Summary: this function preventing us counting twice user and their groups
     created in the same period

     Parameters:
             groups_df (DataFrame): the beginning date -  we want to find user created after
             fb_users_df (DataFrame): the final date - we want to find users created before

    Returns: list of users we should ignore
     """
    fb_user_list = fb_users_df['fbUserId'].tolist()
    fb_users_df.rename(columns={'createdAt': 'userCreatedAt'}, inplace=True)
    groups_df['memberInUserList'] = groups_df['members'].apply(
        lambda members: find_member_in_user_list(fb_user_list, members))
    groups_df.rename(columns={'createdAt': 'groupCreatedAt', 'fbUserId': 'groupFbUeserId'}, inplace=True)
    duplicates_df = pd.merge(fb_users_df, groups_df, how='inner', left_on='fbUserId', right_on='memberInUserList')
    return duplicates_df['fbUserId'].tolist()

