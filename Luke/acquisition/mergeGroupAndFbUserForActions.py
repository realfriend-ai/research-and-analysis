def map_group_id_id_to_his_origin_fb_user_id(fbUserId, groups_df):
    group_fb_user_ids = groups_df['fbUserId'].tolist()
    if fbUserId in group_fb_user_ids:
        indexFound = group_fb_user_ids.index(fbUserId)
        members = groups_df.at[indexFound, 'members']
        for member in members:
            if member.get('role') == 'ADMIN':
                print('changed')
                return member.get('fbUserId')
    return fbUserId
