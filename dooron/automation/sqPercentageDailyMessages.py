import pandas as pd

from dooron.automation.intetnResearch2 import get_received_msgs_from_whatsapp, get_whatsapp_users

ourWhatsappNumbers = ['972542324236', '972502020220']


def get_is_by_us_whatsapp(row):
    fromId = row.get('whatsappPhoneNumber')
    if fromId not in ourWhatsappNumbers:
        return False
    else:
        return True


def find_sq_answers(text):
    words_sq = ['לא', 'בסדר', 'אחלה', 'מעולה', 'אוקי', 'אין בעיה', 'סבבה', 'כן']
    if len(text.split()) < 2:
        for wrd in words_sq:
            if wrd in text:
                return True
    return False


def main_func():
    user_text_whatsapp = get_received_msgs_from_whatsapp(limit=1000000)
    users_whatsapp_df = get_whatsapp_users(user_text_whatsapp['whatsappPhoneNumber'].tolist())
    merged_df = pd.merge(users_whatsapp_df, user_text_whatsapp, how='inner', left_on='whatsappPhoneNumber',
                         right_on='whatsappPhoneNumber')
    merged_df['isByUs'] = merged_df.apply(lambda row: get_is_by_us_whatsapp(row), axis=1)
    merged_df = merged_df[merged_df['isByUs'] == False]
    merged_df['isTextContainSq'] = merged_df['text'].apply(lambda x: find_sq_answers(x))
    return merged_df[['text', 'isTextContainSq']]


merged_df = main_func()
print(merged_df['isTextContainSq'].value_counts())
print(merged_df['isTextContainSq'].value_counts(normalize=True))

