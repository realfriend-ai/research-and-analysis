from constants.intents import set_request_related_luke, set_request_related_dooron


def remove_outliers_per_column(threshold, df, col):
    higherPercent = df[col].quantile(1 - threshold)
    lowerPercent = df[col].quantile(threshold)
    df['isBetweenOutliers'] = df[col].between(lowerPercent, higherPercent)
    df = df[df['isBetweenOutliers'] == True]
    df.drop(columns=['isBetweenOutliers'], inplace=True)
    return df


def find_sq_answers(text):
    if type(text) == str:
        words_sq = ['לא', 'בסדר', 'אחלה', 'מעולה', 'אוקי', 'אין בעיה', 'סבבה', 'כן']
        wrds_to_replace = ['.', '/', ',']
        for rep_wrd in wrds_to_replace:
            text = text.replace(rep_wrd, '')
        if len(text.split()) < 2:
            for wrd in words_sq:
                if wrd in text:
                    return True
        return False
    return False


def intersection(list1, list2):
    for l in list1:
        if l in list2:
            return True
    return False


def get_intents_and_fill_na_dooron(row):
    intents_before = row.get('intentsBefore')
    if type(intents_before) == list:
        if len(intents_before) > 0:
            if intersection(intents_before, set_request_related_dooron):
                return 'SET_REQUEST_RELATED'
            else:
                return intents_before[0]
        else:
            return 'NO_INTENT'
    elif find_sq_answers(row.get('textBefore')):
        return 'SQ'
    else:
        return 'NO_INTENT'


def get_intents_and_fill_na_luke(row):
    intents_before = row.get('intentsBefore')
    if type(intents_before) == list:
        if len(intents_before) > 0:
            if intersection(intents_before, set_request_related_luke):
                return 'SET_REQUEST_RELATED'
            else:
                return intents_before[0]
        else:
            return 'NO_INTENT'
    elif find_sq_answers(row.get('textBefore')):
        return 'SQ'
    else:
        return 'NO_INTENT'
