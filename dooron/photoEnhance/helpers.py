import traceback
import urllib

import pandas as pd


def download_photos_from_liks(df, folder, classification):
    for i in df.index:
        try:
            urllib.request.urlretrieve(df.at[i, 'imageInput'],
                                       f"/Users/asaflev/Desktop/imageEnhanceDebugging/Photos/dooron/"
                                       f"{folder}/{classification}/{i + 2}.jpg")
            if i % 10 == 0:
                print(f'finished {i} samples')
        except Exception as e:
            traceback.print_exception(e, e, e.__traceback__)
            continue


def get_pred_feature(pred, feature):
    if feature in pred:
        res = pred[feature]['displayName']
        return res
    elif pred.get('processed') is False:
        return 'didntGetToModel'
    else:
        return 'noPred'


def get_feature_column(df, feature):
    df[feature] = df.apply(
        lambda row: get_pred_feature(row['_pred'], feature) if row['hasPrediction'] else 'didntGetToModel', axis=1)


def has_predictions_of_all_features(_pred):
    three_predictions = ['ourRankPrediction', 'isRenovatedPrediction', 'hasNaturalLightPrediction']
    for pred in three_predictions:
        if pred not in _pred:
            return False
    return True
