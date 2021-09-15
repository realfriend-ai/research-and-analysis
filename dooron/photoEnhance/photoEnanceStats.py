import pandas as pd

from constants.importantDates import now, last_month
from constants.mongoConnectDooron import properties_collection_dooron
from dooron.photoEnhance.helpers import get_feature_column


def find_enhance_number_per_kind_from_property_enhancement_collection(start, end):
    list1 = list(properties_collection_dooron.find({
        'createdAt':
            {'$gte': start,
             '$lte': end
             },
        'photosCount': {'$gte': 0},
        'enhanced': True,
    }, {'_pred': 1, 'isRenovated': 1, 'hasNaturalLight': 1, 'ourRank': 1, 'createdAt': 1, 'enhanced': 1,
        'photosCount': 1, 'enhancedKind': 1}))
    prop_en_df = pd.DataFrame(list1)
    prop_en_df['hasPrediction'] = prop_en_df['_pred'].map(lambda x: False if pd.isna(x) else True)
    get_feature_column(prop_en_df, 'ourRankPrediction')
    get_feature_column(prop_en_df, 'isRenovatedPrediction')
    get_feature_column(prop_en_df, 'hasNaturalLightPrediction')
    prop_en_df['imageInput'] = prop_en_df['_pred'].apply(lambda x: x.get('imageInput') if type(x) == dict else None)
    prop_en_df = prop_en_df.applymap(lambda x: 'noPred' if x == None else x)
    return prop_en_df


prop_en_df = find_enhance_number_per_kind_from_property_enhancement_collection(start=last_month, end=now)
val_1 = pd.DataFrame(prop_en_df['enhancedKind'].value_counts(normalize=False))
val_2 = pd.DataFrame(prop_en_df['isRenovatedPrediction'].value_counts(normalize=True))
val_3 = pd.DataFrame(prop_en_df['hasNaturalLightPrediction'].value_counts(normalize=True))
val_4 = pd.DataFrame(prop_en_df['ourRankPrediction'].value_counts(normalize=True))
val_5 = pd.DataFrame(prop_en_df['hasPrediction'].value_counts(normalize=True))




# def main_func(start, end):
#     print('property enhancements')
#     property_enhancement_df = find_enhance_number_per_kind_from_property_enhancement_collection(start, end)
#     print(f'between {start} and {end} num of properties in enhancement are: {len(property_enhancement_df.index)}')
#     print(property_enhancement_df['ourRankPrediction'].value_counts())
#     print(property_enhancement_df['isRenovatedPrediction'].value_counts())
#     print(property_enhancement_df['hasNaturalLightPrediction'].value_counts())
#
#
# main_func(start=last_week, end=datetime.now())
#
#
# def find_enhanced_properties_in_the_last_week(start, end):
#     list1 = list(property_collection.find({
#         'createdAt':
#             {'$gte': start,
#              '$lte': end
#              },
#         'enhancedYN': True
#     }, {'enhancedYN': 1, 'enhancedDate': 1, 'enhancedKind': 1, 'enhancements': 1, '_pred': 1}))
#     prop_en_df = pd.DataFrame(list1)
#     prop_en_df['enhancedKind'] = prop_en_df['enhancedKind'].fillna(value='noKind')
#     print(prop_en_df['enhancedKind'].value_counts())
#     prop_en_df['hasPrediction'] = prop_en_df['_pred'].map(lambda x: False if pd.isna(x) else True)
#     print(prop_en_df['hasPrediction'].value_counts())
#     get_feature_column(prop_en_df, 'ourRankPrediction')
#     print(prop_en_df['ourRankPrediction'].value_counts())
#
#     return prop_en_df
#
#
# enhancedProperties_df = find_enhanced_properties_in_the_last_week(start=last_week, end=datetime.now())
