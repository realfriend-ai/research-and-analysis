import pandas as pd

from constants.importantDates import now, last_year
from constants.mongoConnectDooron import properties_collection_dooron
from dooron.photoEnhance.helpers import get_feature_column, download_photos_from_liks


def find_enhance_number_per_kind_from_property_enhancement_collection(start, end, rank):
    list1 = list(properties_collection_dooron.find({
        'createdAt':
            {'$gte': start,
             '$lte': end
             },
        'ourRank': rank,
        'photosCount': {'$gte': 0},
        'enhanced': True,
    }, {'_pred': 1, 'ourRank': 1, 'enhanced': 1, 'enhancedKind': 1}).limit(100000))
    prop_en_df = pd.DataFrame(list1)
    prop_en_df['hasPrediction'] = prop_en_df['_pred'].map(lambda x: False if pd.isna(x) else True)
    get_feature_column(prop_en_df, 'ourRankPrediction')
    get_feature_column(prop_en_df, 'isRenovatedPrediction')
    get_feature_column(prop_en_df, 'hasNaturalLightPrediction')
    prop_en_df['imageInput'] = prop_en_df['_pred'].apply(lambda x: x.get('imageInput') if type(x) == dict else None)
    # prop_en_df = prop_en_df.applymap(lambda x: 'noPred' if x == None else x)
    return prop_en_df


for rank in [1, 2, 4, 5]:
    prop_en_df = find_enhance_number_per_kind_from_property_enhancement_collection(start=last_year, end=now,
                                                                                   rank=rank)
    no_rank_pred = prop_en_df[prop_en_df['ourRankPrediction'] == 'noPred']
    no_rank_pred.dropna(subset=['imageInput'], inplace=True)
    no_rank_pred.reset_index(drop=True, inplace=True)
    rank_temp = no_rank_pred[no_rank_pred['ourRank'] == rank]
    download_photos_from_liks(df=rank_temp, folder='ourRank', classification=f'rank{rank}')
    print(f'Finished getting manually tagged photos of {rank}')
