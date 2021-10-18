from datetime import datetime, timedelta

import pandas as pd

from constants.imageRecModelIds import model_name_dict
from constants.mongoConnectLuke import property_collection
from displaySetting import display_settings

display_settings()
columns = ['protoPayload.requestMetadata.requestAttributes.time', 'protoPayload.methodName',
           'protoPayload.authorizationInfo.resource', 'protoPayload.status.code', 'protoPayload.status.message', ]

df = pd.read_csv('/Users/asaflev/Downloads/downloaded-logs-20211008-162932.csv')
df = df[columns]
df.rename(columns={'protoPayload.requestMetadata.requestAttributes.time': 'reqTime',
                   'protoPayload.methodName': 'reqKind',
                   'protoPayload.authorizationInfo.resource': 'model',
                   'protoPayload.status.code': 'statusCode',
                   'protoPayload.status.message': 'statusMsg'

                   }, inplace=True)

df['modelId'] = df['model'].apply(lambda x: x[x.find('IC'):])
df['reqKind'] = df['reqKind'].apply(lambda x: x[x.find('.AutoMl.') + len('.AutoMl.'):])


def get_model_name_by_id(model_id_input):
    index_in_val = list(model_name_dict.values()).index(model_id_input)
    return list(model_name_dict.keys())[index_in_val]


df['modelName'] = df['modelId'].apply(lambda x: get_model_name_by_id(x))
df['reqTimeDateTime'] = df['reqTime'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%fz"))


def is_the_message_is_already_deplyoing(status_msg):
    if pd.isna(status_msg) is True:
        return False
    if type(status_msg) == str:
        if status_msg.find(errors_for_deploy_str) == -1:
            return False
        else:
            return True


errors_for_deploy_str = 'Another DEPLOY model operation is running on the model:'
df['errorOfAlreadyDeploying'] = df['statusMsg'].apply(lambda x: is_the_message_is_already_deplyoing(x))
df = df[df['errorOfAlreadyDeploying'] == False]
df.sort_values(by=['reqTimeDateTime'], ascending=True, inplace=True, ignore_index=True)


def get_times_of_working_per_model(model_name):
    df_model = df[df['modelName'] == model_name].reset_index(drop=True)
    working_time_df = []
    prevReqKind = 'UndeployModel'
    for i in df_model.index:
        reqKind = df_model.at[i, 'reqKind']
        if (reqKind == 'DeployModel') & (prevReqKind == 'UndeployModel'):
            startTime = df_model.at[i, 'reqTimeDateTime']
            prevReqKind = reqKind
            continue
        if (reqKind == 'UndeployModel') & (prevReqKind == 'DeployModel'):
            endTime = df_model.at[i, 'reqTimeDateTime']
            prevReqKind = reqKind
            working_time_df.append({'startTime': startTime, 'endTime': endTime})
    working_time_df = pd.DataFrame(working_time_df)
    working_time_df.insert(loc=0, column='model', value=model_name)
    working_time_df['startTime'] = working_time_df['startTime'].apply(lambda x: x + timedelta(minutes=30))
    return working_time_df


working_time_df = get_times_of_working_per_model('IS_RENOVATED')


def find_properties_with_no_pred(start, end):
    list1 = list(property_collection.find({
        'createdAt':
            {'$gte': start,
             '$lte': end
             },
        'enhancedYN': True,
    }, {'_pred': 1, 'createdAt': 1, 'enhancedDate': 1, 'photosCount': 1, 'enhancedKind': 1}))
    prop_en_df = pd.DataFrame(list1)
    prop_en_df['hasPrediction'] = prop_en_df['_pred'].map(lambda x: False if pd.isna(x) else True)
    prop_en_df_no_pred = prop_en_df[prop_en_df['hasPrediction'] == False]
    return prop_en_df_no_pred


no_pred_df = find_properties_with_no_pred(start=datetime(2021, 10, 3), end=datetime(2021, 10, 9))
print(f'Total no preds in time frame: {len(no_pred_df.index)}')
num_of_no_pred_in_working_hours = 0
for i in working_time_df.index:
    start_time = working_time_df.at[i, 'startTime']
    end_time = working_time_df.at[i, 'endTime']
    condition_1 = no_pred_df['enhancedDate'] > start_time
    condition_2 = no_pred_df['enhancedDate'] < end_time
    no_pred_df_temp = no_pred_df[condition_1 & condition_2].reset_index()
    num_of_no_pred_in_working_hours = num_of_no_pred_in_working_hours + len(no_pred_df_temp.index)

print(f'Total no preds in working hours time frame: {num_of_no_pred_in_working_hours}')
