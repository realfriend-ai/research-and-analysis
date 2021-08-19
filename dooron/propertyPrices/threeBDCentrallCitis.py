import pandas as pd

from constants.helpers import intersection
from constants.importantDates import first_of_july_21, first_of_aug_21
from constants.israelAreas import neighbourhoodsTLV
from constants.mongoConnectDooron import properties_collection_dooron, propertyReq_collection_dooron

cities_to_check_in = ['תל אביב יפו', 'רמת גן', 'גבעתיים', 'הרצליה', 'חיפה', 'אילת', 'ראשון לציון', 'באר שבע']


def create_properties_data_frame():
    list1 = list(properties_collection_dooron.find({
        'category': 'APT_SALE',
        'createdAt': {
            '$gte': first_of_july_21,
            '$lte': first_of_aug_21,
        },

    }, {'address': 1, 'createdAt': 1, 'numOfRooms': 1, 'saleData': 1, 'squareMeters': 1}))
    propertyData = pd.DataFrame(list1)
    propertyData.dropna(subset=['saleData'], inplace=True)
    propertyData['city'] = propertyData['address'].apply(lambda x: x.get('city'))
    propertyData.query(f'(numOfRooms==3) & (city in {cities_to_check_in})', inplace=True)
    propertyData['salePrice'] = propertyData['saleData'].apply(lambda x: x.get('salePriceIncludeBroker'))
    return propertyData


p_data = create_properties_data_frame()
grouped_by_city = pd.DataFrame(p_data.groupby('city')['salePrice'].median()).reset_index()


def is_three_rooms(num_of_rooms):
    fromNum = num_of_rooms.get('from')
    toNum = num_of_rooms.get('to')
    if fromNum == 3:
        if pd.isna(toNum) is True:
            return True
        if abs(fromNum - toNum) < 2:
            return True
        else:
            return False
    if toNum == 3:
        if pd.isna(fromNum) is True:
            return True
        if abs(fromNum - toNum) < 2:
            return True
        else:
            return False
    return False


def get_property_requests_in_cities():
    list1 = list(propertyReq_collection_dooron.find({
        'category': 'APT_RENT',
        'createdAt': {'$gte': first_of_july_21,
                      '$lte': first_of_aug_21, },
    }, {'city': 1, 'rentPrice': 1, 'areas': 1, 'numOfRooms': 1}
    ))
    property_req_df = pd.DataFrame(list1)
    property_req_df['isThreeRooms'] = property_req_df['numOfRooms'].apply(lambda x: is_three_rooms(x))
    property_req_df = property_req_df[property_req_df['isThreeRooms']]
    return property_req_df


prop_req = get_property_requests_in_cities()
ar = pd.DataFrame(prop_req['areas'].value_counts()).reset_index()


def check_if_city_in_areas(areas, city):
    if city != 'תל אביב יפו':
        for area in areas:
            if (city in area) | (city == area):
                return True
        return False
    if city == 'תל אביב יפו':
        for area in areas:
            if ('תל אביב' in area) | ('תל אביב' == area):
                return True
        if intersection(areas, neighbourhoodsTLV):
            return True
        return False


def get_rent_price(rent_price):
    rentPriceTo = rent_price.get('to')
    if pd.isna(rentPriceTo) is False:
        return rentPriceTo
    else:
        return rent_price.get('from')


def get_median_request_price_per_city(city, property_req_df: pd.DataFrame):
    property_req_df['hasCityInAreas'] = property_req_df['areas'].apply(lambda x: check_if_city_in_areas(x, city))
    property_req_df_city = property_req_df[property_req_df['hasCityInAreas']]
    property_req_df_city['rentPriceTo'] = property_req_df['rentPrice'].apply(lambda x: get_rent_price(x))
    property_req_df_city.reset_index(inplace=True)
    return {'city': city, 'numberOfRequestsInJuly': len(property_req_df_city.index),
            'medianPrice': property_req_df_city['rentPriceTo'].median()}


res_df = []
for city in cities_to_check_in:
    res_df.append(get_median_request_price_per_city(property_req_df=prop_req, city=city))

res_df = pd.DataFrame(res_df)
