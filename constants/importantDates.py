from datetime import datetime
from datetime import timedelta
# 2021
first_of_may_21 = datetime(2021, 5, 1)
first_of_apr_21 = datetime(2021, 4, 1)
first_of_mar_21 = datetime(2021, 3, 1)
first_of_feb_21 = datetime(2021, 2, 1)
first_of_jan_21 = datetime(2021, 1, 1)
# 2020
first_of_dec = datetime(2020, 12, 1)

first_of_nov = datetime(2020, 11, 1)
first_of_oct = datetime(2020, 10, 1)
first_of_sep = datetime(2020, 9, 1)
first_of_aug = datetime(2020, 8, 1)
first_of_july = datetime(2020, 7, 1)
middle_of_june = datetime(2020, 6, 15)
first_of_june = datetime(2020, 6, 1)

first_of_may = datetime(2020, 5, 1)
first_of_april = datetime(2020, 4, 1)
first_of_march = datetime(2020, 3, 1)
first_of_feb = datetime(2020, 2, 1)
first_of_jan = datetime(2020, 1, 1)

# 2019
first_of_dec_2019 = datetime(2019, 12, 1)
first_of_nov_2019 = datetime(2019, 11, 1)

first_of_oct_2019 = datetime(2019, 10, 1)

first_of_sep_2019 = datetime(2019, 9, 1)

middle_of_aug_2019 = datetime(2019, 8, 9)

first_of_aug_2019 = datetime(2019, 8, 1)

middle_of_july_2019 = datetime(2019, 7, 26)
first_of_july_2019 = datetime(2019, 7, 1)
middle_of_june_2019 = datetime(2019, 6, 15)
first_of_june_2019 = datetime(2019, 6, 1)
first_of_may_2019 = datetime(2019, 5, 1)
first_of_april_2019 = datetime(2019, 4, 1)
first_of_march_2019 = datetime(2019, 3, 1)
first_of_feb_2019 = datetime(2019, 2, 1)
first_of_jan_2019 = datetime(2019, 1, 1)

# custom dates
duplicate_new_logic = datetime(2020, 8, 24)

# relative dates
now = datetime.now()
mongoNow = now - timedelta(hours=2)
sixOclockLastNight = mongoNow - timedelta(hours=24)
eightOcklockYesterday = mongoNow - timedelta(hours=34)
last_day = now - timedelta(days=1)
last_two_days = now - timedelta(days=2)
last_week = now - timedelta(days=7)
last_two_weeks = now - timedelta(days=14)

last_month = now - timedelta(days=30)
last_45_days = now - timedelta(days=45)
last_three_months = now - timedelta(days=90)
last_six_months = now - timedelta(days=180)

last_year = now - timedelta(days=365)
next_year = now + timedelta(days=365)
before_five_years = now - timedelta(1825)
