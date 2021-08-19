import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

df = pd.read_csv('/Users/asaflev/Downloads/Duplicates_man_check_tagged - APT_RENT_MAN_CHECK.csv')
df.query(f"manTag in {['f', 'v']}", inplace=True)
y = df['similarityScore']
x = df['manTag']
sns.scatterplot(x=x, y=y)
plt.axhline(y=0.75, color='r', linestyle='dotted')
plt.axhline(y=0.45, color='g', linestyle='dotted')
plt.yticks(np.arange(0, max(y), 0.02))
plt.gcf().set_size_inches(4, 10)
plt.show()


# def get_black_list_metric_by_score(black_list_score):
#     df_white_list = get_white_list_metric_by_score(white_list_score=0.45)
#     df_temp = df[df['similarityScore'] > black_list_score]
#     num_of_cases_should_man_solved = len(df.index) - len(df_white_list.index) - len(df_temp.index)
#     print(f'Black list score is: {black_list_score}')
#     print(df_temp['manTag'].value_counts())
#     print(df_temp['manTag'].value_counts(normalize=True))
#     print(
#         f'Num of cases need to manually solve:'
#         f'  {num_of_cases_should_man_solved} out of {len(df.index)} which is  {round(100 * num_of_cases_should_man_solved / len(df.index))} percent of total')
#
#     return df_temp
#
#
# #
# # black_score_list = [0.75, 0.72, 0.65]
# # for s in black_score_list:
# #     get_black_list_metric_by_score(black_list_score=s)
# #
#
# def get_white_list_metric_by_score(white_list_score):
#     df_temp = df[df['similarityScore'] < white_list_score]
#     # df_black_list = get_black_list_metric_by_score(black_list_score=0.25)
#     # num_of_cases_should_man_solved = len(df.index) - len(df_black_list.index) - len(df_temp.index)
#     # print(f'White list score is: {white_list_score}')
#     # print(df_temp['manTag'].value_counts())
#     # print(df_temp['manTag'].value_counts(normalize=True))
#     # print(
#     #     f'Num of cases need to manually solve:'
#     #     f'  {num_of_cases_should_man_solved} out of {len(df.index)} which is  {round(100 * num_of_cases_should_man_solved / len(df.index))} percent of total')
#     return df_temp
#
#
# black_score_list = [0.75, 0.72, 0.65]
# for s in black_score_list:
#     get_black_list_metric_by_score(black_list_score=s)


def find_cases_need_manual_checking(bl_score, wl_score, path):
    dup_df = pd.read_csv(path)
    dup_df_between_thresholds = dup_df[
        (dup_df['similarityScore'] > wl_score) & (dup_df['similarityScore'] < bl_score)]
    number_of_man_cases = len(dup_df_between_thresholds.index)
    number_of_man_cases_per_day = round(number_of_man_cases / 30)
    print(f'Num of cases per day: {number_of_man_cases_per_day}')


# find_rent_cases_need_manual_checking(bl_score=0.75, wl_score=0.45)


find_cases_need_manual_checking(bl_score=0.75, wl_score=0.45, path='duplicates_rent.csv')
# find_cases_need_manual_checking(bl_score=0.25, wl_score=0.15, path='duplicates_sale.csv')
