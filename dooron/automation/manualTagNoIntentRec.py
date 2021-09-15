import pandas as pd

from removeOutliers import remove_outliers_per_column_and_specific_number

man_tagged_no_intent = pd.read_csv('/Users/asaflev/Downloads/intent research - no intents exapmles (2).csv')
man_tagged_no_intent.rename(columns={'can auto solve': 'canAutoSolve'}, inplace=True)
man_tagged_no_intent = remove_outliers_per_column_and_specific_number(lower_number=0, higher_number=240,
                                                                      df=man_tagged_no_intent,
                                                                      col='timeToResolveTicketSeconds')

print(man_tagged_no_intent['timeToResolveTicketSeconds'].sum())

optional_tags = ['BUG', 'V', 'v']
man_tagged_no_intent_solved = man_tagged_no_intent.query(f'canAutoSolve in {optional_tags}')

print(man_tagged_no_intent_solved['timeToResolveTicketSeconds'].sum() /
      man_tagged_no_intent['timeToResolveTicketSeconds'].sum())
