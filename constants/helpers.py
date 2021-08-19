def remove_outliers_per_column(threshold, df, col):
    higherPercent = df[col].quantile(1 - threshold)
    lowerPercent = df[col].quantile(threshold)
    df['isBetweenOutliers'] = df[col].between(lowerPercent, higherPercent)
    df = df[df['isBetweenOutliers'] == True]
    df.drop(columns=['isBetweenOutliers'], inplace=True)
    return df


def intersection(list1, list2):
    for l in list1:
        if l in list2:
            return True
    return False
