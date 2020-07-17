def add_suffix_to_cols(df, suffix):
    for col in df.columns:
        df = df.withColumnRenamed(col, col + suffix)
    return df

def isfloat(value):
    try:
        float(value)
        return True
    except ValueError:
        return False

def add_prefix_to_cols(df, prefix, exclude_cols):
    columns_to_prefix = [col for col in df.columns if col not in exclude_cols]
    for col in columns_to_prefix:
        if col.isdigit():
            df = df.withColumnRenamed(col, prefix + col)
        elif isfloat(col):
            df = df.withColumnRenamed(col, prefix + str(int(float(col))))
        else:
            df = df.withColumnRenamed(col, prefix + col)
    return df

def fillna_0(df, exclude_cols):
    columns_to_fill = [col for col in df.columns if col not in exclude_cols]
    df = df.fillna(0, subset=columns_to_fill)
    return df