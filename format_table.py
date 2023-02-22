import pandas as pd


def format_values_in_fee_table(table: pd.DataFrame):
    for column in table.columns:
        table[column] = table[column].apply(lambda x: "${:.2f}".format(x))

    return table


def format_values_in_count_table(table: pd.DataFrame):
    for column in table.columns:
        table[column] = table[column].apply(lambda x: "{:.0f}".format(x))

    return table
