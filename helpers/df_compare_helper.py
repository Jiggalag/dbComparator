import numpy as np
import pandas as pd


def get_dataframes_diff(prod_columns, test_columns):
    # TODO: clarify, how I can dinamically set indexes for different tables
    # df_all = pd.concat([prod_columns.set_index('id'), test_columns.set_index('id')], axis='columns', keys=['First', 'Second'])
    df_all = pd.concat([prod_columns, test_columns], axis='columns', keys=['First', 'Second'])
    df_final = df_all.swaplevel(axis='columns')[prod_columns.columns[1:]]
    df_final[(prod_columns != test_columns).any(1)].style.apply(highlight_diff, axis=None)
    return df_final

def highlight_diff(data, color='yellow'):
    attr = 'background-color: {}'.format(color)
    other = data.xs('First', axis='columns', level=-1)
    return pd.DataFrame(np.where(data.ne(other, level=0), attr, ''), index=data.index, columns=data.columns)