import pandas as pd
import os
import main


calcs = main.Calculations()


def test_i1_calc():
    df = pd.DataFrame({
        'Instrument': ['INSTRUMENT1', 'INSTRUMENT1', 'INSTRUMENT1'],
        'Date': ['2014-11-02', '2014-11-03', '2014-11-17'],
        'Price': [3.00875, 3.05255, 3.0662]
    })
    mean = calcs.i1_calc(df)
    assert mean == 3.0425

def test_i2_calc():
    df = pd.DataFrame({
        'Instrument': ['INSTRUMENT2', 'INSTRUMENT2', 'INSTRUMENT2'],
        'Date': ['2014-11-02', '2014-11-03', '2014-11-17'],
        'Price': [3.00875, 3.05255, 3.0662]
    })
    df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
    mean = calcs.i2_calc(df)
    assert mean == 3.0425

def test_i3_calc():
    df = pd.DataFrame({
        'Instrument': ['INSTRUMENT2', 'INSTRUMENT2', 'INSTRUMENT2'],
        'Date': ['2014-11-02', '2014-11-03', '2014-11-17'],
        'Price': [3.00875, 3.05255, 3.0662]
    })
    df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
    mean = calcs.i3_calc(df)
    assert mean == 0

def test_create_table():
    df = pd.DataFrame({
        'Instrument': ['INSTRUMENT2', 'INSTRUMENT2', 'INSTRUMENT2'],
        'Date': ['2014-11-02', '2014-11-03', '2014-11-17'],
        'Price': [3.00875, 3.05255, 3.0662]
    })
    df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')

    calcs.create_table(df, 0.00)
    assert os.path.isfile("./data_test_db.db") == True
