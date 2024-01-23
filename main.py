import pandas as pd
from sqlalchemy import Column, Integer, String, Float, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


Base = declarative_base()


class InstrumentPriceModifier(Base):
    __tablename__ = "INSTRUMENT_PRICE_MODIFIER"

    id = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=False)
    multiplier = Column(Float, default=1.0)


class Calculations:
    def __init__(self):
        return

    def i1_calc(self, df: pd.DataFrame) -> float:
        """
        Function to get the rows with INSTRUMENT1 and calculate the mean.
        :param df: pandas dataframe
        :return: float
        """
        instrument1 = df.loc[df['Instrument'] == 'INSTRUMENT1']
        i1_mean = instrument1['Price'].mean()

        return i1_mean

    def i2_calc(self, df: pd.DataFrame) -> float:
        """
        Function to get the rows with INSTRUMENT2, filter by Novemeber 2014 and calculate the mean.
        :param df: pandas dataframe
        :return: float
        """
        i2 = df.loc[df['Instrument'] == 'INSTRUMENT2']
        i2_2014 = i2.loc[i2['Date'].dt.year == 2014]
        i2_nov_2014 = i2_2014.loc[i2['Date'].dt.month == 11]
        i2_mean = i2_nov_2014['Price'].mean()

        return i2_mean

    def i3_calc(self, df: pd.DataFrame) -> float:
        """
        This function get the data that don't INSTRUMENT1, INSTRUMENT2 and INSTRUMENT3 and gets the average
        value of the existing rows after the filter.
        :param df: pandas dataframe
        :return: float
        """
        i3_average = 0.00

        i3 = df.loc[~df['Instrument'].isin(["INSTRUMENT1", "INSTRUMENT2", "INSTRUMENT3"])]
        i3_other = i3[::-15]

        if len(i3_other) > 0:
            i3_average = i3.average()

        return i3_average

    def create_table(self, df: pd.DataFrame, mult: float) -> None:
        """
        Function to create the SQLite3 database file and insert data.

        :param df: pandas dataframe
        :param mult: float
        :return: None
        """
        engine = create_engine('sqlite:///./data_test_db.db', echo=True)
        Base.metadata.create_all(bind=engine)
        session = sessionmaker(bind=engine)
        session = session()

        for row in df.iloc:
            query = session.query(InstrumentPriceModifier).filter(InstrumentPriceModifier.name == "INSTRUMENT1")
            result = query.all()

            if len(result) == 0:
                data = InstrumentPriceModifier(
                   name=row['Instrument'],
                   multiplier=row['Price']
                )
            else:
                data = InstrumentPriceModifier(
                    name=row['Instrument'],
                    multiplier=row['Price'] * mult
                )

            session.add(data)
        session.commit()

        return


if __name__ == "__main__":
    calcs = Calculations()

    df = pd.read_csv("example_input.txt", header=None, parse_dates=[1])
    df.rename(columns={0: 'Instrument', 1: 'Date', 2: 'Price'}, inplace=True)

    # -- get instrument 1 calculation --
    i1 = calcs.i1_calc(df)
    print("Instrument1 mean: "+str(i1))

    # -- get instrument 2 calculation --
    i2 = calcs.i2_calc(df)
    print("Instrument2 mean for November 2014: "+str(i2))

    # -- get instrument 3 calculation --
    i3 = calcs.i3_calc(df)
    print("Other instrument average: "+str(i3))

    # -- database functions
    calcs.create_table(df, i2)
