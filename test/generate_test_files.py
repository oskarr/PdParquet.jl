import os
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo

def generate_df_1():
    """Generates a generic dataframe."""
    dt = pd.to_datetime([datetime.fromisoformat(f'1970-01-01 01:23:45.678+0{d}:00') for d in range(5)], utc=True)
    df = pd.DataFrame({
        'StringCol': ["Lorem", "ipsum", "dolor", "sit", "amet"],
        'CategoricalCol': pd.Categorical(["A", "B", "A", "B", "C"]),
        'DateTimeCol': dt.tz_convert(None),
        'DateTimeColTz': dt.tz_convert('Europe/Berlin'),
        'DateTimeColUTC': dt.tz_convert('UTC'),
    })
    df.to_parquet('generated/df1.parquet')
    print(df)
    print(df.dtypes)

if __name__ == "__main__":
    if not os.path.isdir("generated"):
        os.mkdir("generated")
    generate_df_1()
