import pandas as pd
from iss_pass_pipeline.iss_pass_etl import fetch_iss_pass_data

def test_fetch_iss_pass():
    df = fetch_iss_pass_data(51.5074, -0.1278, n=1)
    assert isinstance(df, pd.DataFrame), "The result should be a pandas DataFrame"
    if not df.empty:
        assert 'risetime' in df.columns, "DataFrame should contain 'risetime' column"
        assert 'duration' in df.columns, "DataFrame should contain 'duration' column"
        assert pd.api.types.is_datetime64_any_dtype(df['risetime']), "'risetime' should be a datetime type"
        assert pd.api.types.is_integer_dtype(df['duration']), "'duration' should be an integer type"


    