import pandas as pd
import sys

def get_stats(file_path):
    df = pd.read_parquet(file_path)


    size = df.size()

    columns = {}

    for col in df.columns:
        columns[col] = {
            "cardinality": df[col].nunique(),
            "mean": df[col].mean(),
            "stddev": df[col].std(),
            "min": df[col].min(),
            "max": df[col].max()
        }

    data = pd.DataFrame(columns)

    

    return data


__main__(

    args = sys.argv[1:]
    for file_path in os.listdir(args[1]):
        print(get_stats(file_path))



)
