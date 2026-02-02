import sys
import pandas as pd

args = sys.argv

print(f'Arguments are: {args}')

df = pd.DataFrame({"A": [1, 2, 3], "B": ['a', 'b', 'c']})
df['C'] = args[1]
print(df)

df.to_parquet('output.parquet')