# create_test_parquet.py
import pandas as pd

# Use the users.csv with city_id for a good test
df = pd.read_csv('examples/users.csv')
df.to_parquet('examples/users.parquet')

print("✅ 'examples/users.parquet' created successfully.")