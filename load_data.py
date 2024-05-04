import pandas as pd

# Use raw string or double backslashes to avoid escaping
file_path = r'D:\Data Pipeline\real-estate-data-from-7-indian-cities\REData.csv'
# Or file_path = 'C:\\Users\\Mandar S\\real-estate-data-from-7-indian-cities\\data.csv'

newdata = pd.read_csv(file_path)
newdata.head()
newdata.info()