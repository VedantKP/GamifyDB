import pandas as pd
from read_data import readCsv

imdbSchema = ['name','url','year','rating','votes','plot']
salesSchema = ['Name','Year','Genre','NA_Sales','EU_Sales','JP_Sales','Other_Sales','Global_Sales']

# Function to create the 'Key' column for imdb dataframe
def create_key_imdb(row):
    return str(row['url']).split('/')[4]

# Function to create the 'Key' column for vgsales dataframe
def create_key_vgsales(row):
    name_key = ''.join(word[:3] for word in row['Name'].split())
    year_key = str(row['Year'])[-2:]
    genre_key = row['Genre'][:3]
    return name_key + year_key + genre_key

'''
Function to check duplicates in VG Sales dataframe
(Sorted neighbourhood method)
1. Create key
2. Sort elements based on key
3. Run a window on the dataframe to find duplicates
'''
def checkDuplicates(df: pd.DataFrame, datasetName: str) -> bool:
    duplicates = False
    # 1. Create Key
    if datasetName.lower() == 'vgsales':
        df['Key'] = df.apply(create_key_vgsales, axis=1)
    elif datasetName.lower() == 'imdb':
        df['Key'] = df.apply(create_key_imdb, axis=1)
    # 2. Sort elements based on key
    df.sort_values(by='Key', inplace=True)
    # df.to_csv(path_or_buf='../datasets/sortedKey{}.csv'.format(datasetName),sep=',',encoding='utf-8',index=False)
    # 3. Run a window to detect duplicates
    windowSize = 3
    totalRows = len(df)
    for i in range(totalRows - windowSize + 1):
        windowData = df.iloc[i:i+windowSize]
        keyCounts = windowData['Key'].value_counts() #extract unique key values
        sameKeyCount = keyCounts[keyCounts > 1].sum() #find the total number of rows involved in the duplication, by counting the number of keys that appear more than once and summing each such key count to get total duplicate rows
        if sameKeyCount >= 2: #if there are at least two rows with the same key, duplicate detected
            for value in keyCounts[keyCounts > 1].index:
                probableMatchingData = windowData[windowData['Key'] == value]
                # compare Name, Year and Genre in the sales data OR name and year in IMDB data to check if duplicates exist for the same key
                if (datasetName.lower() == 'vgsales' and probableMatchingData['Name'].nunique() == 1 and probableMatchingData['Year'].nunique() == 1 and probableMatchingData['Genre'].nunique() == 1) or (datasetName.lower() == 'imdb' and probableMatchingData['name'].nunique() == 1 and probableMatchingData['year'].nunique() == 1):
                    print('Duplicate found!')
                    print(probableMatchingData)        
                    duplicates = True
                    return duplicates
    
    print('duplicates found for {}!'.format(datasetName)) if duplicates else print('no duplicates for {}'.format(datasetName))
    return duplicates

'''
Function reads vgsales dataset and converts Year's datatype from float64 to int64.
Return the whole dataframe
'''
def getDataVgSales():
    vgsales_df = readCsv(path='../datasets/vgsales.csv',schema=salesSchema)
    vgsales_df = vgsales_df.fillna(0).astype({'Year':'int64'}) #fillna used to remove null values so that year can be cast to int64 for further comparison
    colsToRound = ['NA_Sales','EU_Sales','JP_Sales','Other_Sales','Global_Sales']
    for col in colsToRound:
        vgsales_df[col] = vgsales_df[col].round(2)    
    
    duplicates = checkDuplicates(vgsales_df.copy(deep=True),'vgsales')
    if duplicates:
        agg_df = vgsales_df.groupby(['Name', 'Year', 'Genre']).agg({
        'NA_Sales': 'sum',
        'EU_Sales': 'sum',
        'JP_Sales': 'sum',
        'Other_Sales': 'sum',
        'Global_Sales': 'sum'
        }).reset_index()

        agg_df.columns = ['Name', 'Year', 'Genre', 'NA_Sales', 'EU_Sales', 'JP_Sales', 'Other_Sales', 'Global_Sales']
        for col in colsToRound:
            agg_df[col] = agg_df[col].round(2)
        # agg_df.to_csv(path_or_buf='../datasets/vgFilter.csv',sep=',',index=False,encoding='utf-8')
        return agg_df
    else:
        return vgsales_df
'''
Function performs the following tasks:
1. Reads imdb data from csv file.
2. Removes duplicate records.
3. Matches data with vgsales dataset to return the matched data.
'''
def getDataImdb(names,years):
    imdb_df = readCsv(path='../datasets/imdb-videogames.csv',schema=imdbSchema)
    imdb_df = imdb_df.fillna(0).astype({'year':'int64','name':'string'})
    duplicates = checkDuplicates(imdb_df.copy(deep=True),'imdb')
    if duplicates:
        imdb_df = imdb_df.drop_duplicates('name')
        
    subset_df = pd.DataFrame(columns=imdbSchema)
    subset_df = subset_df.astype({'year':'int64','name':'string','rating':'float64'}) #Creates a blank dataframe with same schema as imdb

    for name,year in zip(names,years):
        record = imdb_df.loc[(imdb_df['name'] == name) & (imdb_df['year'] == year)] #Extracts records with matching name and year for the game
        if not record.empty:
            subset_df = pd.concat([subset_df,record],axis=0)
    # print(subset_df)
    #subset_df.to_csv(path_or_buf='../datasets/imdbExtract2.csv',sep=',',index=False,encoding='utf-8')
    return subset_df


"""returns matching dataframes from sales and imdb"""
def getCorrectSalesAndImdb():
    vgsales_df = getDataVgSales()  # all data "sales"
    names, year = vgsales_df.Name, vgsales_df.Year
    imdb_df = getDataImdb(names, year)  # imdb with intersection of sales and imdb data
    return vgsales_df, imdb_df


"""Function to calculate Jaccard similarity between two strings"""
def jaccard_similarity(s1, s2):
    set1 = set(s1)
    set2 = set(s2)
    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))
    return intersection / union if union != 0 else 0


"""Function to map attributes from input schemas to the global schema using Jaccard similarity"""
def map_to_global_schema(global_schema, source_df):
    mapping = {}
    for source_attribute in source_df.columns:
        max_similarity = 0
        mapped_attribute = None

        for global_attribute in global_schema:
            similarity = jaccard_similarity(source_attribute.lower(), global_attribute.lower())
            if similarity > max_similarity:
                max_similarity = similarity
                mapped_attribute = global_attribute

        mapping[source_attribute] = mapped_attribute

    return mapping


"""Combine sales and imdb to global table"""
def combine_to_global_table():
    """Define global schema"""
    global_schema = [
        'name', 'rating', 'votes', 'year', 'url', 'plot', 'Genre',
        'NA_Sales', 'EU_Sales', 'JP_Sales', 'Other_Sales', 'Global_Sales'
    ]

    # get both dataframes
    vgsales_df, imdb_df = getCorrectSalesAndImdb()
    
    # apply automated mapping algorithm
    mapping_vgsales_to_global = map_to_global_schema(global_schema, vgsales_df)
    mapping_imdb_to_global = map_to_global_schema(global_schema, imdb_df)

    imdb_df.rename(columns=mapping_imdb_to_global, inplace=True)

    # merge both tables to global dataframe
    global_df = pd.merge(vgsales_df.rename(columns=mapping_vgsales_to_global),
                         imdb_df,
                         on=['name', 'year'],  # Merging on 'name' and 'year'
                         how='inner')  # Choose appropriate merge type

    global_df = global_df.reindex(columns=global_schema)
    global_df.to_csv(path_or_buf='../datasets/globalDF.csv',sep=',',index=False,encoding='utf-8')
    return global_df
    
# Calling the function to get the combined global table
combined_global_table = combine_to_global_table()
