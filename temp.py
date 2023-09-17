import pandas as pd
file_name = 'System_engineer_question3.csv'
col_names = [
    'ID', 'id', 'seq', 'origin', 'Asn - deamidation risk Cnt', 'Cys Cnt', 'Isomerization Cnt', 'Met Cnt', 'N-Glycosylation Cnt',
    'Pro Cnt', 'Strong Deamidation Cnt', 'Weak Deamidation Cnt', 'SEQUENCE_TYPE', 'STOICHIOMETRY', 'Format', 'Isotype'
]

df = pd.read_csv(file_name, usecols=col_names, low_memory=False)
df = df[df['ID'].notna()].set_index('ID', drop=True) # Assuming ID is not a unique identifier and just an incrementation of the index
# print((df.head(1).to_dict('records')))

print(df.loc[39])
