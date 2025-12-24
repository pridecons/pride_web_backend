from utils.NSE_Formater.security_format import SecuritiesConverter 

conv = SecuritiesConverter() 
df = conv.convert_to_csv( dat_file_path="Securities_November212025.dat", csv_file_path="securities_master.csv", ) 

if df is not None:
    df.to_csv("securities_master.csv", index=False)
    print("CSV successfully saved!")
else:
    print("Failed to parse securities file.")