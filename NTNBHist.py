import pandas as pd

# Load with correct encoding/decimal
df = pd.read_csv("downloads/precotaxatesourodireto.csv", sep=';', decimal=',', encoding='latin1', low_memory=False)

# Clean up column names
df.columns = [c.strip() for c in df.columns]

# Filter: IPCA+ com Juros Semestrais, maturity 15/05/2035
mask_title = df['Tipo Titulo'].str.contains('IPCA\\+.*Juros Semestrais', regex=True, na=False)
mask_mat   = df['Data Vencimento'].astype(str).str.contains('15/05/2035', na=False)
sub = df[mask_title & mask_mat].copy()

# Parse date column (important!)
sub['Data Base'] = pd.to_datetime(sub['Data Base'], dayfirst=True, errors='coerce')
#print(sub.head())

# Drop bad rows
sub = sub.dropna(subset=['Data Base'])
#print(sub.head())

# Pick the yield column (Taxa Compra a.a.)
yield_cols = [c for c in sub.columns if 'Taxa' in c and 'Compra']
col = yield_cols[0]
print(col)

# Set datetime as index
sub = sub.set_index('Data Base').sort_index()

# Now resample by month-end and take last available yield
monthly = sub[col].resample('M').last()
print(monthly.head())

#Last 15 years only
last_15y = monthly.last('180M')

# Format result
out = last_15y.round(2).rename('RealYield_%a.a.').to_frame()
out.index = out.index.strftime('%Y-%m')
print(out.to_csv(index=True))
out.to_csv("ntnb2035_monthly_yields.csv", index=True)