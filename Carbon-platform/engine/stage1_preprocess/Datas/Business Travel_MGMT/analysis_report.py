


import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# File paths
data_path = r"C:\Users\FlorianDemir\Desktop\Business Travel_MGMT\January 2025(WholeYear)\cleaned_source_Raw_Data.xlsx"
output_dir = r"C:\Users\FlorianDemir\Desktop\Business Travel_MGMT\January 2025(WholeYear)"
os.makedirs(output_dir, exist_ok=True)

# Read the data
df = pd.read_excel(data_path)

# Convert columns to float for numerical analysis
for col in ['Total Amount', 'Kg CO2', 'Km Total']:
    df[col] = pd.to_numeric(df[col], errors='coerce')

# 1. Summary by Cost Center
gb_cost_center = df.groupby('Cost Center').agg({
    'Booking Number': 'count',
    'Total Amount': 'sum',
    'Kg CO2': 'sum',
    'Km Total': 'sum'
}).rename(columns={
    'Booking Number': 'Total Travels',
    'Total Amount': 'Total Spend',
    'Kg CO2': 'Total CO2',
    'Km Total': 'Total Km'
})
gb_cost_center = gb_cost_center.sort_values('Total Travels', ascending=False)

# 1b. By Cost Center & Month
gb_cost_center_month = df.groupby(['Cost Center', 'Year', 'Month']).agg({
    'Booking Number': 'count',
    'Total Amount': 'sum',
    'Kg CO2': 'sum',
    'Km Total': 'sum'
}).rename(columns={
    'Booking Number': 'Total Travels',
    'Total Amount': 'Total Spend',
    'Kg CO2': 'Total CO2',
    'Km Total': 'Total Km'
}).reset_index()

# 2. Summary by Time (month, quarter, year)
gb_month = df.groupby(['Year', 'Month']).agg({'Booking Number': 'count', 'Total Amount': 'sum', 'Kg CO2': 'sum'}).reset_index()
gb_quarter = df.groupby(['Year', 'Quarter']).agg({'Booking Number': 'count', 'Total Amount': 'sum', 'Kg CO2': 'sum'}).reset_index()
gb_year = df.groupby(['Year']).agg({'Booking Number': 'count', 'Total Amount': 'sum', 'Kg CO2': 'sum'}).reset_index()

# 3. Most traveled destinations, airlines, hotels
gb_dest = df['Destination Country'].value_counts().head(10)
gb_airline = df['Airline Name'].value_counts().head(10)
gb_hotel = df['Hotel Name'].value_counts().head(10)

# 4. Monthly total CO2
gb_month_co2 = df.groupby(['Year', 'Month'])['Kg CO2'].sum().reset_index()

# 5. Save rows with negative km
neg_km = df[df['Km Total'] < 0]
neg_km.to_excel(os.path.join(output_dir, 'negative_km_rows.xlsx'), index=False)

# 6. Graphs
plt.figure(figsize=(10,6))
gb_cost_center['Total Travels'].plot(kind='bar')
plt.title('Total Travels by Cost Center')
plt.ylabel('Total Travels')
plt.tight_layout()
plt.savefig(os.path.join(output_dir, 'travels_by_cost_center.png'))
plt.close()

plt.figure(figsize=(10,6))
gb_month_co2['CO2_Label'] = gb_month_co2['Year'].astype(str) + '-' + gb_month_co2['Month'].astype(str)
plt.plot(gb_month_co2['CO2_Label'], gb_month_co2['Kg CO2'], marker='o')
plt.title('Monthly Total CO2 Emissions')
plt.ylabel('Kg CO2')
plt.xlabel('Year-Month')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig(os.path.join(output_dir, 'monthly_co2.png'))
plt.close()

# 7. CO2 Exists column and ratio
df['CO2 Exists'] = df['Kg CO2'].apply(lambda x: 'No' if pd.isnull(x) or x == 0 else 'Yes')
co2_missing_ratio = (df['CO2 Exists'] == 'No').sum() / len(df)
print(f"CO2 missing ratio: %{co2_missing_ratio*100:.2f}")

# 7b. Pie chart for CO2 Exists (English labels)
co2_counts = df['CO2 Exists'].value_counts()
labels = ['CO2 Present', 'CO2 Missing']
values = [int(co2_counts.get('Yes', 0) or 0), int(co2_counts.get('No', 0) or 0)]
plt.figure(figsize=(6,6))
plt.pie(values, labels=labels, autopct=lambda pct: f'{pct:.1f}% ({int(pct/100.*sum(values))})', startangle=90, colors=['#4CAF50', '#FF7043'])
plt.title('Ratio of Rows With/Without CO2 Value')
plt.tight_layout()
plt.savefig(os.path.join(output_dir, 'co2_exists_pie_chart.png'))
plt.close()

# 8. Save all summaries to a new Excel file with different sheets
with pd.ExcelWriter(os.path.join(output_dir, 'analysis_summary.xlsx')) as writer:
    gb_cost_center.to_excel(writer, sheet_name='By Cost Center')
    gb_cost_center_month.to_excel(writer, sheet_name='By Cost Center & Month', index=False)
    gb_month.to_excel(writer, sheet_name='By Month', index=False)
    gb_quarter.to_excel(writer, sheet_name='By Quarter', index=False)
    gb_year.to_excel(writer, sheet_name='By Year', index=False)
    gb_dest.to_excel(writer, sheet_name='Top Destinations')
    gb_airline.to_excel(writer, sheet_name='Top Airlines')
    gb_hotel.to_excel(writer, sheet_name='Top Hotels')
    neg_km.to_excel(writer, sheet_name='Negative Km', index=False)
    df[['CO2 Exists']].to_excel(writer, sheet_name='CO2 Exists', index=False)

print(f"Analysis and reports saved to: {output_dir}") 