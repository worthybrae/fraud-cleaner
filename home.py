import streamlit as st
import pandas as pd
from haversine import haversine, Unit
import hashlib
import geohash


def hash_device_id(device_id):
    return hashlib.sha256(device_id.encode()).hexdigest()

def calculate_truncation(val):
    digits = len(str(val))
    if -180 <= val <= -100:
        return digits - 5
    elif -100 < val <= -10:
        return digits - 4
    elif -10 < val < 0:
        return digits - 3
    elif 0 <= val < 10:
        return digits - 2
    elif 10 <= val < 100:
        return digits - 3
    elif 100 <= val <= 180:
        return digits - 4

def calculate_geohash(lat, lon, precision=8):
    return geohash.encode(lat, lon, precision=precision)

# Function to calculate haversine distance
def calculate_haversine_distance(lat1, lon1, lat2, lon2):
    try:
        point1 = (lat1, lon1)
        point2 = (lat2, lon2)
        return haversine(point1, point2, unit=Unit.METERS)
    except:
        return 0
    
st.title('data cleaner')

# Upload the file
uploaded_file = st.file_uploader("upload results file", type=["csv", "xlsx"])

on = st.toggle("hash device ids")

if st.button('analyze'):
    # Read the file into a dataframe
    with st.spinner('reading csv file...'):
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(uploaded_file)
        elif uploaded_file.name.endswith('.xlsx'):
            df = pd.read_excel(uploaded_file)

    with st.spinner('creating new columns...'):
        df['id'] = df['id'].str.lower()
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['truncated_timestamp_minute'] = df['timestamp'].dt.floor('min')

    with st.spinner('analyzing device behavior...'):
        if 'supply_id' in df.columns:
            # Group by id and truncated timestamp value to get the required aggregations
            grouped_df = df.groupby(['id', 'truncated_timestamp_minute']).agg(
                min_latitude=pd.NamedAgg(column='latitude', aggfunc='min'),
                max_latitude=pd.NamedAgg(column='latitude', aggfunc='max'),
                mode_latitude=pd.NamedAgg(column='latitude', aggfunc=lambda x: x.mode()[0]),
                min_longitude=pd.NamedAgg(column='longitude', aggfunc='min'),
                max_longitude=pd.NamedAgg(column='longitude', aggfunc='max'),
                mode_longitude=pd.NamedAgg(column='longitude', aggfunc=lambda x: x.mode()[0]),
                supply_id_list=pd.NamedAgg(column='supply_id', aggfunc=lambda x: ','.join(x.str[:9].unique())),
                has_ip=pd.NamedAgg(column='ip_address', aggfunc=lambda x: any(x.notnull()))
            ).reset_index()
        else:
            grouped_df = df.groupby(['id', 'truncated_timestamp_minute']).agg(
                min_latitude=pd.NamedAgg(column='latitude', aggfunc='min'),
                max_latitude=pd.NamedAgg(column='latitude', aggfunc='max'),
                mode_latitude=pd.NamedAgg(column='latitude', aggfunc=lambda x: x.mode()[0]),
                min_longitude=pd.NamedAgg(column='longitude', aggfunc='min'),
                max_longitude=pd.NamedAgg(column='longitude', aggfunc='max'),
                mode_longitude=pd.NamedAgg(column='longitude', aggfunc=lambda x: x.mode()[0]),
                has_ip=pd.NamedAgg(column='ip_address', aggfunc=lambda x: any(x.notnull()))
            ).reset_index()

    with st.spinner('aggregating device insights...'):
        grouped_df['geohash'] = grouped_df.apply(lambda row: calculate_geohash(row.mode_latitude, row.mode_longitude), axis=1)
        grouped_df['latitude_truncation'] = grouped_df.apply(lambda row: calculate_truncation(row.mode_latitude), axis=1)
        grouped_df['longitude_truncation'] = grouped_df.apply(lambda row: calculate_truncation(row.mode_longitude), axis=1)
        grouped_df['latitude_truncated'] = grouped_df['mode_latitude'].apply(lambda x: float(f"{x:.4f}"))
        grouped_df['longitude_truncated'] = grouped_df['mode_longitude'].apply(lambda x: float(f"{x:.4f}"))
        grouped_df['time_truncated_timestamp'] = grouped_df['truncated_timestamp_minute'].dt.time

        # Calculate the distance traveled in that minute
        grouped_df['travel_range'] = grouped_df.apply(
            lambda row: calculate_haversine_distance(row['min_latitude'], row['min_longitude'],
                                                    row['max_latitude'], row['max_longitude']), axis=1)
        
        grouped_df['is_teleporting'] = grouped_df.apply(lambda x: x['travel_range'] > 20, axis=1)

        grouped_df['replay_hash'] = grouped_df.apply(lambda row: hashlib.md5(str(row[['time_truncated_timestamp', 'latitude_truncated', 'longitude_truncated']].values).encode()).hexdigest(), axis=1)
        grouped_df['is_replay'] = grouped_df.duplicated(subset=['replay_hash'], keep='first')

        clusters_df = pd.read_csv('clusters.csv', header=None)
        clusters_list = clusters_df[0].tolist()

        grouped_df['geohash_8'] = grouped_df['geohash'].str[:8]

        # Check if the left eight characters are in the list from clusters.csv
        grouped_df['is_stacking'] = grouped_df['geohash_8'].isin(clusters_list)

        # New column for latitude and longitude truncation check
        grouped_df['is_ip_derived'] = (grouped_df['latitude_truncation'] == 4) & \
                                             (grouped_df['longitude_truncation'] == 4) & \
                                             (grouped_df['has_ip'] == True)    

        new_column_names = {
            'truncated_timestamp_minute': 'timestamp',
            'mode_latitude': 'latitude',
            'mode_longitude': 'longitude'
        }

        # Rename columns
        grouped_df.rename(columns=new_column_names, inplace=True)
        grouped_df = grouped_df.sort_values(by=['id', 'timestamp'])

        # Add previous latitude and longitude
        grouped_df['prev_latitude'] = grouped_df.groupby('id')['latitude'].shift(1)
        grouped_df['prev_longitude'] = grouped_df.groupby('id')['longitude'].shift(1)
        grouped_df['prev_timestamp'] = grouped_df.groupby('id')['timestamp'].shift(1)

        grouped_df['time_diff_minutes'] = (grouped_df['timestamp'] - grouped_df['prev_timestamp']).dt.total_seconds() / 60

        # Calculate the distance in km
        grouped_df['distance_km'] = grouped_df.apply(
            lambda row: calculate_haversine_distance(
                row['prev_latitude'], 
                row['prev_longitude'], 
                row['latitude'], 
                row['longitude']
            ) if pd.notnull(row['prev_latitude']) and pd.notnull(row['prev_longitude']) else 0, axis=1)

        # Calculate speed in km/min
        grouped_df['speed_kmpm'] = grouped_df['distance_km'] / grouped_df['time_diff_minutes']

        grouped_df['is_teleporting'] = grouped_df['speed_kmpm'] > 20

        # Update is_legit based on is_teleporting and other conditions
        grouped_df['is_legit'] = ~(grouped_df['is_ip_derived'] | grouped_df['is_stacking'] | grouped_df['is_replay'] | grouped_df['is_teleporting'])
        
        grouped_df = grouped_df[['id', 'latitude', 'longitude', 'timestamp', 'prev_latitude', 'prev_longitude', 'prev_timestamp', 'is_replay', 'is_stacking', 'is_teleporting', 'is_ip_derived', 'is_legit']]

        if on:
            grouped_df['id'] = grouped_df['id'].apply(hash_device_id)

        filtered_df = grouped_df[grouped_df['is_legit']]
        bad_df = grouped_df[grouped_df['is_legit'] == False]

        filtered_df['prev_latitude'] = filtered_df.groupby('id')['latitude'].shift(1)
        filtered_df['prev_longitude'] = filtered_df.groupby('id')['longitude'].shift(1)
        filtered_df['prev_timestamp'] = filtered_df.groupby('id')['timestamp'].shift(1)

        bad_df['prev_latitude'] = bad_df.groupby('id')['latitude'].shift(1)
        bad_df['prev_longitude'] = bad_df.groupby('id')['longitude'].shift(1)
        bad_df['prev_timestamp'] = bad_df.groupby('id')['timestamp'].shift(1)

        # Convert DataFrame to CSV
        csv = filtered_df.to_csv(index=False) 
        bad_csv = bad_df.to_csv(index=False)   

    # Display the grouped data
    st.dataframe(grouped_df)

    # Download button
    st.download_button(
        label="get clean data",
        data=csv,
        file_name='filtered_data.csv',
        mime='text/csv'
    )

    st.download_button(
        label="get bad data",
        data=bad_csv,
        file_name='filtered_data.csv',
        mime='text/csv'
    )



                    

            
                
