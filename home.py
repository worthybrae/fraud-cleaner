import streamlit as st
import re
from helpers.snowflake import query_snowflake
import pandas as pd


# Function to check UUID format
def is_valid_uuid(uuid_to_test, version=4):
    regex = f"^[a-f0-9]{{8}}-?[a-f0-9]{{4}}-?{version}[a-f0-9]{{3}}-?[89ab][a-f0-9]{{3}}-?[a-f0-9]{{12}}$"
    match = re.fullmatch(regex, uuid_to_test)
    return bool(match)

st.title('elvira lite')

if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False


password = st.text_input("password", type="password")
if password:
    # Check if the password is correct
    if password == st.secrets["password"]:
        st.session_state.authenticated = True
        st.success("password accepted!")
    else:
        st.error("incorrect password")


# Check if the password is correct
if password == st.secrets["password"]:
    id_input = st.text_input("id")

    # UUID format validation
    if id_input and not is_valid_uuid(id_input):
        st.error("invalid id format")

    # Toggle for hashed device ID
    is_hashed = st.checkbox("hashed id?")

    # Clean button
    if st.button("clean"):
        with st.spinner('cleaning data...'):
            if is_hashed:
                id_field = "md5_hex(lower(idfa)) as id"
            else:
                id_field = "lower(idfa) as id"
            query = f"""
                with all_data as (
                    select
                        supply_id,
                        {id_field},
                        date_trunc('minute', timestamp) as datetime,
                        left(geohash, 8) as geo,
                        derived_country as country,
                        latitude,
                        longitude,
                        created_at,
                        case 
                            when position('.', to_char(latitude)) > 0 
                            then length(substr(to_char(latitude), position('.', to_char(latitude)) + 1))
                            else 0
                        end as latitude_truncation,
                        case 
                            when position('.', to_char(longitude)) > 0 
                            then length(substr(to_char(longitude), position('.', to_char(longitude)) + 1))
                            else 0
                        end as longitude_truncation
                    from
                        singularity.public.h4_maid_clustered
                    where 
                        (idfa like '{id_input[:3].lower()}%' or idfa like '{id_input[:3].upper()}%') and
                        lower(idfa) = '{id_input.lower()}' and
                        horizontal_accuracy < 2000 and 
                        latitude != longitude and 
                        idfa != '00000000-0000-0000-0000-000000000000' and
                        latitude_truncation > 2 and 
                        longitude_truncation > 2
                ), clusters as (
                    select 
                        specific_geo 
                    from 
                        datapillar.automated.cluster_geohashes
                ), grouped_data as (
                    select
                        supply_id,
                        id,
                        datetime,
                        geo,
                        country,
                        created_at,
                        first_value(latitude) over (partition by id, datetime order by created_at, latitude, longitude) as main_latitude,
                        first_value(longitude) over (partition by id, datetime order by created_at, latitude, longitude) as main_longitude,
                        first_value(geo) over (partition by id, datetime order by created_at, latitude, longitude) as main_geo,
                        rank() over (partition by id, datetime order by created_at, latitude, longitude) as ingest_order,
                        min(latitude) over (partition by id, datetime) as min_lt,
                        min(longitude) over (partition by id, datetime) as min_ln,
                        max(latitude) over (partition by id, datetime) as max_lt,
                        max(longitude) over (partition by id, datetime) as max_ln,
                        case when haversine(min_lt, min_ln, max_lt, max_ln) > 5 then 1 else 0 end as is_conflicted,
                        case when ingest_order > 1 then 1 else 0 end as is_late,
                        case when geo in (select specific_geo from clusters) then 1 else 0 end as is_stacked
                    from
                        all_data
                ), enriched_data as (
                    select
                        supply_id,
                        id,
                        datetime,
                        country,
                        created_at,
                        main_latitude,
                        main_longitude,
                        is_conflicted,
                        is_late,
                        is_stacked,
                        md5_hex(concat(id, dayofweek(datetime), mod(date_part('minute', datetime), 15), trunc(main_latitude, 4), trunc(main_longitude, 4))) as replay_hash,
                        md5_hex(concat(id, supply_id, date_trunc('week', datetime), left(geo, 2))) as infection_hash
                    from 
                        grouped_data
                ), se_data as (
                    select
                        supply_id,
                        id,
                        datetime,
                        country,
                        created_at,
                        main_latitude,
                        main_longitude,
                        is_conflicted,
                        is_late,
                        is_stacked,
                        replay_hash,
                        infection_hash,
                        rank() over (partition by id, replay_hash order by datetime, created_at) as replay_order
                    from
                        enriched_data
                ), see_data as (
                    select
                        supply_id,
                        id,
                        datetime,
                        country,
                        created_at,
                        main_latitude,
                        main_longitude,
                        is_conflicted,
                        is_late,
                        is_stacked,
                        infection_hash,
                        lag(datetime) over (partition by id, replay_hash order by replay_order) as prev_replay_datetime,
                        datediff('days', prev_replay_datetime, datetime) as day_diff,
                        timediff('minutes', to_time(prev_replay_datetime), to_time(datetime)) as min_diff,
                        case 
                            when day_diff = 0 and min_diff = 0 then 1 
                            else 0 
                        end as is_dupe,
                        case 
                            when mod(day_diff, 7) = 0 and mod(min_diff, 45) = 0 and day_diff != 0 then 1 
                            when mod(day_diff, 7) = 0 and min_diff = 1 then 1 
                            when day_diff = 1 and mod(min_diff, 61) = 0 then 1
                            else 0 
                        end as is_replay
                    from
                        se_data
                ), seee_data as (
                    select
                        supply_id,
                        id,
                        datetime,
                        country,
                        created_at,
                        main_latitude,
                        main_longitude,
                        is_conflicted,
                        is_late,
                        is_stacked,
                        is_dupe,
                        is_replay,
                        max(is_replay) over (partition by infection_hash) as is_infected
                    from
                        see_data
                ), enr_data as (
                    select
                        supply_id,
                        id,
                        datetime,
                        country,
                        created_at,
                        main_latitude,
                        main_longitude,
                        is_conflicted,
                        is_late,
                        is_stacked,
                        is_dupe,
                        is_replay,
                        is_infected,
                        lag(main_latitude) over (partition by id order by datetime) as prev_latitude,
                        lag(main_longitude) over (partition by id order by datetime) as prev_longitude,
                        lag(datetime) over (partition by id  order by datetime) as prev_datetime,
                        haversine(prev_latitude, prev_longitude, main_latitude, main_longitude) AS dist_travelled,
                        timediff('minutes', prev_datetime, datetime) as min_dif,
                        dist_travelled / case when min_dif = 0 then 1 else min_dif end as km_per_min
                    from
                        seee_data
                ), enri_data as (
                    select
                        supply_id,
                        id,
                        datetime,
                        country,
                        created_at,
                        main_latitude,
                        main_longitude,
                        is_conflicted,
                        is_late,
                        is_stacked,
                        is_dupe,
                        is_replay,
                        is_infected,
                        dist_travelled,
                        km_per_min,
                        case 
                            when dist_travelled > 50 then 1 
                            else 0 
                        end as travel_flag,
                        sum(travel_flag) over (partition by id order by datetime rows between unbounded preceding and current row) as travel_segment
                    from
                        enr_data
                ), enric_data as (
                    select
                        supply_id,
                        id,
                        datetime,
                        main_latitude,
                        main_longitude,
                        country,
                        created_at,
                        is_conflicted,
                        is_late,
                        is_stacked,
                        is_dupe,
                        is_replay,
                        is_infected,
                        count(1) over (partition by id, travel_segment) as total_seg_locs,
                        min(datetime) over (partition by id, travel_segment) as seg_min,
                        max(datetime) over (partition by id, travel_segment) as seg_max,
                        timediff('minutes', seg_min, seg_max) as min_range,
                        max(km_per_min) over (partition by id, travel_segment) as max_speed,
                        max(dist_travelled) over (partition by id, travel_segment) as max_dist,
                        case when (max_speed < 25 or min_range > 120) and total_seg_locs > 10 then 0 else 1 end as is_teleporting
                    from
                        enri_data
                ), hui_data as (
                    select
                        supply_id,
                        id,
                        datetime,
                        main_latitude,
                        main_longitude,
                        country,
                        created_at,
                    from
                        enric_data
                    where
                        is_conflicted = 0 and
                        is_late = 0 and
                        is_stacked = 0 and
                        is_dupe = 0 and
                        is_replay = 0 and
                        is_infected = 0 and
                        is_teleporting = 0
                )
                select
                    *,
                    lag(main_latitude) over (partition by id order by datetime) as prev_latitude,
                    lag(main_longitude) over (partition by id order by datetime) as prev_longitude,
                from
                    hui_data
                order by
                    datetime
            """
            results = query_snowflake(
                query, 
                st.secrets["snowflake_username"],
                st.secrets["snowflake_password"],
                st.secrets["snowflake_account"],
                st.secrets["snowflake_warehouse_lg"],
                st.secrets["snowflake_database"],
                st.secrets["snowflake_schema"],
            )
        results_df = pd.DataFrame(results[1:], columns=results[0])
        st.dataframe(results_df)
        results_csv = results_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="clean data",
            data=results_csv,
            file_name='cleaned_data.csv',
            mime='text/csv',
        )


else:
    st.error("incorrect password")
