import snowflake.connector
import os
import time
import streamlit as st


# Establish a connection to Snowflake
def create_connection(user, password, account, wh, database, schema):
    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=wh,
        database=database,
        schema=schema
    )
    print(conn)
    return conn

def check_query_status(connection, query_id):
    cursor = connection.cursor()
    try:
        query = f"""
            select
                execution_status,
                compilation_time / 1000,
                execution_time / 1000
            from
                table(snowflake.information_schema.query_history())
            where
                query_id = '{query_id}'
        """
        cursor.execute(f"use warehouse pinger")
        cursor.execute(query)
        result = cursor.fetchone()
        if result:
            return {"status": result[0], "compilation_time": result[1], "execution_time": result[2]}
        else:
            raise ValueError("No results found for the given query ID.")
    except Exception as e:
        raise ValueError(f"An error occurred while checking the query status: {e}")
    finally:
        cursor.close()

def fetch_results_from_query_id(conn, query_id):
    cursor = conn.cursor()
    try:
        # Fetch results based on the query_id
        cursor.execute(f"use warehouse cruncher")
        cursor.execute(f"SELECT * FROM TABLE(RESULT_SCAN('{query_id}'))")
        columns = [desc[0] for desc in cursor.description]
        results = cursor.fetchall()

        # Check if results are empty and handle it
        if not results:
            # print("No results found for the provided query_id.")
            return None

        results_with_headers = [columns] + results
        return results_with_headers
    except Exception as e:
        # print(f"Error fetching results: {e}")
        return None
    finally:
        cursor.close()

# Submit a query to Snowflake and fetch results
def query_snowflake(query, user, password, account, wh, database, schema, delay=15, warehouse='snowflake_warehouse_lg', execute_async=True):
    # print('Connecting to Snowflake...')
    conn = create_connection(user, password, account, wh, database, schema)
    # print('A connection to Snowflake has been created!')
    cursor = conn.cursor()
    results = None

    # print('Executing Snowflake query...\n')
    # Execute the query in asynchronous mode
    warehouse_selection = f"use warehouse cruncher"
    cursor.execute(warehouse_selection)
    if execute_async:
        cursor.execute(query, _no_results=True)
    else:
        cursor.execute(query)
    start = time.time()
    query_id = cursor.sfqid
    time.sleep(delay)

    while True:
        query_info = check_query_status(conn, query_id)
        status = query_info["status"]
        if status == "SUCCESS":
            results = fetch_results_from_query_id(conn, query_id)  
            # print("Query Completed!")
            break
        elif status in ["FAILED_WITH_ERROR", "ABORTED"]:
            # print(f"Query {status}")
            break
        else:
            # Display compilation and execution times
            if query_info["execution_time"] > 1:
                # Convert milliseconds to seconds
                # print(f"\rThe query has taken {convert_seconds_to_min_sec(query_info['execution_time'])} to execute...", end='', flush=True)
                pass
            elif query_info["compilation_time"] > 1:
                # print(f"\rThe query has taken {convert_seconds_to_min_sec(query_info['compilation_time'])} to compile...", end='', flush=True)
                pass
            else:
                end = time.time()
                # print(f"\rThe query has taken {convert_seconds_to_min_sec(end - start)} to compile...", end='', flush=True)
                pass
            time.sleep(delay)
        current_time = time.time()
        if current_time - start > 3600:
            cancel_query = f"SELECT SYSTEM$CANCEL_QUERY('{query_id}');"
            cursor.execute(cancel_query)
            break

    '''except Exception as e:
        # print(f"An error occurred: {e}")
        pass
    finally:
        # print('Closing cursor and connection...')
        cursor.close()
        conn.close()'''
    
    return results
