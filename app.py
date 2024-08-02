from flask import Flask, render_template, request
import singlestoredb as s2
import time
import pandas as pd
import json
import plotly
import plotly.express as px

app = Flask(__name__)

# Database connection parameters
DB_PARAMS = {
    'host': 'svc-5dca9fc8-714e-47ab-b2f1-1171a1d7bbcf-dml.aws-virginia-5.svc.singlestore.com',
    'port': 3306,
    'user': 'admin',
    'password': 'SingleStore!3',
    'database': 'nyc_taxi'
}

# Optimized queries
queries = {
    "Top 10% Earnings": {
        "query": """
        WITH earnings_ranked AS (
            SELECT 
            VendorID, FROM_UNIXTIME(tpep_pickup_datetime / 1000000) as tpep_pickup_datetime, FROM_UNIXTIME(tpep_dropoff_datetime / 1000000) as tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, 
            store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, 
            improvement_surcharge, total_amount, congestion_surcharge, Airport_fee,
            PERCENT_RANK() OVER (ORDER BY (fare_amount + tip_amount) DESC) AS percentile
            from yellow_tripdata_pl_new
        )
        SELECT *
        FROM earnings_ranked
        WHERE percentile <= 0.1
        LIMIT 50;
        """,
        "explanation": "This analysis identifies the top 10% of taxi rides by total earnings. Understanding these high-value trips can help optimize resource allocation and marketing strategies for premium services."
    },
    "Payment Type Dynamics": {
        "query": """
        SELECT 
            payment_type,
            AVG(tip_amount) AS avg_tip,
            AVG(tip_amount / fare_amount) AS avg_tip_percentage,
            COUNT(*) AS trip_count
        FROM yellow_tripdata_pl_new
        GROUP BY payment_type
        ORDER BY avg_tip DESC;
        """,
        "explanation": "By comparing average tip amounts and percentages across different payment methods, we can identify trends that may inform decisions on payment system upgrades or driver incentive programs."
    },
    "Dynamic Pricing Analysis": {
        "query": """
        SELECT 
            RatecodeID,
            DAYOFWEEK(FROM_UNIXTIME(tpep_pickup_datetime / 1000000)) AS day_of_week,
            HOUR(FROM_UNIXTIME(tpep_pickup_datetime / 1000000)) AS hour_of_day,
            AVG(fare_amount / NULLIF(trip_distance, 0)) AS avg_fare_per_mile
        FROM yellow_tripdata_pl_new
        WHERE trip_distance > 0 and RatecodeID is not NULL
        GROUP BY RatecodeID, day_of_week, hour_of_day
        ORDER BY RatecodeID, day_of_week, hour_of_day;
        """,
        "explanation": "This analysis breaks down the average fare per mile based on rate code, day of the week, and hour. It can help in developing dynamic pricing strategies and identifying the most profitable times and zones for drivers."
    },
    "Busiest Hours": {
        "query": """
        SELECT 
            DAYOFWEEK(FROM_UNIXTIME(tpep_pickup_datetime / 1000000)) AS day_of_week,
            HOUR(FROM_UNIXTIME(tpep_pickup_datetime / 1000000)) AS hour_of_day,
            COUNT(*) AS total_trips
        FROM yellow_tripdata_pl_new
        GROUP BY day_of_week, hour_of_day
        ORDER BY total_trips DESC
        LIMIT 5;
        """,
        "explanation": "Identifying the top 5 busiest hours of the week allows for better resource allocation, surge pricing implementation, and driver shift planning to meet peak demand efficiently."
    },
    "Shortest Path": {
        "query": """
        SELECT 
            PULocationID, 
            DOLocationID, 
            MIN(trip_distance) AS shortest_distance,
            AVG(trip_distance) AS avg_distance,
            COUNT(*) AS trip_count
        FROM yellow_tripdata_pl_new
        GROUP BY PULocationID, DOLocationID
        ORDER BY shortest_distance ASC
        LIMIT 1;
        """,
        "explanation": "Finding the shortest path between locations can optimize route planning, reduce operational costs, and improve customer satisfaction by minimizing travel time and distance."
    }
}

def execute_query(query):
    connection = s2.connect(**DB_PARAMS)
    cursor = connection.cursor()
    
    start_time = time.time()
    cursor.execute(query)
    result = cursor.fetchall()
    end_time = time.time()
    
    column_names = [desc[0] for desc in cursor.description]
    
    cursor.close()
    connection.close()
    
    execution_time = round(end_time - start_time, 2)
    return result, column_names, execution_time

def create_plot(df, x, y, title):
    fig = px.bar(df, x=x, y=y, title=title)
    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

@app.route('/')
def index():
    return render_template('index.html', queries=queries.keys())

@app.route('/query', methods=['POST'])
def query():
    selected_query = request.form['query']
    query_details = queries[selected_query]
    query = query_details['query']
    explanation = query_details['explanation']
    
    try:
        result, column_names, execution_time = execute_query(query)
        
        # Convert result to DataFrame for easier manipulation
        df = pd.DataFrame(result, columns=column_names)
        
        # Create a plot based on the query type
        plot = None
        if selected_query == "Payment Type Dynamics":
            plot = create_plot(df, x='payment_type', y='avg_tip', title='Average Tip by Payment Type')
        elif selected_query == "Busiest Hours":
            plot = create_plot(df, x='hour_of_day', y='total_trips', title='Busiest Hours of the Week')
        
        return render_template('index.html', 
                               queries=queries.keys(), 
                               selected_query=selected_query, 
                               query=query, 
                               result=df.to_dict('records'), 
                               column_names=column_names, 
                               explanation=explanation, 
                               execution_time=execution_time,
                               plot=plot)
    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        return render_template('index.html', 
                               queries=queries.keys(), 
                               selected_query=selected_query, 
                               query=query, 
                               error=error_message)

if __name__ == '__main__':
    app.run(debug=True)