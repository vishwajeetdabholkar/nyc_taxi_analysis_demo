from flask import Flask, render_template, request
import time
import pandas as pd
import json
import plotly
import plotly.express as px
import requests

app = Flask(__name__)

# Queries to be executed in Druid
queries = {
    "Top 10% Earnings": {
        "query": """
            WITH earnings AS (
                SELECT 
                    (fare_amount + tip_amount) AS total_earnings,
                    ROW_NUMBER() OVER (ORDER BY fare_amount + tip_amount DESC) AS earnings_rank,
                    COUNT(*) AS total_count
                FROM "trips_xaa"
                GROUP BY fare_amount + tip_amount  
            )
            SELECT *
            FROM earnings
            WHERE earnings_rank <= FLOOR(0.1 * total_count)
            LIMIT 10
        """,
        "explanation": "This analysis identifies the top 10% of taxi rides by total earnings, considering both fare_amount and tip_amount."
    },
    "Payment Type Dynamics": {
        "query": """
                SELECT
                'Credit Card' AS payment_type_1,
                CASE WHEN p2.payment_type = 1 THEN 'Credit card' WHEN p2.payment_type = 2 THEN 'Cash' WHEN p2.payment_type = 3 THEN 'No charge' WHEN p2.payment_type = 4 THEN 'Dispute' WHEN p2.payment_type = 5 THEN 'Unknown' WHEN p2.payment_type = 6 THEN 'Voided trip' END AS payment_type_2,
                p1.avg_tip_amount AS avg_tip_amount_1,
                p2.avg_tip_amount AS avg_tip_amount_2,
                ABS(p1.avg_tip_amount - p2.avg_tip_amount) AS difference,
                ((p1.avg_tip_amount - p2.avg_tip_amount) / p1.avg_tip_amount * 100) AS percent_change,
                CASE WHEN ((p1.avg_tip_amount - p2.avg_tip_amount) / p1.avg_tip_amount * 100) > 50 THEN 'Significantly Higher' WHEN p1.avg_tip_amount < p2.avg_tip_amount THEN 'Significantly Lower' ELSE 'No Significant Difference' END AS comparison_with_other_payment_types
                FROM (
                SELECT
                    payment_type,
                    AVG(tip_amount) AS avg_tip_amount
                FROM "trips_xaa"
                WHERE payment_type IN (1, 2, 3, 4, 5, 6)
                GROUP BY payment_type
                ) AS p1
                CROSS JOIN (
                SELECT
                    payment_type,
                    AVG(tip_amount) AS avg_tip_amount
                FROM "trips_xaa"
                WHERE payment_type IN (1, 2, 3, 4, 5, 6)
                GROUP BY payment_type
                ) AS p2
                WHERE p1.payment_type = 1
        """,
        "explanation": "This query compares average tip amounts across different payment types. It calculates the difference and percent change compared to credit card tips."
    },
    "Dynamic Pricing Analysis": {
        "query": """
                SELECT RatecodeID,
                CASE WHEN int_dy=1 THEN 'Mon' 
                            WHEN int_dy=2 THEN 'Tue' 
                            WHEN int_dy=3 THEN 'Wed'
                            WHEN int_dy=4 THEN 'Thu'
                            WHEN int_dy=5 THEN 'Fri'
                            WHEN int_dy=6 THEN 'Sat'
                            WHEN int_dy=7 THEN 'Sun' 
                            END AS wk_dy,
                "Hour",
                avg_fare_per_mile
                FROM (
                SELECT 
                "rate_code_id" as  RatecodeID,
                    TIME_EXTRACT(__time, 'DOW') AS "int_dy",
                    TIME_EXTRACT(__time, 'HOUR') AS "Hour",
                    SUM(fare_amount) / SUM(trip_distance) AS avg_fare_per_mile
                FROM 
                    "trips_xaa"
                    where "rate_code_id" is not NULL
                GROUP BY 
                    "rate_code_id", TIME_EXTRACT(__time, 'DOW'), TIME_EXTRACT(__time, 'HOUR') 
                    )
                    LIMIT 10           
        """,
        "explanation": "This analysis breaks down the average fare per mile based on rate code, day of the week, and hour. It includes a CASE statement to display day names."
    },
    "Busiest Hours": {
        "query": """
        SELECT
            TIME_EXTRACT("__time", 'HOUR') AS hour_of_day,
            COUNT("trip_id") AS total_trips
        FROM "trips_xaa"
        GROUP BY TIME_EXTRACT("__time", 'HOUR') 
        ORDER BY COUNT("trip_id") DESC
        LIMIT 5
        """,
        "explanation": "This query identifies the top 5 busiest hours of the week."
    },
    # "Shortest Path": {
    #     "query": """
        
    #     """,
    #     "explanation": "This query finds the shortest path between locations."
    # }
}

def execute_query(query):
    url = "http://localhost:8888/druid/v2/sql"
    query = query.strip()
    payload = {
        "query": query,
        "context": {
            "enableWindowing": True  # Add this context to enable window functions
        }
    }
    try:
        start_time = time.time()
        # Send the request to Druid
        response = requests.post(url, json=payload)

        # Check if the request was successful
        if response.status_code == 200:
            # Parse the JSON response
            results = response.json()

            # Create a Pandas DataFrame from the results
            df = pd.json_normalize(results)
        else:
            raise Exception(f"Error with query: {response.status_code} - {response.text}")

    except requests.exceptions.RequestException as e:
        raise Exception(f"An error occurred: {e}")
    
    end_time = time.time()
    execution_time = round(end_time - start_time, 2)
    return df, df.columns.tolist(), execution_time

def create_plot(df, x, y, title):
    fig = px.bar(df, x=x, y=y, title=title)
    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

@app.route('/')
def index():
    return render_template('index.html', queries=queries.keys())

@app.route('/query', methods=['POST'])
def query():
    selected_query = request.form.get('query', None)

    if selected_query is None or selected_query not in queries:
        error_message = "Invalid or missing query selection"
        return render_template('index.html', 
                               queries=queries.keys(), 
                               error=error_message)

    query_details = queries[selected_query]
    query = query_details['query']
    explanation = query_details['explanation']
    
    try:
        # Execute the query and retrieve the result
        result, column_names, execution_time = execute_query(query)
        
        # Convert result to a list of dictionaries (always ensure it's a valid list)
        result_dict = result.to_dict('records') if not result.empty else []

        # Create a plot based on the query type (only if result contains the necessary data)
        plot = None
        if selected_query == "Payment Type Dynamics":
            if 'payment_type' in result.columns and 'avg_tip_amount_1' in result.columns:
                plot = create_plot(result, x='payment_type', y='avg_tip_amount_1', title='Average Tip by Payment Type')
        elif selected_query == "Busiest Hours":
            if 'hour_of_day' in result.columns and 'total_trips' in result.columns:
                plot = create_plot(result, x='hour_of_day', y='total_trips', title='Busiest Hours of the Week')

        return render_template('index.html', 
                               queries=queries.keys(), 
                               selected_query=selected_query, 
                               query=query, 
                               result=result_dict,  # Always pass the result as a list of dicts
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

