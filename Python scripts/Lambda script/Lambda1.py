import json
import pandas as pd
import boto3
from sodapy import Socrata


def lambda_handler(event, context):
    # Initialize Socrata client
    client = Socrata("data.cityofnewyork.us", None)
    
    # Retrieve data
    results = client.get("ssq6-fkht", limit=400618)
    
    # Convert results to DataFrame
    results_df = pd.DataFrame.from_records(results)
    #results_df = pd.DataFrame({'a':[1,2,3],'b':[2,3,4]})
    print(results_df)
    
    # Convert DataFrame to CSV format (you can choose other formats if needed)
    csv_data = results_df.to_csv(index=False)
    
    # Initialize S3 client
    s3 = boto3.client('s3')
    
    # Upload CSV data to S3
    bucket_name = 'zone1rawdata'
    key = 'api2.csv'  # Name of the file in S3
    s3.put_object(Bucket=bucket_name, Key=key, Body=csv_data)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Data saved to S3 successfully!')
    }

