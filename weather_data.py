import pandas as pd
import sqlite3
from flask import Flask, request, jsonify, abort
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func

# import the data from onedrive where I saved it
file_path = r'C:\Users\jakob\OneDrive\Desktop\weather_data\USC00110072.txt'

# Read the file into a DataFrame
df = pd.read_csv(file_path, delim_whitespace=True, header=None)

# add columns to the data set
df.columns = ['date', 'max_temp', 'min_temp', 'precipitation_amount']

conn = sqlite3.connect('weather.db')

# Write the DataFrame to a table named 'weather_data' in the database
df.to_sql('weather_data', conn, if_exists='replace', index=False)

cursor = conn.cursor()

# Drop the table if it exists
cursor.execute("DROP TABLE IF EXISTS weather_data")

# Create the table
cursor.execute("""
    CREATE TABLE weather_data (
        date TEXT,
        max_temp REAL,
        min_temp REAL,
        precipitation_amount REAL
    )
""")

# Remove duplicate rows in the SQL table inside our newly defined schema

cursor.execute("""
    DELETE FROM weather_data
    WHERE rowid NOT IN (
        SELECT MIN(rowid)
        FROM weather_data
        GROUP BY date, max_temp, min_temp, precipitation_amount
    )
""")

# Commit the changes
conn.commit()

# Create a new table for the aggregated results
cursor.execute("""
    CREATE TABLE IF NOT EXISTS weather_summary AS
    SELECT
        SUBSTR('%Y', date) AS year,
        'USC00110072' AS station_id,
        
        --ignor missing values (-9999) and return null when the statistics cannot be calculated
        CASE WHEN COUNT(max_temp) > 0 THEN AVG(CASE WHEN max_temp != -9999 THEN max_temp ELSE NULL END) ELSE NULL END AS avg_max_temp,
        CASE WHEN COUNT(min_temp) > 0 THEN AVG(CASE WHEN min_temp != -9999 THEN min_temp ELSE NULL END) ELSE NULL END AS avg_min_temp,
        CASE WHEN COUNT(precipitation_amount) > 0 THEN SUM(CASE WHEN precipitation_amount != -9999 THEN precipitation_amount / 10.0 ELSE NULL END) ELSE NULL END AS total_precipitation_cm
    FROM weather_data
    GROUP BY year, station_id
""")

# Commit the changes
conn.commit()

# Read the summary table back into a DataFrame to confirm the results
summary_df = pd.read_sql_query("SELECT * FROM weather_summary", conn)
print(summary_df.head())

# Close the connection
conn.close()

print("Data has been summarized and written to weather_summary in weather.db")

###----------------------------API CONFIGURATION---------------------------------###

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///weather.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# Define SQLAlchemy model
class WeatherData(db.Model):
    __tablename__ = 'weather_data'
    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.Text)
    max_temp = db.Column(db.Float)
    min_temp = db.Column(db.Float)
    precipitation_amount = db.Column(db.Float)

# API endpoints
@app.route('/api/weather', methods=['GET'])
def get_weather():
    # Parse query parameters
    date_filter = request.args.get('date')
    station_id = request.args.get('station_id')
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 10))

    # Base query
    query = WeatherData.query

    # Apply filters if present
    if date_filter:
        query = query.filter(WeatherData.date == date_filter)
    if station_id:
        query = query.filter(WeatherData.station_id == station_id)

    # Pagination
    paginated_query = query.paginate(page=page, per_page=per_page, error_out=False)
    weather_data = paginated_query.items

    # Serialize to JSON
    result = []
    for data in weather_data:
        result.append({
            'date': data.date,
            'max_temp': data.max_temp,
            'min_temp': data.min_temp,
            'precipitation_amount': data.precipitation_amount
        })

    # Return JSON response
    return jsonify({
        'weather_data': result,
        'page': paginated_query.page,
        'per_page': paginated_query.per_page,
        'total_pages': paginated_query.pages,
        'total_items': paginated_query.total
    })

@app.route('/api/weather/stats', methods=['GET'])
def get_weather_stats():
    # Parse query parameters
    date_filter = request.args.get('date')
    station_id = request.args.get('station_id')

    # Base query
    query = db.session.query(
        func.strftime('%Y', WeatherData.date).label('year'),
        'USC00110072'.label('station_id'),
        func.avg(WeatherData.max_temp).label('avg_max_temp'),
        func.avg(WeatherData.min_temp).label('avg_min_temp'),
        func.sum(WeatherData.precipitation_amount / 10.0).label('total_precipitation_cm')
    ).filter(WeatherData.max_temp != -9999, WeatherData.min_temp != -9999, WeatherData.precipitation_amount != -9999)

    # Apply filters if present
    if date_filter:
        query = query.filter(func.strftime('%Y', WeatherData.date) == date_filter)
    if station_id:
        query = query.filter(WeatherData.station_id == station_id)

    # Execute query
    stats_data = query.group_by('year', 'station_id').all()

    # Serialize to JSON
    result = []
    for stats in stats_data:
        result.append({
            'year': stats.year,
            'station_id': stats.station_id,
            'avg_max_temp': stats.avg_max_temp,
            'avg_min_temp': stats.avg_min_temp,
            'total_precipitation_cm': stats.total_precipitation_cm
        })

    # Return JSON response
    return jsonify({'weather_stats': result})

if __name__ == '__main__':
    app.run(debug=True)





