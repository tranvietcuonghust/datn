from flask import Flask, render_template
from google.cloud import bigquery
import plotly.graph_objects as go
import plotly.io as pio

app = Flask(__name__)

def get_data(query):
    client = bigquery.Client()
    query_job = client.query(query)
    results = query_job.result()
    df = results.to_dataframe()
    return df

def create_figure(df):
    fig = go.Figure(data=go.Bar(x=df['City'], y=df['avg_price']))
    div = pio.to_html(fig, full_html=False)
    return div

@app.route('/')
def home():
    query = "SELECT City, AVG(CAST(Price AS FLOAT64)) AS avg_price FROM `bds.bds_3` WHERE Price IS NOT NULL GROUP BY City ORDER BY avg_price DESC"
    df = get_data(query)
    plot_div = create_figure(df)
    return render_template('home.html', plot_div=plot_div)

@app.route('/dashboard')
def index():
    return render_template('index.html')


if __name__ == '__main__':
    app.run(debug=True,port=5002)