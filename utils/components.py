import dash
from dash import html
from dash import dcc
import dash_mantine_components as dmc
import utils.components as comp
from pip import main
import plotly.express as px
import datetime as dt
import plotly.graph_objects as go
from dash import callback
from dash.dependencies import Input, Output, State
from utils.dbx_utils import get_bme_data
from utils.dbx_utils import get_moving_average
from constants import custom_color
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import dash_daq as daq
from constants import app_description
from datetime import timezone





from dash.dependencies import Input, Output

app_start_ts = dt.datetime.now()


df1= get_moving_average(Temp_15s_Moving_Average=[],Temp_60s_Moving_Average=[],Humidity_15s_Moving_Average=[],Humidity_60s_Moving_Average=[],TimestampSecond=[])

df = get_bme_data(TempReading=[],HumidityReading=[], EventTimestamp=[],EventDate=[])


x= df.EventTimestamp
y= df.TempReading
a= df.HumidityReading
b= df.EventDate
p= df1.Temp_15s_Moving_Average
q= df1.Temp_60s_Moving_Average
h= df1.Humidity_15s_Moving_Average
g= df1.Humidity_60s_Moving_Average

t= df1.TimestampSecond



resolution = 1000


templine = px.line(df,x=x,y=y)
humidityline = px.line(df, x=x, y=a)
temp_magraph= px.line(df1, x=t, y=[p,q])
hum_magraph = px.line(df1, x=t, y=[g,h])



external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets,suppress_callback_exceptions=True)

app.layout = dmc.MantineProvider(
    withGlobalStyles=True,
    theme={"colorScheme":"dark"},
    children=dmc.NotificationsProvider(
        [
            comp.header(
                app,
                "#FFFFFF",
                "Dash with Databricks",
                header_background_color="#111014",
            ),
            comp.create_text_columns(app_description, "description"),
            dmc.Tabs(
                grow=True,
                variant="outline",
                children=[
                    dmc.Tab(
                        label="Temperature & Humidity Streams" , children=comp.LEFT_TAB
                    ),
                    dmc.Tab(label="Scatter Plot Mode", children=comp.RIGHT_TAB),
                ],
            ),
        ]
    ),
)



#Clientside callback

@callback(Output("time", "children"), Input("serverside_interval", "n_intervals"))
def refresh_data_at_interval(interval_trigger):
   """
   This simple callback demonstrates how to use the Interval component to update data at a regular interval.
   This particular example updates time every second, however, you can subsitute this data query with any acquisition method your product requires.
   """
   return dt.datetime.now().strftime("%M:%S")

@app.callback(Output('graph', 'extendData'),
             [Input('graph_interval', 'n_intervals')])
def update_data(n_intervals):
    index= n_intervals % resolution
    return dict(x=[[x[index]]], y=[[y[index]]]), [0], 10

@app.callback(Output('hgraph', 'extendData'),
             [Input('hgraph_interval', 'n_intervals')])
def update_data(n_intervals):
    index= n_intervals % resolution
    return dict(x=[[x[index]]], y=[[a[index]]]), [0], 10

@app.callback(Output('temp_magraph', 'extendData'),
             [Input('temp_magraph_interval', 'n_intervals')])
def update_data(n_intervals):
    index= n_intervals % resolution
    return dict(x=[[t[index]]], y=[[p[index]],q[index]]), [0], 10

@app.callback(Output('hum_magraph', 'extendData'),
             [Input('hum_magraph_interval', 'n_intervals')])
def update_data(n_intervals):
    index= n_intervals % resolution
    return dict(x=[[t[index]]], y=[[g[index]],h[index]]), [0], 10

@app.callback(
     Output('clientside-store-data', 'data'),
     Input('serverside-interval', 'n_intervals'),
 )
def update_store_data(n_intervals):
     last_row = n_intervals*10000
     stored_data = df.iloc[0:last_row]
     return stored_data.to_dict('records')


app.clientside_callback(
     """
     function(n_intervals, data) {
         if(data[n_intervals] === undefined) {
             return '';
         }
         return data[n_intervals]['TempReading'];
     }
     """,
     Output('clientside-content', 'children'),
     Input('clientside-interval','n_intervals'),
     State('clientside-store-data', 'data'),
 )

@app.callback(
     Output('clientside-store-datab', 'data'),
     Input('serverside-intervalb', 'n_intervals'),
 )
def update_store_data(n_intervals):
     last_row = n_intervals*10000
     stored_data = df.iloc[0:last_row]
     return stored_data.to_dict('records')


app.clientside_callback(
     """
     function(n_intervals, data) {
         if(data[n_intervals] === undefined) {
             return '';
         }
         return data[n_intervals]['HumidityReading'];
     }
     """,
     Output('clientside-contentb', 'children'),
     Input('clientside-intervalb','n_intervals'),
     State('clientside-store-datab', 'data'),
 )

@app.callback(
     Output('clientside-store-datac', 'data'),
     Input('serverside-intervalc', 'n_intervals'),
 )
def update_store_data(n_intervals):
     last_row = n_intervals*10000
     stored_data = df.iloc[0:last_row]
     return stored_data.to_dict('records')


app.clientside_callback(
     """
     function(n_intervals, data) {
         if(data[n_intervals] === undefined) {
             return '';
         }
         return data[n_intervals]['EventTimestamp'];
     }
     """,
     Output('clientside-contentc', 'children'),
     Input('clientside-intervalc','n_intervals'),
     State('clientside-store-datac', 'data'),
 )

@app.callback(
     Output('clientside-store-datad', 'data'),
     Input('serverside-intervald', 'n_intervals'),
 )
def update_store_data(n_intervals):
     last_row = n_intervals*10000
     stored_data = df.iloc[0:last_row]
     return stored_data.to_dict('records')


app.clientside_callback(
     """
     function(n_intervals, data) {
         if(data[n_intervals] === undefined) {
             return '';
         }
         return data[n_intervals]['EventDate'];
     }
     """,
     Output('clientside-contentd', 'children'),
     Input('clientside-intervald','n_intervals'),
     State('clientside-store-datad', 'data'),
 )

if str(app_start_ts).startswith('2'):
    print(app_start_ts)


if __name__ == '__main__':
    app.run_server(debug=True, port=5559)
