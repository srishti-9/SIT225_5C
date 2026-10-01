from arduino_iot_cloud import ArduinoCloudClient
from arduino_secrets import DEVICE_ID, SECRET_KEY
from datetime import datetime
import threading
import csv
import os

from dash import Dash, dcc, html
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
import plotly.graph_objs as go

from stream_buffer import StreamBuffer

# CONFIG
CSV_FILE = "accelerometer_xyz.csv"
MAX_POINTS = 200
CHANNELS = ["x", "y", "z"]

# Shared rolling buffer (the only state shared between cloud and Dash threads)
buf = StreamBuffer(channel_names=CHANNELS, maxlen=MAX_POINTS)

# Temporary values from cloud
x_value = y_value = z_value = None
x_received = y_received = z_received = False

# CSV
if not os.path.exists(CSV_FILE):
    with open(CSV_FILE, "w", newline="") as f:
        csv.writer(f).writerow(["timestamp", "x", "y", "z"])


# Save data
def save_combined_data():
    global x_received, y_received, z_received

    if x_received and y_received and z_received:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        with open(CSV_FILE, "a", newline="") as f:
            csv.writer(f).writerow([timestamp, x_value, y_value, z_value])

        # Reusable wrapper replaces the old hard-coded add_sample(x, y, z)
        buf.add_sample(x=x_value, y=y_value, z=z_value)

        print(f"{timestamp} | X={x_value:.2f} Y={y_value:.2f} Z={z_value:.2f}")

        x_received = y_received = z_received = False


# Cloud callbacks
def on_x(client, value):
    global x_value, x_received
    x_value = value
    x_received = True
    save_combined_data()


def on_y(client, value):
    global y_value, y_received
    y_value = value
    y_received = True
    save_combined_data()


def on_z(client, value):
    global z_value, z_received
    z_value = value
    z_received = True
    save_combined_data()


# Arduino Cloud
client = ArduinoCloudClient(
    device_id=DEVICE_ID,
    username=DEVICE_ID,
    password=SECRET_KEY
)
client.register("accelerometer_x", value=None, on_write=on_x)
client.register("accelerometer_y", value=None, on_write=on_y)
client.register("accelerometer_z", value=None, on_write=on_z)


# Dash App
# The figure is created ONCE with three empty traces. After that it is never
# rebuilt: new samples are appended in the browser via Plotly's extendData.
initial_fig = go.Figure(
    data=[go.Scatter(x=[], y=[], mode="lines", name=c.upper()) for c in CHANNELS]
)
initial_fig.update_layout(
    template="plotly_dark",
    height=500,
    xaxis_title="Time",
    yaxis_title="Acceleration",
    xaxis=dict(type="date"),
    uirevision=True,
    margin=dict(l=40, r=20, t=40, b=40),
    legend=dict(orientation="h", y=1.1),
)

app = Dash(__name__)

app.layout = html.Div([
    html.H2("Live Smartphone Accelerometer"),
    dcc.Graph(id="live-graph", figure=initial_fig),
    # Per-browser cursor: the sequence number of the last sample already sent
    dcc.Store(id="cursor", data=0),
    dcc.Interval(id="interval", interval=50, n_intervals=0),
])


@app.callback(
    Output("live-graph", "extendData"),
    Output("cursor", "data"),
    Input("interval", "n_intervals"),
    State("cursor", "data"),
)
def push_new_samples(_, cursor):
    new_cursor, times, values = buf.get_since(cursor or 0)

    if not times:
        raise PreventUpdate  # nothing new: send nothing, graph stays as is

    # extendData = (new data per trace, trace indices, max points to keep)
    payload = dict(
        x=[times] * len(CHANNELS),
        y=[values[c] for c in CHANNELS],
    )
    return (payload, list(range(len(CHANNELS))), MAX_POINTS), new_cursor


# Run both simultaneously
def start_cloud():
    print("Connecting to Arduino IoT Cloud...")
    client.start()


threading.Thread(target=start_cloud, daemon=True).start()

print("Open http://127.0.0.1:8050")
app.run(debug=False)