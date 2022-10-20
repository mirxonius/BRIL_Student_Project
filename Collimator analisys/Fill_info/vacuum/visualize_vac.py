import json
#import plotly.express as px
#import plotly.graph_objects as go
import numpy as np


if __name__ == "__main__":

    with open("vacuum_data.json",'r') as f:
        data = json.load(f)

    print(data.keys())