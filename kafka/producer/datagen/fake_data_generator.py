import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import random
import time


def generate_telemetry():
    # this function simulates and IoT device sending off individual records to a kafka producer
    # for now it just generates telemetry as if it's a particular well 
     return {
        "timestamp": int(time.time()),
        "well_id": f"well_1",
        "pressure": random.uniform(1000, 5000),
        "temperature": random.uniform(50, 200),
        "flow_rate": random.uniform(100, 1000),
        
    }