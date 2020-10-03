import numpy as np

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
import pandas as pd

from flask import Flask, jsonify


#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
Measurements = Base.classes.measurement
Stations = Base.classes.station

#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def home():
    "List all available api routes."
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/start<br/>"
        f"/api/v1.0/start/end"
    )


@app.route("/api/v1.0/precipitation")
def precip():
    # Create our session (link) from Python to the DB
    session = Session(engine)
    #Query
    max_date =  session.query(func.max(Measurements.date)).all()
    for ea in max_date:
        max_date = ea[0]

    max_date = pd.to_datetime(max_date)-pd.Timedelta(weeks=52)
    results =  session.query(Measurements.date, Measurements.prcp).filter(Measurements.date > max_date.date()).all()
    data = []   
    for ea in results:
        data.append({"date": ea[0], "inches": ea[1]})
    return jsonify(data)
    session.close()

@app.route("/api/v1.0/stations")
def stations():
    # Create our session (link) from Python to the DB
    session = Session(engine)
    no_stations = session.query(Measurements.station).distinct().all()
    stations_list = list(np.ravel(no_stations))
    return jsonify(stations_list)
    session.close()


@app.route("/api/v1.0/tobs")
def tobs():

    # Create our session (link) from Python to the DB
    session = Session(engine)

    max_date =  session.query(func.max(Measurements.date)).all()
    for ea in max_date:
        max_date = ea[0]

    max_date = pd.to_datetime(max_date)-pd.Timedelta(weeks=52)
    count= session.query(Measurements.station, func.count(Measurements.station)).group_by(Measurements.station).all()

    counts = []
    for ea in count:
        counts.append({"Station": ea[0], "Count": ea[1]})

    df= pd.DataFrame(counts)
    df = df.sort_values("Count",ascending =False).reset_index()
    temp_obs = session.query(Measurements.station, func.count(Measurements.tobs)).group_by(Measurements.station).all()
    temp_counts = []
    for r in temp_obs:
        temp_counts.append({"station": r[0], "temp_count": r[1]})
    df = pd.DataFrame(temp_counts)
    df = df.sort_values("temp_count", ascending=False).reset_index()

    sta = df['station'][0]
    data = session.query(Measurements.date, Measurements.tobs).filter(Measurements.station==sta).filter(Measurements.date>max_date.date()).all()
    data_df = []
    for ea in data:
        data_df.append({"date": ea[0], "temp": ea[1]})
    return jsonify(data_df)
    
    session.close()

@app.route("/api/v1.0/<start>")
def start(start):
    session = Session(engine)

    # Perform a query to retrieve the data and precipitation scores

    records =  session.query(Measurements.date, Measurements.tobs).filter(Measurements.date >= start).all()
    data = []
    for ea in records:
        data.append(ea[1])
    
    json_data = []
    json_data.append({"TMIN": min(data), "TAVG": (sum(data)/len(data)), "TMAX": max(data)})
    
    return jsonify(json_data)

    session.close()


@app.route("/api/v1.0/<start>/<end>")
def startend(start, end):
    session = Session(engine)

    # Perform a query to retrieve the data and precipitation scores

    records =  session.query(Measurements.date, Measurements.tobs).filter(Measurements.date >= start).filter(Measurements.date <= end).all()
    data = []
    for ea in records:
        data.append(ea[1])
    
    json_data = []
    json_data.append({"TMIN": min(data), "TAVG": (sum(data)/len(data)), "TMAX": max(data)})
    
    return jsonify(json_data)

    session.close()

if __name__ == '__main__':
    app.run(debug=True)
