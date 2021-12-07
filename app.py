from flask import Flask, jsonify
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func,inspect
import numpy as np
import pandas as pd
import datetime as dt



#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################




@app.route("/")
def welcome():
    return (
        f"Welcome to Hawaii Historical Weather API<br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/2010-12-31<br/>"
        f"/api/v1.0/2010-12-31/2011-12-31</br>"
    )


@app.route("/api/v1.0/precipitation")
def precipitation():
    engine = create_engine("sqlite:///./resources/hawaii.sqlite")
    inspector = inspect(engine)
    inspector.get_table_names()
    Base = automap_base()
    # reflect the tables
    Base.prepare(engine, reflect=True)
    Base.classes.keys()
    Measurement = Base.classes.measurement
    Station = Base.classes.station
    # Create our session (link) from Python to the DB
    session = Session(engine)

    print("Debug -/api/v1.0/precipitation ")
    latest_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()[0]
    latest_date_dt = dt.datetime.strptime(latest_date,'%Y-%m-%d')
    one_year_bac = latest_date_dt - dt.timedelta(days=365)
    print("Debug - Most recent Date:",latest_date_dt)
    results=session.query(Measurement.date, Measurement.prcp).order_by(Measurement.date.desc()).\
    filter(Measurement.date > one_year_bac).all()
    prcp_df = pd.DataFrame(results[:], columns=['Date', 'Precipitation'])
    prcp_df_ret = prcp_df.set_index('Date')
    prcp_dict =prcp_df_ret.to_dict()
	
    return jsonify(prcp_dict)


@app.route("/api/v1.0/stations")
def stations():
    engine = create_engine("sqlite:///./resources/hawaii.sqlite")
    inspector = inspect(engine)
    inspector.get_table_names()
    Base = automap_base()
    # reflect the tables
    Base.prepare(engine, reflect=True)
    Base.classes.keys()
    Measurement = Base.classes.measurement
    Station = Base.classes.station
    # Create our session (link) from Python to the DB
    session = Session(engine)

    print("Debug -/api/v1.0/stations ")
    results=session.query(Measurement.station).group_by(Measurement.station).order_by(Measurement.station).all()
    station_df = pd.DataFrame(results[:], columns=['Station'])
    station_dict =station_df.to_dict()
	
    return jsonify(station_dict)

@app.route("/api/v1.0/tobs")
def tobs():

    engine = create_engine("sqlite:///./resources/hawaii.sqlite")
    inspector = inspect(engine)
    inspector.get_table_names()
    Base = automap_base()
    # reflect the tables
    Base.prepare(engine, reflect=True)
    Base.classes.keys()
    Measurement = Base.classes.measurement
    Station = Base.classes.station
    # Create our session (link) from Python to the DB
    session = Session(engine)

    print("Debug -/api/v1.0/tobs ")
    results=session.query(Measurement.station, func.count(Measurement.station)).group_by(Measurement.station).order_by(func.count(Measurement.station).desc()).first()
    most_active_station=results[0]
    latest_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()[0]
    latest_date_dt = dt.datetime.strptime(latest_date,'%Y-%m-%d')
    one_year_bac = latest_date_dt - dt.timedelta(days=365)

    active_stn_temps = session.query(Measurement.tobs).filter((Measurement.station=='USC00519281')  | (Measurement.date > one_year_bac))
    temp_df = pd.DataFrame(active_stn_temps[:], columns=["Temperatures"])
    temp_dict =temp_df.to_dict()
	
    return jsonify(temp_dict)

@app.route("/api/v1.0/<start>")
def summary(start):

    engine = create_engine("sqlite:///./resources/hawaii.sqlite")
    inspector = inspect(engine)
    inspector.get_table_names()
    Base = automap_base()
    # reflect the tables
    Base.prepare(engine, reflect=True)
    Base.classes.keys()
    Measurement = Base.classes.measurement
    Station = Base.classes.station
    # Create our session (link) from Python to the DB
    session = Session(engine)

    print("Debug -/api/v1.0/tobs ")
    start_date= dt.datetime.strptime(start,'%Y-%m-%d')
    min_max_avg = session.query(func.min(Measurement.tobs), func.max(Measurement.tobs),func.avg(Measurement.tobs)).filter(Measurement.date > start_date)
    for rec in min_max_avg:
        temp_min =rec[0]
        temp_max =rec[1]
        temp_avg =rec[2]
    summary_dict = {'Min':temp_min,'Max':temp_max,'Avg':temp_avg}

#    summary_df = pd.DataFrame([['Min',temp_min],['Max',temp_max],['Avg',temp_avg]],columns=['Type','Temperature'])    
#    summary_dict =summary_df.to_dict()
    return jsonify(summary_dict)

@app.route("/api/v1.0/<start>/<end>")
def summary_range(start,end):

    engine = create_engine("sqlite:///./resources/hawaii.sqlite")
    inspector = inspect(engine)
    inspector.get_table_names()
    Base = automap_base()
    # reflect the tables
    Base.prepare(engine, reflect=True)
    Base.classes.keys()
    Measurement = Base.classes.measurement
    Station = Base.classes.station
    # Create our session (link) from Python to the DB
    session = Session(engine)

    print("Debug -/api/v1.0/tobs ")
    start_date= dt.datetime.strptime(start,'%Y-%m-%d')
    end_date= dt.datetime.strptime(end,'%Y-%m-%d')
    min_max_avg = session.query(func.min(Measurement.tobs), func.max(Measurement.tobs),func.avg(Measurement.tobs)).filter((Measurement.date > start_date) | (Measurement.date < end_date))
    for rec in min_max_avg:
        temp_min =rec[0]
        temp_max =rec[1]
        temp_avg =rec[2]
    summary_dict = {'Min':temp_min,'Max':temp_max,'Avg':temp_avg}

#    summary_df = pd.DataFrame([['Min',temp_min],['Max',temp_max],['Avg',temp_avg]],columns=['Type','Temperature'])    
#    summary_dict =summary_df.to_dict()
    return jsonify(summary_dict)


if __name__ == "__main__":
    app.run(debug=True)
