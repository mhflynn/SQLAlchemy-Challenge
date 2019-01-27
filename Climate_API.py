import numpy as np

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

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
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Utility Functions
#################################################
# 
import datetime as dt

""" Function to adjust year value of a date value
         Inputs : date - String value for date in ISO format %Y-%M-%D
                  delta - Integer, +/-  """

def date_year_delta (date, delta) :
    try :
        qd = dt.datetime.strptime(date, '%Y-%m-%d')  # Usable format for the date
        return dt.datetime(qd.year+delta, qd.month, qd.day).strftime('%Y-%m-%d') # adjust year

    except ValueError :
        return date  # If error, return input date value

""" Function to validity of date format
         Inputs : date - String value for date in ISO format %Y-%M-%D """

def check_date_valid (date) :
    try :
        check = dt.datetime.strptime(date, '%Y-%m-%d')  # Usable format for the date
        return check

    except ValueError :
        return False

#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        "<strong>Available Routes:</strong><br/>"
        "/api/v1.0/stations<br/>"
        "/api/v1.0/precipitation<br/>"
        "/api/v1.0/temperature<br/>"
        "/api/v1.0/'start date'   <u>e.g. /api/v1.0/2017-04-01</u><br/>"
        "/api/v1.0/'start date'/'end date'   <u>e.g. /api/v1.0/2017-04-01/2017-05-01</u>"
    )


@app.route("/api/v1.0/station")
@app.route("/api/v1.0/stations")
def stations():
    """ Return a list of all stations from Station table. """

    # Query for stations and convert results into flat list
    rtnlist = list(np.ravel(session.query(Station.station).all()))

    return jsonify(rtnlist)


@app.route("/api/v1.0/prcp")
@app.route("/api/v1.0/precipitation")
def precipitation():
    """ Return a dictionary of precipitation data with date as the key 
        Note : I interpret the instructions to provide equivalent of a dictionary
               with with one key per date. As there are multiple stations, there are 
               multiple precipitation observations for each date. The api query will 
               return the average precipitation value for each date, sorted. """

    # Create a dictionary for the query results
    rtnlist = []
    for date, prcp in session.query(Measurement.date, func.avg(Measurement.prcp))\
                             .group_by(Measurement.date).order_by(Measurement.date) :
        rtnlist.append({"date":date, "prcp":prcp})

    return jsonify(rtnlist)


@app.route("/api/v1.0/tobs")
@app.route("/api/v1.0/temperature")
def temperature():
    """ Return temperature observations for most recent year available as a 
        json object of date, temperature pairs. 
        Note : As there are multiple stations there are multiple temperature
               observations for each date. The query will return a date/temp
               pair for each tobs in the table for the most recent table year. """
    
    #Adjust end date by 1 year
    max_date = session.query(func.max(Measurement.date)).scalar()
    query_date = date_year_delta (max_date, -1)

    # Create a dictionary for the query results
    rtnlist = []
    for date, temp in session.query(Measurement.date, Measurement.tobs)\
                             .filter(Measurement.date >= query_date).order_by(Measurement.date) :
        rtnlist.append({"date":date, "temp":temp})

    return jsonify(rtnlist)


@app.route("/api/v1.0/<start>")
def tempstart(start):
    """ Return a JSON list of the minimum temperature, the average temperature, 
        and the max temperature from the given start date to the most recent
        date available from the Measurement table. """

    return tobs_query (start, dt.datetime.today().strftime('%Y-%m-%d'))


@app.route("/api/v1.0/<start>/<end>")
def tempstart_end(start, end):
    """ Return a JSON list of the minimum temperature, the average temperature, 
        and the max temperature from the given start date through the given end date 
        from the Measurement table. """

    return tobs_query(start, end)

def tobs_query(start, end) :
    """ Utility function to query min, avg and max temperature observations
        from the Measurement table, between the given start and end dates
        inclusive. """

    # Get the latest date in the Measurement table
    max_date = dt.datetime.strptime(session.query(func.max(Measurement.date)).scalar(),'%Y-%m-%d')
    
    # Checl for valid date inputs
    q_start = check_date_valid(start)
    q_end   = check_date_valid(end)

    if q_start and q_end and (q_start <= q_end) and (q_start <= max_date) :
        min,avg,max = session.query(func.min(Measurement.tobs),\
                                    func.avg(Measurement.tobs),\
                                    func.max(Measurement.tobs))\
                             .filter(Measurement.date>=start, Measurement.date<=end).all()[0]
        rtnlist = [{'min':min, 'avg':avg, 'max':max}]
    else :
        rtnlist = ["Unrecognized API or invalid date values"]
        #rtnlist = [{'min':'None', 'avg':'None', 'max':'None'}]

    return jsonify(rtnlist)


if __name__ == '__main__':
    app.run(debug=True)
