#!/usr/bin/env python

import pandas as pd
import numpy as np
import shapefile

dat = shapefile.Reader("/Users/joshuakaplan/msoa/england_msoa_2011.shp") 
msoas = set([i[2] for i in dat.iterRecords()])

from bokeh.plotting import figure
output_file("test.html")

TOOLS="pan,wheel_zoom,box_zoom,reset,previewsave"
figure(title="test", tools=TOOLS, plot_width=900, plot_height=800)

for msoa in msoas:
    data = getDict(msoa, dat)
    patches(data[msoa]['lat_list'], data[msoa]['lng_list'], fill_color="#138808", line_color="black")


# Given a shapeObject return a list of list for latitude and longitudes values
#       - Handle scenarios where there are multiple parts to a shapeObj

def getParts ( shapeObj ):

    points = []

    num_parts = len( shapeObj.parts )
    end = len( shapeObj.points ) - 1
    segments = list( shapeObj.parts ) + [ end ]

    for i in range( num_parts ):
        points.append( shapeObj.points[ segments[i]:segments[i+1] ] )


    return points


# Return a dict with three elements
#        - state_name
#        - total_area
#        - list of list representing latitudes
#        - list of list representing longitudes
#
#  Input: State Name & ShapeFile Object

def getDict ( msoa, shapefile ):

    msoaDict = {msoa: {} }

    rec = []
    shp = []
    points = []


    # Select only the records representing the
    # "msoa" and discard all other
    for i in shapefile.shapeRecords( ):

        if i.record[2] == msoa:
            rec.append(i.record)
            shp.append(i.shape)

    # In a multi record state for calculating total area
    # sum up the area of all the individual records
    #        - first record element represents area in cms^2
        total_area = sum( [float(i[0]) for i in rec] ) / (1000*1000)


      # For each selected shape object get
      # list of points while considering the cases where there may be
      # multiple parts  in a single record
        for j in shp:
            for i in getParts(j):
                points.append(i)

      # Prepare the dictionary
      # Seperate the points into two separate lists of lists (easier for bokeh to consume)
      #      - one representing latitudes
      #      - second representing longitudes

        lat = []
        lng = []
        for i in points:
            lat.append( [j[0] for j in i] )
            lng.append( [j[1] for j in i] )


        msoaDict[msoa]['lat_list'] = lat
        msoaDict[msoa]['lng_list'] = lng
        msoaDict[msoa]['total_area'] = total_area

        return msoaDict