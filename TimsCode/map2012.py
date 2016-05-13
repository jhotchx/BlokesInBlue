# -*- coding: utf-8 -*-
"""
Created on Sat Apr 16 21:34:23 2016

@author: joshuakaplan
"""
import pyproj
import shapefile
import pandas
import math
import numpy as np
from collections import OrderedDict
from bokeh.models.glyphs import Patches, Line, Circle
from bokeh.models import (
    GMapPlot, Range1d, ColumnDataSource, LinearAxis, GeoJSONDataSource,
    HoverTool, PanTool, WheelZoomTool, BoxSelectTool, ResetTool, PreviewSaveTool,
    GMapOptions, Slider,
    NumeralTickFormatter, PrintfTickFormatter)
from bokeh.plotting import figure, show, output_file

## Getting unemployment data into usable format
crime = pandas.read_csv('crime_unemployment.csv',usecols=('LAD_name','Year','count'))
cols = ['LAD','Year','crimes']
crime.columns=cols
unemployment = pandas.read_csv('UnemploymentLAD.csv', usecols=('local authority: district / unitary (prior to April 2015)','Unemployment rate - aged 16-64','Date'))
unemployment['Unemployment rate - aged 16-64'] = pandas.to_numeric(unemployment['Unemployment rate - aged 16-64'], errors='coerce')

unemployment.columns = unemployment.columns.map(lambda x: x.replace(' ', '_') if isinstance(x, (str, unicode)) else x)
unemployment.columns = unemployment.columns.map(lambda x: x.replace('-', '_') if isinstance(x, (str, unicode)) else x)
unemployment.columns = unemployment.columns.map(lambda x: x.replace("`", "'") if isinstance(x, (str, unicode)) else x)

cols = ['LAD','Unemployment','Date']
unemployment.columns = cols
unemp2012 = unemployment.loc[(unemployment.Date==2012),['LAD','Unemployment','Date']]
crime2012 = crime.loc[(crime.Year==2012),['LAD','Year','crimes']]
#unemp2012 = unemp2012[pandas.notnull(unemp2012['Unemployment'])]

## pulling lat/longs from shapefile
sf = shapefile.Reader("/Users/Ahn/Desktop/ukcrime/Shapefile/england_lad_2011_gen.shp") 

#http://gis.stackexchange.com/questions/168310/how-to-convert-projected-coordinates-to-geographic-coordinates-without-arcgis
#https://karlhennermann.wordpress.com/2015/02/16/how-to-make-lsoa-and-msoa-boundaries-from-uk-data-service-align-properly-in-arcgis/
shapes = sf.shapes()
records = sf.records()
#fields = sf.fields

def transform(epsg_in, epsg_out, x_in, y_in):

    # define source and destination coordinate systems based on the ESPG code
    srcProj = pyproj.Proj(init='epsg:%i' % int(epsg_in), preserve_units=True)
    dstProj = pyproj.Proj(init='epsg:%i' % int(epsg_out), preserve_units=True)

    # perform transformation
    x_out,y_out = pyproj.transform(srcProj, dstProj, x_in, y_in)
    return x_out,y_out

data = dict([])
for i in range(len(shapes)):
    temp = dict()
    lats = list()        
    longs=list()
    for j in range(len(shapes[i].points)):
        x = shapes[i].points[j][0]
        y = shapes[i].points[j][1]
        lats.append(transform(epsg_in=27700,epsg_out=4326,x_in=x,y_in=y)[1])
        longs.append(transform(epsg_in=27700,epsg_out=4326,x_in=x,y_in=y)[0])
        name = records[i][1]
    temp['name']=name
    temp['lats']=lats
    temp['longs']=longs
    data[i] = temp

lad_names =[lad["name"] for lad in data.values()]
lad_lats = [lad["lats"] for lad in data.values()]
lad_longs = [lad["longs"] for lad in data.values()]
lad_unemployment = list()
lad_crime=list()
for i in range(len(shapes)):
    try:
        lad_unemployment.append(unemp2012.Unemployment[unemp2012.LAD==lad_names[i]].values[0])
    except IndexError:
        lad_unemployment.append(np.nan)
    try:
        lad_crime.append(crime2012.crimes[crime2012.LAD==lad_names[i]].values[0])
    except IndexError:
        lad_crime.append(np.nan)

#lad_unemployment= unemp2012[unemp2012['LAD'].isin(lad_names)]

# Sets color depending on unemployment rate:
#colors = ["#F1EEF6", "#D4B9DA", "#C994C7", "#DF65B0", "#DD1C77", "#980043"] #reds
#colors = ["#87CEFA", "#6495ED", "#4682B4", "#4169E1", "#0000FF", "#0000CD", "#483D8B", "#00008B"] #blues
colors = ["#d5edff", "#6daaee", "#3858f9", "#002566"] #blues
sd = 2.9
avg = 6.7
LAD_colors = []
for i in range(len(lad_names)):
    if math.isnan(lad_unemployment[i]):
        LAD_colors.append("black")
    else:
        try:
            if lad_unemployment[i] < avg-sd:
                LAD_colors.append(colors[0])
            elif lad_unemployment[i] < avg:
                LAD_colors.append(colors[1])
            elif lad_unemployment[i] < avg+sd:
                LAD_colors.append(colors[2])
            else:
                LAD_colors.append(colors[3])
        except KeyError:
                LAD_colors.append("black")

source = ColumnDataSource(data=dict(
    y=lad_lats,
    x=lad_longs,
    color=LAD_colors,
    name=lad_names,
    uerate=lad_unemployment,
    crime=lad_crime
))

#https://github.com/queise/Berlin_Maps/blob/master/Berlin_dens_gmap.py
p = GMapPlot(title="MSOA", plot_width=1000, plot_height=750, x_range = Range1d(), y_range = Range1d(), map_options = GMapOptions(lat=52.6816, lng=-1.0000, zoom=7))
p.map_options.map_type = "terrain"
patch = Patches(xs="x", ys="y", fill_color="color", fill_alpha=0.7, line_color="black", line_width=0.5)
patches_glyph = p.add_glyph(source, patch)
p.add_tools(PanTool(), WheelZoomTool(), BoxSelectTool(), HoverTool(), ResetTool(), PreviewSaveTool())

hover = p.select(dict(type=HoverTool))
hover.point_policy = "follow_mouse"
hover.tooltips = OrderedDict([
    ("Name", "@name"),
    ("Unemployment Rate 2012","@uerate"),
    ("Crime Rate 2012","@crime")
])

output_file("map2012.html", title="LAD GMap with Unemployment 2012", mode="cdn")
show(p)
