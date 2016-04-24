# -*- coding: utf-8 -*-
"""
Created on Sat Apr 16 21:34:23 2016

@author: joshuakaplan
"""
import pyproj
import shapefile
import pandas
import numpy as np
from collections import OrderedDict
from bokeh.models.glyphs import Patches, Line, Circle
from bokeh.models import (
    GMapPlot, Range1d, ColumnDataSource, LinearAxis, GeoJSONDataSource,
    HoverTool, PanTool, WheelZoomTool, BoxSelectTool, ResetTool, PreviewSaveTool,
    GMapOptions,
    NumeralTickFormatter, PrintfTickFormatter)
from bokeh.plotting import figure, show, output_file

## Getting unemployment data into usable format
unemployment = pandas.read_csv('UnemploymentLAD.csv', usecols=('local authority: district / unitary (prior to April 2015)','Unemployment rate - aged 16-64','Date'))
unemployment['Unemployment rate - aged 16-64'] = pandas.to_numeric(unemployment['Unemployment rate - aged 16-64'], errors='coerce')

unemployment.columns = unemployment.columns.map(lambda x: x.replace(' ', '_') if isinstance(x, (str, unicode)) else x)
unemployment.columns = unemployment.columns.map(lambda x: x.replace('-', '_') if isinstance(x, (str, unicode)) else x)

cols = ['LAD','Unemployment','Date']
unemployment.columns = cols
unemp2011 = unemployment.loc[(unemployment.Date==2011),['LAD','Unemployment','Date']]
unemp2011 = unemp2011[pandas.notnull(unemp2011['Unemployment'])]

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

#colors = ["#F1EEF6", "#D4B9DA", "#C994C7", "#DF65B0", "#DD1C77", "#980043"] #reds
#colors = ["#87CEFA", "#6495ED", "#4682B4", "#4169E1", "#0000FF", "#0000CD", "#483D8B", "#00008B"] #blues

# Sets color depending on population:
maxue = np.amax(unemp2011[[1]], axis=0)[0]
minue = np.amin(unemp2011[[1]], axis=0)[0]
colors = ["#87CEFA", "#6495ED", "#4682B4", "#4169E1", "#0000FF", "#0000CD", "#483D8B", "#00008B"] #blues
LAD_colors = []
for i in range(len(unemp2011[[0]])):
    try:
        uenorm = int(unemp2011.Unemployment[i] / 2 - .9)
        idx = min(uenorm, 7)
        LAD_colors.append(colors[idx])
    except KeyError:
        LAD_colors.append("black")

lad_names =[lad["name"] for lad in data.values()]
lad_lats = [lad["lats"] for lad in data.values()]
lad_longs = [lad["longs"] for lad in data.values()]
lad_unemployment= unemp2011[unemp2011['LAD'].isin(lad_names)]
col = colors*1200

source = ColumnDataSource(data=dict(
    y=lad_lats,
    x=lad_longs,
    color=LAD_colors,
    name=unemp2011[[0]], uerate=unemp2011[[1]]
#    name=lad_names,
#    uerate=lad_unemployment.Unemployment
))

#https://github.com/queise/Berlin_Maps/blob/master/Berlin_dens_gmap.py
p = GMapPlot(title="MSOA", plot_width=1200, plot_height=800, x_range = Range1d(), y_range = Range1d(), map_options = GMapOptions(lat=52.6816, lng=1.8317, zoom=7))
p.map_options.map_type = "terrain"
patch = Patches(xs="x", ys="y", fill_color="color", fill_alpha=0.7, line_color="black", line_width=0.5)
patches_glyph = p.add_glyph(source, patch)
p.add_tools(PanTool(), WheelZoomTool(), BoxSelectTool(), HoverTool(), ResetTool(), PreviewSaveTool())

hover = p.select(dict(type=HoverTool))
hover.point_policy = "follow_mouse"
hover.tooltips = OrderedDict([
    ("Name", "@name"),
    ("Latitude", "$y{1.11111}"),
    ("Longitude", "$x{1.11111}"),
    ("Unemployment Rate 2011","@uerate")
])

output_file("LADGMapUnemploy.html", title="LAD GMap with Unemployment", mode="cdn")
show(p)
