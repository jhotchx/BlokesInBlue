# -*- coding: utf-8 -*-
"""
Created on Sat Apr 30 14:53:50 2016

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
    GMapOptions, widgets, BoxZoomTool,
    NumeralTickFormatter, PrintfTickFormatter)
from bokeh.models.widgets import Panel, Tabs
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
## separate uneployment data out by year
years = [2011,2012,2013,2014,2015]
unemp = dict()
crim = dict()
for year in years:
    unemp[year]=unemployment.loc[(unemployment.Date==year),['LAD','Unemployment','Date']]
    crim[year]=crime.loc[(crime.Year==year),['LAD','Year','crimes']]

#unemp2011 = unemployment.loc[(unemployment.Date==2011),['LAD','Unemployment','Date']]
#unemp2011 = unemp2011[pandas.notnull(unemp2011['Unemployment'])]

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
lad_unemployment = dict()
lad_crime=dict()
for year in years:   
    lad_unemp_year = list()
    lad_crime_year = list()
    for i in range(len(shapes)):
        try:
            lad_unemp_year.append(unemp[year].Unemployment[unemp[year].LAD==lad_names[i]].values[0])
        except IndexError:
            lad_unemp_year.append(np.nan)
        try:
            lad_crime_year.append(crim[year].crimes[crim[year].LAD==lad_names[i]].values[0])
        except IndexError:
           lad_crime_year.append(np.nan)
    lad_unemployment[year] = lad_unemp_year
    lad_crime[year]=lad_crime_year

#lad_unemployment= unemp2011[unemp2011['LAD'].isin(lad_names)]

# Sets color depending on unemployment rate:
#colors = ["#F1EEF6", "#D4B9DA", "#C994C7", "#DF65B0", "#DD1C77", "#980043"] #reds
#colors = ["#87CEFA", "#6495ED", "#4682B4", "#4169E1", "#0000FF", "#0000CD", "#483D8B", "#00008B"] #blues
sd = []
avg = []
for year in years:
    sd.append(np.std([g for g in lad_unemployment[year] if str(g) != 'nan']))
    avg.append(np.mean([h for h in lad_unemployment[year] if str(h) != 'nan']))
sd = np.mean(sd)
avg = np.mean(avg)
colors = ["#d5edff", "#6daaee", "#3858f9", "#043c9a"] #blues
LAD_colors = dict()
for year in years:
    LAD_colors_year = list()
    for i in range(len(lad_names)):
        if math.isnan(lad_unemployment[year][i]):
            LAD_colors_year.append("black")
        else:
            try:
                if lad_unemployment[year][i] < avg-sd:
                    LAD_colors_year.append(colors[0])
                elif lad_unemployment[year][i] < avg:
                    LAD_colors_year.append(colors[1])
                elif lad_unemployment[year][i] < avg+sd:
                    LAD_colors_year.append(colors[2])
                else:
                    LAD_colors_year.append(colors[3])
            except KeyError:
                    LAD_colors_year.append("black")
    LAD_colors[year]=LAD_colors_year

source1 = ColumnDataSource(data=dict(
    y=lad_lats,
    x=lad_longs,
    color=LAD_colors[2011],
    name=lad_names,
    uerate=lad_unemployment[2011],
    total_crime=lad_crime[2011]
))

source2 = ColumnDataSource(data=dict(
    y=lad_lats,
    x=lad_longs,
    color=LAD_colors[2012],
    name=lad_names,
    uerate=lad_unemployment[2012],
    total_crime=lad_crime[2012]
))

source3 = ColumnDataSource(data=dict(
    y=lad_lats,
    x=lad_longs,
    color=LAD_colors[2013],
    name=lad_names,
    uerate=lad_unemployment[2013],
    total_crime=lad_crime[2013]
))

source4 = ColumnDataSource(data=dict(
    y=lad_lats,
    x=lad_longs,
    color=LAD_colors[2014],
    name=lad_names,
    uerate=lad_unemployment[2014],
    total_crime=lad_crime[2014]
))

source5 = ColumnDataSource(data=dict(
    y=lad_lats,
    x=lad_longs,
    color=LAD_colors[2015],
    name=lad_names,
    uerate=lad_unemployment[2015],
    total_crime=lad_crime[2015]
))

TOOLS="pan,wheel_zoom,box_zoom,reset,hover,save"

p1 = figure(title="2011",webgl=True, tools=TOOLS)
p1.patches('x', 'y', source=source1, fill_alpha=1, fill_color='color',
          line_color='black', line_width=0.5)
p1.square(-6.3, 49.9, legend="<3.8%", fill_color="#d5edff", size=.1)
p1.square(-6.3, 49.9, legend="3.8-6.7%", fill_color="#6daaee", size=.1)
p1.square(-6.3, 49.9, legend="6.7-9.6%", fill_color="#3858f9", size=.1)
p1.square(-6.3, 49.9, legend=">9.6%", fill_color="#043c9a", size=.1)
p1.square(-6.3, 49.9, legend="No Information", fill_color="black", size=.1)
p1.legend.glyph_width = 30
p1.legend.glyph_height = 30
p1.legend.location = "top_left"
hover = p1.select_one(HoverTool)
hover.point_policy = "follow_mouse"
hover.tooltips = [
    ("Name", "@name"),
    ("Unemployment Rate","@uerate"),
    ("Crime Rate","@total_crime")
]

p2 = figure(title="2012",webgl=True, tools=TOOLS)
p2.patches('x', 'y', source=source2, fill_alpha=1, fill_color='color',
          line_color='black', line_width=0.5)
p2.square(-6.3, 49.9, legend="<3.8%", fill_color="#d5edff", size=.1)
p2.square(-6.3, 49.9, legend="3.8-6.7%", fill_color="#6daaee", size=.1)
p2.square(-6.3, 49.9, legend="6.7-9.6%", fill_color="#3858f9", size=.1)
p2.square(-6.3, 49.9, legend=">9.6%", fill_color="#043c9a", size=.1)
p2.square(-6.3, 49.9, legend="No Information", fill_color="black", size=.1)
p2.legend.glyph_width = 30
p2.legend.glyph_height = 30
p2.legend.location = "top_left"
hover = p2.select_one(HoverTool)
hover.point_policy = "follow_mouse"
hover.tooltips = [
    ("Name", "@name"),
    ("Unemployment Rate","@uerate"),
    ("Crime Rate","@total_crime")
]

p3 = figure(title="2013",webgl=True, tools=TOOLS)
p3.patches('x', 'y', source=source3, fill_alpha=1, fill_color='color',
          line_color='black', line_width=0.5)
p3.square(-6.3, 49.9, legend="<3.8%", fill_color="#d5edff", size=.1)
p3.square(-6.3, 49.9, legend="3.8-6.7%", fill_color="#6daaee", size=.1)
p3.square(-6.3, 49.9, legend="6.7-9.6%", fill_color="#3858f9", size=.1)
p3.square(-6.3, 49.9, legend=">9.6%", fill_color="#043c9a", size=.1)
p3.square(-6.3, 49.9, legend="No Information", fill_color="black", size=.1)
p3.legend.glyph_width = 30
p3.legend.glyph_height = 30
p3.legend.location = "top_left"
hover = p3.select_one(HoverTool)
hover.point_policy = "follow_mouse"
hover.tooltips = [
    ("Name", "@name"),
    ("Unemployment Rate","@uerate"),
    ("Crime Rate","@total_crime")
]

p4 = figure(title="2014",webgl=True, tools=TOOLS)
p4.patches('x', 'y', source=source4, fill_alpha=1, fill_color='color',
          line_color='black', line_width=0.5)
p4.square(-6.3, 49.9, legend="<3.8%", fill_color="#d5edff", size=.1)
p4.square(-6.3, 49.9, legend="3.8-6.7%", fill_color="#6daaee", size=.1)
p4.square(-6.3, 49.9, legend="6.7-9.6%", fill_color="#3858f9", size=.1)
p4.square(-6.3, 49.9, legend=">9.6%", fill_color="#043c9a", size=.1)
p4.square(-6.3, 49.9, legend="No Information", fill_color="black", size=.1)
p4.legend.glyph_width = 30
p4.legend.glyph_height = 30
p4.legend.location = "top_left"
hover = p4.select_one(HoverTool)
hover.point_policy = "follow_mouse"
hover.tooltips = [
    ("Name", "@name"),
    ("Unemployment Rate","@uerate"),
    ("Crime Rate","@total_crime")
]

p5 = figure(title="2015",webgl=True, tools=TOOLS)
p5.patches('x', 'y', source=source5, fill_alpha=1, fill_color='color',
          line_color='black', line_width=0.5)
p5.square(-6.3, 49.9, legend="<3.8%", fill_color="#d5edff", size=.1)
p5.square(-6.3, 49.9, legend="3.8-6.7%", fill_color="#6daaee", size=.1)
p5.square(-6.3, 49.9, legend="6.7-9.6%", fill_color="#3858f9", size=.1)
p5.square(-6.3, 49.9, legend=">9.6%", fill_color="#043c9a", size=.1)
p5.square(-6.3, 49.9, legend="No Information", fill_color="black", size=.1)
p5.legend.glyph_width = 30
p5.legend.glyph_height = 30
p5.legend.location = "top_left"
hover = p5.select_one(HoverTool)
hover.point_policy = "follow_mouse"
hover.tooltips = [
    ("Name", "@name"),
    ("Unemployment Rate","@uerate"),
    ("Crime Rate","@total_crime")
]


tab1 = Panel(child=p1, title="2011")
tab2 = Panel(child=p2, title="2012")
tab3 = Panel(child=p3, title="2013")
tab4 = Panel(child=p4, title="2014")
tab5 = Panel(child=p5, title="2015")

tabs = Tabs(tabs=[tab1,tab2,tab3,tab4,tab5]) #Tabs(tabs=[tab1,tab2,tab3,tab4,tab5 ])

output_file("LADmaps.html", title="Local Area Districts")
show(tabs)
