# -*- coding: utf-8 -*-
"""
Created on Fri Apr 22 21:14:00 2016

@author: joshuakaplan
"""# -*- coding: utf-8 -*-
"""
Created on Sat Apr 16 21:34:23 2016

@author: joshuakaplan
"""
import pyproj
import shapefile
from bokeh.models.glyphs import Patches
from bokeh.models import (
    GMapPlot, Range1d, ColumnDataSource,HoverTool, PanTool, WheelZoomTool, 
    BoxSelectTool,ResetTool, PreviewSaveTool,GMapOptions)
from bokeh.plotting import show, output_file
import pandas

## Getting unemployment data into usable format
unemployment = pandas.read_csv('UnemploymentLAD.csv')

cols = unemployment.columns
cols = cols.map(lambda x: x.replace(' ', '_') if isinstance(x, (str, unicode)) else x)
unemployment.columns = cols
cols = unemployment.columns
cols = cols.map(lambda x: x.replace('-', '_') if isinstance(x, (str, unicode)) else x)
unemployment.columns = cols

test =unemployment.loc[:,['local_authority:_district_/_unitary_(prior_to_April_2015)','Unemployment_rate___aged_16_64','Date']]
cols = ['LAD','Unemployment','Date']
test.columns = cols
unemp2011 = test.loc[(test.Date==2011),['LAD','Unemployment','Date']]

## pulling lat/longs from shapefile
sf = shapefile.Reader("/Users/joshuakaplan/lad/england_lad_2011_gen.shp") 

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

# we should make the colors based off of unemployment rate, just to learn how to do it
colors = ["#F1EEF6", "#D4B9DA", "#C994C7", "#DF65B0", "#DD1C77", "#980043"]

lad_names =[lad["name"] for lad in data.values()]
lad_lats = [lad["lats"] for lad in data.values()]
lad_longs = [lad["longs"] for lad in data.values()]
lad_unemployment= unemp2011[unemp2011['LAD'].isin(lad_names)]
col = colors*1200

source = ColumnDataSource(data=dict(
    y=lad_lats,
    x=lad_longs,
    color=col[:len(set(lad_names))],
    name=lad_names,
    unemployment=lad_unemployment.Unemployment
))

TOOLS="pan,wheel_zoom,box_zoom,reset,hover,save"
p = GMapPlot(title="LAD", plot_width=1200, plot_height=800, x_range = Range1d(), y_range = Range1d(), map_options = GMapOptions(lat=51.5074, lng=0.1278, zoom=10))
p.map_options.map_type = "terrain"
patch = Patches(xs="x", ys="y", fill_color="color", fill_alpha=0.7, line_color="black", line_width=0.5)
patches_glyph = p.add_glyph(source, patch)

p.add_tools(PanTool(), WheelZoomTool(), BoxSelectTool(), HoverTool(), ResetTool(), PreviewSaveTool())

hover = p.select_one(HoverTool)
hover.point_policy = "follow_mouse"
hover.tooltips = [
    ("Name", "@name"),
    ("(Lat, Long)", "($y, $x)"),
    ("Unemployment Rate 2011","@unemployment")
]

output_file("LADGMap.html", title="LAD GMap test", mode="cdn")
show(p)

