# -*- coding: utf-8 -*-
"""
Created on Sat Apr 16 23:34:11 2016

@author: joshuakaplan
"""
from bokeh.models import HoverTool
from bokeh.plotting import figure, show, output_file, ColumnDataSource

colors = ["#F1EEF6", "#D4B9DA", "#C994C7", "#DF65B0", "#DD1C77", "#980043"]

msoa_names =[msoa["name"] for msoa in data.values()]
msoa_lats = [msoa["lats"] for msoa in data.values()]
msoa_longs = [msoa["longs"] for msoa in data.values()]
col = colors*1200

source = ColumnDataSource(data=dict(
    x=msoa_lats,
    y=msoa_longs,
    color=col[:len(set(msoa_names))],
    name=msoa_names
))

TOOLS="pan,wheel_zoom,box_zoom,reset,hover,save"

p = figure(title="MSOA", tools=TOOLS)

p.patches('x', 'y', source=source, fill_alpha=0.7, fill_color='color',
          line_color='black', line_width=0.5)

hover = p.select_one(HoverTool)
hover.point_policy = "follow_mouse"
hover.tooltips = [
    ("Name", "@name"),
    ("(Long, Lat)", "($x, $y)"),
]

output_file("MSOA.html", title="MSOA test")
show(p)
