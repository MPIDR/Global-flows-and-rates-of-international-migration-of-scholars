# Create figures for the paper ...todo...
#%%
##############################################################################
# Install packages if needed
# install duckdb:
# %pip install duckdb --upgrade --user
#%%
# to export as pdf we need python-kaleido
# (according to https://plotly.com/python/static-image-export/ )
# %conda install -c conda-forge python-kaleido
#%%
##############################################################################

#%%
# change this to "scopus" or "openalex" to switch between the two data providers:
data_provider=["openalex", "scopus"][0]

path_input = "./data_input/"
path_processed = "./data_processed/"
path_plots = "./plots/"

fn_country_enriched =  f"{path_processed}{data_provider}_2023_V1_scholarlymigration_country_enriched.parquet" 
fn_flows_enriched = f"{path_processed}{data_provider}_2023_V1_scholarlymigration_countryflows_enriched.parquet"


#%%
import pandas as pd
import duckdb

con = duckdb.connect(':memory:')
# show all columns and rows:
pd.set_option('display.max_columns', 80)
pd.set_option('display.max_rows', 70)
print("duckdb version:", duckdb.__version__)
#%%

q = f"""SELECT * FROM '{fn_country_enriched}'"""
dfcountry = con.execute(q).df()
print(dfcountry.columns)

q = f"""SELECT * FROM '{fn_flows_enriched}'"""
dfflow = con.execute(q).df()
print(dfflow.columns)
#%%
#%%

minpop = data_provider=="openalex" and 60000 or 40000

#%%
#select 15 countries with the highest number of avg_paddedpop_min:
countrylist = dfflow[dfflow.year==2018].groupby(
    ["countrynamefrom"]).mean("paddedpopfrom").sort_values(
        by='paddedpopfrom', ascending=False).head(15)#.sort_values("paddedpopfrom")#.countrynamefrom.unique()
countrylist = list(countrylist.index)
print(countrylist)
# scopus:
# ['China', 'United States', 'India', 'Japan', 'Germany', 'United Kingdom', 'Brazil', 'Italy', 'France', 'Spain', 'Korea, Rep.', 'Russian Federation', 'Canada', 'Australia', 'Iran, Islamic Rep.']
# openalex:
# ['United States', 'China', 'Japan', 'India', 'Germany', 'Brazil', 'United Kingdom', 'France', 'Italy', 'Spain', 'Russian Federation', 'Canada', 'Indonesia', 'Iran, Islamic Rep.', 'Australia']
# hardcode the scopus list, so the figures are more comparable:
countrylist = ['China', 'United States', 'India', 'Japan', 'Germany', 'United Kingdom', 'Brazil', 'Italy', 'France', 'Spain', 'Korea, Rep.', 'Russian Federation', 'Canada', 'Australia', 'Iran, Islamic Rep.']
#%%
#df = dfflow[(dfflow.year==2018) & (dfflow.avg_paddedpop_min>minpop)]
df = dfflow[(dfflow.year==2018) & (dfflow.countrynamefrom.isin(countrylist)) & (dfflow.countrynameto.isin(countrylist))]
df = df[['countrynamefrom', 'countrynameto', 'n_migrations', 'normalized_migration1', 'normalized_migration2']]
df.columns = ['country_source', 'country_destination', 'number_migrations', 'normalized_migration1', 'normalized_migration2']
print(df.country_source.nunique(),df.country_source.unique())

# df
# %pip install networkx --upgrade --user
# %pip install matplotlib --upgrade --user
# %pip install seaborn --upgrade --user


# ###############################################################################
# # plot as network graph:
# import networkx as nx
# import matplotlib.pyplot as plt

# # Assuming your DataFrame is named 'df'
# G = nx.from_pandas_edgelist(df, source='country_source', target='country_destination', edge_attr='number_migrations')

# # Set node positions
# pos = nx.spring_layout(G)

# # Set edge weights and widths
# edge_weights = [d['number_migrations'] for u, v, d in G.edges(data=True)]
# edge_widths = [w/500 for w in edge_weights]

# pos = nx.spring_layout(G, k=0.15, iterations=20)
# # Draw nodes and edges
# nx.draw_networkx_nodes(G, pos, node_size=300, node_color='lightblue')
# nx.draw_networkx_labels(G, pos, font_size=6, font_family='sans-serif')
# nx.draw_networkx_edges(G, pos, edge_color='gray', width=edge_widths)

# # Add edge labels
# edge_labels = nx.get_edge_attributes(G, 'number_migrations')
# # nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_size=3)

# plt.axis('off')
# plt.show()

# %%
###############################################################################
# plot flows as heatmap:

import seaborn as sns
import plotly.express as px

# rename the values "Russian Federation" to "Russia" and "United States" to "USA" and "United Kingdom" to "UK"
# (saves space in the plot)
df['country_source'] = df['country_source'].replace('Russian Federation', 'Russia')
df['country_source'] = df['country_source'].replace('United States', 'USA')
df['country_source'] = df['country_source'].replace('United Kingdom', 'UK')
df['country_source'] = df['country_source'].replace('Iran, Islamic Rep.', 'Iran')
df['country_destination'] = df['country_destination'].replace('Russian Federation', 'Russia')
df['country_destination'] = df['country_destination'].replace('United States', 'USA')
df['country_destination'] = df['country_destination'].replace('United Kingdom', 'UK')
df['country_destination'] = df['country_destination'].replace('Iran, Islamic Rep.', 'Iran')


# Assuming your DataFrame is named 'df'
pivot_df = df.pivot(index='country_source', columns='country_destination', values='number_migrations')
pivot_df_normalized = df.pivot(index='country_source', columns='country_destination', values='normalized_migration2')
# y-axis label: source country
# x-axis label: destination country
# font-size of the countries: 5

vmax = pivot_df.mean().mean() * 2.4
print("vmax:", vmax)

bb = sns.heatmap(pivot_df_normalized, cmap='Blues', linewidths=0.5, annot=pivot_df, fmt='g',
   annot_kws={"size": 5}, square=True, vmin=0, vmax=vmax, #cbar_kws={'label': 'Number of migrants'},
    cbar=False, 
)#xticklabels=5, yticklabels=5)
bb.set_xticklabels(bb.get_xticklabels(), rotation=90, horizontalalignment='right')
bb.set_yticklabels(bb.get_yticklabels(), rotation=0, horizontalalignment='right')
bb.tick_params(labelsize=7)
# set font of title:
bb.title.set_fontsize(10)

bb.set_title('Flow of scholarly migrants in 2018')
bb.set_xlabel('Destination country')
bb.set_ylabel('Source country')

# plt.show()

fig = bb.get_figure()
fig.subplots_adjust(bottom=0.29,left=0.01)
fn =f'{path_plots}{data_provider}_flow_heatmap.pdf'
bla = fig.savefig(fn,dpi=300)
# %%

###############################################################################
# plot flow from one country to some other countries as line plot:

for country_source in ["United States"]:# ["Germany", "United Kingdom", "United States", "Japan", "China", "Brazil"]:
    # select the 5 countries with the highest number of migrants from the source country:
    destinations = dfflow[(dfflow.countrynamefrom==country_source)&(dfflow.avg_paddedpop_min>1000)].groupby(
        'countrynameto').agg({'n_migrations':'sum'}).sort_values("n_migrations", ascending=False).head(5).index
    print(destinations)
    # select the data for the source country and the 5 countries with the highest number of migrants:
    df1tomany = dfflow[(dfflow.countrynamefrom==country_source)&(dfflow.countrynameto.isin(destinations))]

    # df1tomany = dfflow[(dfflow.countrynamefrom==country_source)&(dfflow.avg_paddedpop_min>200000)]
    # add values for "other countries" to the dataframe (sum of all other countries):



    df1tomany.sort_values(by=['year'], inplace=True)
    df1tomany.rename({'countrynamefrom':'country_source', 'countrynameto':'country_destination'}, axis=1, inplace=True)
    fig1 = px.line(df1tomany, x="year", y="n_migrations",
                            color="country_destination",log_y=True,line_shape="linear", render_mode="svg",
                            hover_name="year", #text="iso3codeto",#range_y=[0.015,0.075],
                            hover_data=[f"n_migrations",f"normalized_migration1",f"normalized_migration2"],
)
    fig1.update_traces(textposition='top center')
    fig1.update_layout(title=f"Flow of scholarly migrants from {country_source} to other countries",
                        xaxis_title="Year",    
                        yaxis_title=f"Number of scholarly migrants from {country_source} to ...",
                        font=dict(size=12, color="#7f7f7f")
                      )
    # change y-axis to show the number of migrants in millions:
    fig1.update_yaxes(tickformat=".0f")
    fig1.show()
    fig1.write_image(f"{path_plots}{data_provider}_flow_over_time_{country_source}_to_othercountries.pdf")

# %%

###############################################################################
# plot flow from some countries to a specific country as a line plot:

for country_destination in  ["United States"]:#["Germany", "United Kingdom", "United States", "Japan", "China", "Brazil"]:

    origins = dfflow[(dfflow.countrynameto==country_destination)&(dfflow.avg_paddedpop_min>1000)].groupby(
        'countrynamefrom').agg({'n_migrations':'sum'}).sort_values("n_migrations", ascending=False).head(5).index
    print(origins)
    # select the data for the source country and the 5 countries with the highest number of migrants:
    df1tomany = dfflow[(dfflow.countrynameto==country_destination)&(dfflow.countrynamefrom.isin(origins))]
    df1tomany.sort_values(by=['year'], inplace=True)
    df1tomany.rename({'countrynamefrom':'country_source', 'countrynameto':'country_destination'}, axis=1, inplace=True)
    fig1 = px.line(df1tomany, x="year", y="n_migrations",
                            color="country_source",log_y=True,line_shape="linear", render_mode="svg",
                            hover_name="year", # text="iso3codefrom",#range_y=[0.015,0.075],
                            hover_data=[f"n_migrations",f"normalized_migration1",f"normalized_migration2"])
    fig1.update_traces(textposition='top center')
    fig1.update_layout(title=f"Flow of scholarly migrants from other countries to {country_destination}",
                        xaxis_title="Year",
                        yaxis_title=f"Number of scholarly migrants from ... to {country_destination}",
                        font=dict(size=12, color="#7f7f7f")
                        )
    fig1.show()
    fig1.write_image(f"{path_plots}{data_provider}_flow_over_time_othercountries_to_{country_destination}.pdf")


# %%
###############################################################################
# plot single country numbers as line plot: (not in paper)

dfcountry1 = dfcountry[dfcountry["avg_paddedpop"]>150000].sort_values(by=["countrycode","year"])

inoutlist = ["out","in"]
for inout in inoutlist:
    fig1 = px.line(dfcountry1, x="year", y=f"{inout}migrationrate",
                     color="countryname",log_y=True,line_shape="linear", render_mode="svg",
                  hover_name="year",# text="iso2code",#range_y=[0.015,0.075],
                  hover_data=[f"number_of_inmigrations",f"number_of_outmigrations",f"netmigration","gdp_per_capita","paddedpop"])
    fig1.show()
    #save as pdf:
    # fig1.write_image(f"{path_plots}{data_provider}_country_migration_rates_{inout}.pdf")

inout = "net"
fig1 = px.line(dfcountry1, x="year", y=f"{inout}migrationrate",
                 color="countryname",log_y=False,line_shape="linear", render_mode="svg",
              hover_name="year",# text="iso2code",#range_y=[0.015,0.075],
              hover_data=[f"number_of_inmigrations",f"number_of_outmigrations",f"netmigration","gdp_per_capita","paddedpop"])
fig1.show()
#save as pdf:
# fig1.write_image(f"{path_plots}{data_provider}_country_migration_rates_{inout}.pdf")



# %%
###############################################################################
# plot single country numbers as world-maps:
###############################################################################

startyear = 2013
endyear = 2017

#%%
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

#%%

#%%

###############################################################################
# Aggregate single country rates over a perios of some years:

dfmigrouped = dfcountry[(dfcountry["year"] >= startyear ) & (dfcountry["year"] <= endyear )].groupby("countrycode").agg(
        {
            "number_of_inmigrations":"mean",
            "number_of_outmigrations":"mean",
            "netmigration":"mean",
            #"padded_population_of_researchers":"mean",
            "paddedpop":"mean",
            "year":"mean",
            "countryname":"first",
            "iso3code":"first"
        })
# calculate rates:
dfmigrouped["outmigrationrate"] = dfmigrouped["number_of_outmigrations"]/dfmigrouped["paddedpop"]
dfmigrouped["inmigrationrate"] = dfmigrouped["number_of_inmigrations"]/dfmigrouped["paddedpop"]
dfmigrouped["netmigrationrate"] = dfmigrouped["netmigration"]/dfmigrouped["paddedpop"]
dfmigrouped["Net migration rate"] = dfmigrouped["netmigration"]/dfmigrouped["paddedpop"]
worldwide_total_researchers_in_period = dfmigrouped["paddedpop"].sum()
print(f"{worldwide_total_researchers_in_period=}")

#%%
###############################################################################
# plot single country rates (in- out- net-) as world-maps:
hover_data={
            'countryname':True,
            'outmigrationrate':True, 
            # 'padded_population_of_researchers':True,
            'paddedpop':True,
            'year':True,
        }

for inout in ["outmigrationrate","inmigrationrate","netmigrationrate"]:
    range_color = {"outmigrationrate":[0.0,0.1], "inmigrationrate":[0.0,0.1], "netmigrationrate":[-0.015,0.015]}[inout]
    
    if inout in ["outmigrationrate","inmigrationrate"]:
        color_continuous_scale = [[0.0, 'rgb(170,220,249,0.5)'],
          [.04, 'rgb(140,180,215,0.5)'],
          [.12, 'rgb(185,225,115,0.5)'],
          [.30, 'rgb(51,160,44,0.5)'],
          [1.00, 'rgb(227,5,5,0.5)']
         ]
    else:
        color_continuous_scale = "Temps_R"
        #  [
        #   [-0.5, 'rgb(170,220,249,0.5)'],
        #   [-0.05, 'rgb(140,180,215,0.5)'],
        #   [0.05, 'rgb(51,160,44,0.5)'],
        #   [0.5, 'rgb(227,5,5,0.5)']
        #   ]
    fig = px.choropleth(dfmigrouped.reset_index(),
              locations='iso3code',
              locationmode='ISO-3',
              color=inout,#'padded_population_of_researchers',
              range_color=range_color,
              color_continuous_scale = "Temps_R",#color_continuous_scale,
              projection="robinson",
              scope='world',
              hover_data =hover_data
              )

    fig.update_layout(title={
            'text': f"<b>{inout.capitalize()} migration rates (per 1,000 scholars), {startyear} - {endyear}",
            'y':0.95,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'},
            font=dict(size=12, color="#7f7f7f")
            )
    
    # choose angle and center so we see everything except antarctica:
    fig.update_geos(
        visible=False,showcountries=True,
        center=dict(lon=7, lat=12),
        lataxis_range=[-74,62], lonaxis_range=[-142, 167]
    )
    # remove the border around the world:
    fig.update_traces(marker_line_width=0)
    # fig.show()

    #save as pdf:
    
    fig.update_layout(width =900, height=440, font_size=13,
                  font_family="Helvetica",
                  margin_l=0, margin_t=12, margin_b=1, margin_r=0)
    # fig.write_image(f"{path_plots}{data_provider}_worldmap_country_migration_rates_{inout}.pdf")

#%%
###############################################################################
# Net migration rates:

fig = px.choropleth(dfmigrouped.reset_index(),
          locations='iso3code',
          locationmode='ISO-3',
          color="Net migration rate",
          range_color=[-0.015,0.015],
          color_continuous_scale = "Temps_R",#color_continuous_scale,
          projection="robinson",
          scope='world',
          hover_data =hover_data
          )

fig.update_layout(coloraxis_colorbar=dict(
        yanchor="top", 
        y=0.95, x=0,
        ticks="outside",
        ticksuffix="",
        title=""))
fig.update_layout(title={
        'text': f"<b>Net migration rate, {startyear} - {endyear}",
        'y':0.95,
        'x':0.5,
        'xanchor': 'center',
        'yanchor': 'top'},
        font=dict(size=12),#, color="#7f7f7f")
        legend_title_text=""
    )
    
# choose angle and center so we see everything except antarctica:
fig.update_geos(
    visible=False,showcountries=True,
    center=dict(lon=4, lat=19),
    lataxis_range=[-74,76], lonaxis_range=[-142, 167]
)
# remove the border around the world:
fig.update_traces(marker_line_width=0)
# fig.show()

#save as pdf:

fig.update_layout(width =900, height=490, font_size=13,
              font_family="Helvetica",
              margin_l=0, margin_t=12, margin_b=1, margin_r=0)
fig.write_image(f"{path_plots}{data_provider}_worldmap_country_migration_rates_net3.pdf")


##############################
# Map of the world with the number of scholars per country (not used in the paper):
#%%

conf = {"scrollZoom": False}
fig = px.choropleth(dfmigrouped.reset_index(),
              locations='iso3code',
              locationmode='ISO-3',
              color='paddedpop',
            #   range_color=[.0,100],
              color_continuous_scale = [[0.0, 'rgb(170,220,249,0.5)'],
                      [.01, 'rgb(140,180,215,0.5)'],
                      [.12, 'rgb(185,225,115,0.5)'],
                      [.30, 'rgb(51,160,44,0.5)'],
                      [1.00, 'rgb(227,5,5,0.5)']
                     ],
              projection="robinson",
              scope='world',
              hover_data =hover_data
              )

fig.update_layout(title={
        'text': f"<b>Average number of scholars per country </b><br> years {startyear} - {endyear}",
        'y':0.898,
        'x':0.5,
        'xanchor': 'center',
        'yanchor': 'top'},
        legend_title="",
)

fig.update_traces(
    hovertemplate="<br>".join([
        "%{customdata[0]}",
        "Scholars: %{customdata[2]}",
        "Emigration rate: %{customdata[1]:.3f}",
    ])
)

# choose angle and center so we see everything except antarctica:
fig.update_geos(
    visible=False,showcountries=True,
    center=dict(lon=7, lat=12),
    lataxis_range=[-74,62], lonaxis_range=[-142, 167]
)
# remove the border around the world:
fig.update_traces(marker_line_width=0)
fig.write_image(f"{path_plots}{data_provider}_worldmap_country_migration_rates_{inout}_2.pdf")


#/Figure_1_{startyear}-{endyear}.html",config=conf)
# %%
# create pdf:
fig.update_layout(width =900, height=440, font_size=13,
                  font_family="Helvetica",
                  margin_l=0, margin_t=12, margin_b=1, margin_r=0)

# fig.write_image(f"{path_plots}{data_provider}_worldmap_country_population_{startyear}-{endyear}.pdf")

# %%