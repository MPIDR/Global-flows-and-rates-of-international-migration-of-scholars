
###############################################################################
# Merge smigdb from different runs/data providers: OpenAlex and Scopus
###############################################################################
#%%
import duckdb
#%%
data_providers=["openalex", "scopus"]

path_input = "./data_input/"
path_processed = "./data_processed/"
path_plots = "./plots/"


fn_country_data_enriched_1 =  f"{path_processed}{data_providers[0]}_2023_V1_scholarlymigration_country_enriched.parquet" # f'{path_processed}{data_provider}_country_enriched.parquet' #
fn_country_data_enriched_2 =  f"{path_processed}{data_providers[1]}_2023_V1_scholarlymigration_country_enriched.parquet" # f'{path_processed}{data_provider}_country_enriched.parquet' #
fn_flows_enriched_1 = f"{path_processed}{data_providers[0]}_2023_V1_scholarlymigration_countryflows_enriched.parquet"
fn_flows_enriched_2 = f"{path_processed}{data_providers[1]}_2023_V1_scholarlymigration_countryflows_enriched.parquet"

# peak inside the files:
con = duckdb.connect(':memory:')
con.execute("PRAGMA threads=4")

#%%
#fn_country_1:
con.execute(f"select * from parquet_scan('{fn_country_data_enriched_1}') OFFSET 500 LIMIT 5").fetchdf()
#%%
#fn_country_2:
con.execute(f"select * from '{fn_country_data_enriched_2}' OFFSET 500 LIMIT 5").fetchdf()
#%%
# Merge files with duckdb on iso3code and year:
q = f"""
SELECT
    upper(coalesce(df1.iso3code, df2.iso3code,df1.countrycode,df2.countrycode)) as countrycode,
    coalesce(df1.year, df2.year) as year,
    coalesce(df1.countryname, df2.countryname) as countryname,
    coalesce(df1.gdp_per_capita, df2.gdp_per_capita) as gdp_per_capita,
    coalesce(df1.population, df2.population) as population,
    df1.padded_population_of_researchers as paddedpop_openalex,
    df2.paddedpop as paddedpop_scopus,
    df1.number_of_inmigrations as inmig_openalex,
    df2.number_of_inmigrations as inmig_scopus,
    df1.number_of_outmigrations as outmig_openalex,
    df2.number_of_outmigrations as outmig_scopus,
    -- cast as float to avoid integer division:
    cast(df1.number_of_inmigrations as float) / df1.padded_population_of_researchers as inmigrate_openalex,
    cast(df2.number_of_inmigrations as float) / df2.paddedpop as inmigrate_scopus,
    cast(df1.number_of_outmigrations as float) / df1.padded_population_of_researchers as outmigrate_openalex,
    cast(df2.number_of_outmigrations as float) / df2.paddedpop as outmigrate_scopus,
    cast(df1.number_of_inmigrations as float) / df1.population as inmigrate_openalex_pop,
    cast(df2.number_of_inmigrations as float) / df2.population as inmigrate_scopus_pop,
    df1.netmigration as netmig_openalex,
    df2.netmigration as netmig_scopus,
    cast(df1.netmigration as float) / df1.padded_population_of_researchers as netmigrate_openalex,
    cast(df2.netmigration as float) / df2.paddedpop as netmigrate_scopus,
    coalesce(df1.region, df2.region) as region,
    coalesce(df1.incomelevel, df2.incomelevel) as incomelevel
FROM '{fn_country_data_enriched_1}' AS df1
FULL OUTER JOIN '{fn_country_data_enriched_2}' AS df2 ON df1.iso3code = df2.iso3code AND df1.year = df2.year
"""
dfmerged = con.execute(q).fetchdf()
dfmerged.to_csv(path_processed + f"dfmerged_{data_providers[0]}_{data_providers[1]}_country.csv")
# %%
dfmerged#.columns
# %%
dfmerged.plot.scatter(x="inmigrate_openalex", y="inmigrate_scopus")
# %%
import plotly.express as px
fig = px.scatter(dfmerged, x="inmigrate_openalex", y="inmigrate_scopus", color="year", marginal_x="histogram", marginal_y="histogram", hover_data=["countryname", "year", "paddedpop_openalex", "paddedpop_scopus"])
fig.write_html(path_plots + "inmigrate_openalex_vs_inmigrate_scopus.html")
# %%
fig
# %%

fig = px.scatter(dfmerged, x="paddedpop_openalex", y="paddedpop_scopus", color="year", marginal_x="histogram", marginal_y="histogram", hover_data=["countryname", "year", "paddedpop_openalex", "paddedpop_scopus"])
fig.write_html(path_plots + "paddedpop_openalex_vs_paddedpop_scopus.html")
fig
# %%

fig = px.scatter(dfmerged, x="inmig_openalex", y="inmig_scopus", color="year", marginal_x="histogram", marginal_y="histogram", hover_data=["countryname", "year", "paddedpop_openalex", "paddedpop_scopus"])
fig.write_html(path_plots + "inmig_openalex_vs_inmig_scopus.html")
fig
#%%

fig = px.scatter(dfmerged, x="outmig_openalex", y="outmig_scopus", color="year", marginal_x="histogram", marginal_y="histogram", hover_data=["countryname", "year", "paddedpop_openalex", "paddedpop_scopus"])
fig.write_html(path_plots + "outmig_openalex_vs_outmig_scopus.html")
fig
#%%

fig = px.scatter(dfmerged, x="gdp_per_capita", y="outmigrate_openalex", color="year", marginal_x="histogram", marginal_y="histogram", hover_data=["countryname", "year", "paddedpop_openalex", "paddedpop_scopus"])
fig.write_html(path_plots + "gdp_per_capita_vs_outmigrate_openalex.html")
fig

#%%
fig = px.scatter(dfmerged, x="netmig_openalex", y="netmig_scopus", opacity=0.2, color="year", hover_data=["countryname", "year", "paddedpop_openalex", "paddedpop_scopus"])
fig.update_xaxes(range=[-2000, 2000], dtick=1000,title="Net migration (OpenAlex)")
fig.update_yaxes(range=[-2000, 2000], dtick=1000,title="Net migration (Scopus)")
fig.write_html(path_plots + "netmig_openalex_vs_netmig_scopus.html")
fig.write_image(path_plots + "netmig_openalex_vs_netmig_scopus.pdf")
fig

#%%
fig = px.scatter(dfmerged, x="netmig_openalex", y="netmig_scopus",
#    size="paddedpop_openalex",
    opacity=0.8, color="year",
    hover_data=["countryname", "year", "paddedpop_openalex", "paddedpop_openalex"])
fig.update_traces(marker=dict( line=dict(width=0, color='DarkSlateGrey')),
          selector=dict(mode='markers'))
fig.update_xaxes(range=[-2000, 2000], dtick=1000,title="Net migration (OpenAlex)")
fig.update_yaxes(range=[-2000, 2000], dtick=1000,title="Net migration (Scopus)")
# fig.write_html(path + "netmig_openalex_vs_netmig_scopus.html")
fig.write_image(path_plots + "netmig_openalex_vs_netmig_scopus_pop.pdf")
fig
#%%


import pandas as pd
# show all columns:
pd.set_option('display.max_columns', None)
dfmerged[dfmerged["paddedpop_openalex"]>100000]
# %%
