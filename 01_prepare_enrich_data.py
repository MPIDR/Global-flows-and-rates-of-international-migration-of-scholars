# Description: read and merge gdp data and population numbers
# from the worldbank api. This also adds different country codes
# and country names to the data.
#
# The output can be used for plotting and publication.
# 
##############################################################################
# Install packages if needed
# install duckdb:
# %pip install duckdb --upgrade --user
#%%
import os

# choose which source data provider to use:
data_provider=["openalex", "scopus"][1]

path_input = "./data_input/"
path_processed = "./data_processed/"
path_plots = "./plots/"

# create folders if they don't exist:
os.makedirs(path_input, exist_ok=True)
os.makedirs(path_processed, exist_ok=True)
os.makedirs(path_plots, exist_ok=True)

# input data files:
fn_country_data = f"{path_input}{data_provider}_2024_V1_scholarlymigration_country.parquet"
fn_flow_data = f"{path_input}{data_provider}_2024_V1_scholarlymigration_countryflows.parquet"

# output data files:
fn_country_data_enriched =  f"{path_processed}{data_provider}_2024_V1_scholarlymigration_country_enriched.parquet" # f'{path_processed}{data_provider}_country_enriched.parquet' #
fn_flows_enriched = f"{path_processed}{data_provider}_2024_V1_scholarlymigration_countryflows_enriched.parquet"

# temporary
# fn_country_data = fn_country_data.replace(".parquet",".csv")
# fn_flow_data = fn_flow_data.replace(".parquet",".csv")

#%%
import requests
import pandas as pd
import duckdb

#%%
###############################################################################
# Download country data from worldbank:
# (iso2code, iso3code, countryname, region, incomelevel)
###############################################################################
#  get a country information dataframe from worldbank:
url = "http://api.worldbank.org/v2/country/all?format=json&per_page=30000"
r = requests.get(url)
data = r.json()
dfcountries = pd.json_normalize(r.json()[1])
#%%
dfcountries.rename(columns={
    "id":"iso3code",
    "region.value":"region_value",
    "incomeLevel.id":"incomeLevel_id"},inplace=True)
#%%
# save copy of dfcountries:
dfcountries.to_csv(path_input+"dfcountries.csv",index=False)

#%%
###############################################################################
# Read and merge bibliometric data with country data:
###############################################################################

# The data sources use different country codes:
if data_provider == "scopus":
    col_countrycode = "iso3Code"
elif data_provider == "openalex":
    col_countrycode = "iso2Code"

# take a look at the country data:
con = duckdb.connect(':memory:')
query = f"""SELECT * 
FROM '{fn_country_data}' AS dfsmig
LIMIT 5;"""
con.execute(query).fetchdf()
#%%
# use duckdb to merge bibliometric data with country data:

query = f"""SELECT 
dfsmig.year as year,
dfsmig.countrycode as countrycode,
dfsmig.padded_population_of_researchers,
dfsmig.number_of_inmigrations,
dfsmig.number_of_outmigrations,
dfsmig.netmigration,
outmigrationrate, inmigrationrate, netmigrationrate,

dfcountries.iso2Code as iso2code,
dfcountries.iso3Code as iso3code,
dfcountries.name as countryname, 
dfcountries.region_value as region, 
dfcountries.incomeLevel_id as incomelevel,
-- calculate the average number of scholars per country:
avg(paddedpop) OVER (PARTITION BY countrycode) as avg_paddedpop
FROM '{fn_country_data}' AS dfsmig
LEFT JOIN dfcountries ON dfcountries.{col_countrycode} = upper(countrycode)
;"""
dfsmig = con.execute(query).fetchdf()
print(dfsmig.sample(3))
#%%
###############################################################################
# Download and merge yearly gdp and population size data from worldbank:
###############################################################################


######################################################
# download gdp data from worldbank:
# https://data.worldbank.org/indicator/NY.GDP.PCAP.CD?locations=DE
# and save it as csv in the data folder


#NY.GDP.PCAP.PP.KD    GDP per capita, PPP (constant 2017 international $)
# SP.POP.TOTL         Population, total
url = "https://api.worldbank.org/v2/country/all/indicator/NY.GDP.PCAP.CD?format=json&strdate=1995-01-01&per_page=30000"
r = requests.get(url)
data = r.json()
dfgdp1 = pd.json_normalize(r.json()[1])
#%%
# create a wide dataframe with years as columns and the "value" column as values:
dfgdp1_wide = pd.pivot_table(dfgdp1, values='value', index=['countryiso3code'], columns=['date'])

for iyear in range(2022,2024):
    if str(iyear) not in dfgdp1_wide.columns:
        dfgdp1_wide[str(iyear)] = pd.NA
dfgdp1_wide
#%%
# forward fill the missing values:
dfgdp1_wide = dfgdp1_wide.ffill(axis=1)
dfgdp1_wide
#%%
# melt the dataframe back to long format:
dfgdp1_ffilled = pd.melt(dfgdp1_wide.reset_index(),id_vars=["countryiso3code"],value_vars=[str(x) for x in range(1996,2023)])
dfgdp1_ffilled
#%%

######################################################
# merge with dfsmig (bibliometric data, country level):
query = f"""SELECT
dfsmig.*,
dfgdp1_ffilled.value as gdp_per_capita
FROM dfsmig
LEFT JOIN dfgdp1_ffilled ON dfgdp1_ffilled.countryiso3code = dfsmig.iso3code
AND dfgdp1_ffilled.date = dfsmig.year
;"""
dfsmiggdp = con.execute(query).fetchdf()
dfsmiggdp.sample(4)
#%%
dfsmiggdp.info()


#%%
###############################################################################
# Download and merge population data from worldbank:
#
# Do the same as with the gdp data, but for the population data:
url = "https://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL?format=json&strdate=1995-01-01&per_page=30000"

r = requests.get(url)
data = r.json()
dfpop1 = pd.json_normalize(r.json()[1])
dfpop1_wide = pd.pivot_table(dfpop1, values='value', index=['countryiso3code'], columns=['date'])

for iyear in range(2022,2024):
    if str(iyear) not in dfpop1_wide.columns:
        dfpop1_wide[str(iyear)] = pd.NA
#%%
dfpop1_wide = dfpop1_wide.ffill(axis=1)
dfpop1_ffilled = pd.melt(dfpop1_wide.reset_index(),id_vars=["countryiso3code"],value_vars=[str(x) for x in range(1996,2024)])
dfpop1_ffilled.to_csv(path_input+"world_bank_popuplation_ffilled.csv",index=False)
#%%
# dfpop1_ffilled.dropna(inplace=True)
# dfsmiggdp.dropna(subset=["year","countryiso3code"],inplace=True)
#%%
# merge with dfsmiggdp:
query = f"""
SELECT
  dfsmiggdp.*,
  dfpop1_ffilled.value as population
FROM dfsmiggdp
  LEFT JOIN dfpop1_ffilled ON dfpop1_ffilled.countryiso3code = dfsmiggdp.iso3code
    AND dfpop1_ffilled.date = dfsmiggdp.year
;"""
dfsmiggdppop = con.execute(query).fetchdf()
dfsmiggdppop.sample(4)
#%%
dfsmiggdppop.info()


#%%
###############################################################################
# save to parquet and csv:
###############################################################################
#%%
# save to parquet:
query = f"""Copy 
(SELECT * FROM dfsmiggdppop
  WHERE year >1997 and year <2021
  ORDER BY iso3code, year)
TO '{fn_country_data_enriched}';"""
con.execute(query)
#%%
# save to csv:
query = f"""Copy 
(SELECT * FROM dfsmiggdppop
  WHERE year >1997 and year <2021
  ORDER BY iso3code, year)
TO '{fn_country_data_enriched[:-8]}.csv'  (HEADER, DELIMITER ',');"""
con.execute(query)
#%%


###############################################################################
###############################################################################
# Enrich/merge the flow data with country data:
###############################################################################

###############################################################################
#%%
# load flow-data from b041:
# dfccf: country-country-flow
q=f"""SELECT * FROM '{fn_flow_data}' limit 10;"""
dfccf = con.execute(q).df()
dfccf
#%%


query = f"""
SELECT 
  upper(migrationfrom) as migrationfrom,
  upper(migrationto) as migrationto,
  n_migrations,
  netmigrations,
  year as year,
  -- (migrationyearpadding) as year,
  --* EXCLUDE (avgyears_in_previous_country, avgyears_in_previous_country_back, avgauthorage, avgauthorage_back)
FROM '{fn_flow_data}'
"""
dfccf = con.execute(query).df()
dfccf
#%%
print(dfccf.columns)
# Index(['column0', 'migrationfrom', 'migrationto', 'migrationyearceil',
#        'n_migrations'],
#       dtype='object')
print(dfccf.count())
dfccf.sample(3)
#%%

###############################################################################
# merge the information from country-year-data with flow-data:
# (country names, etc.)
query = f"""SELECT ccf.n_migrations as n_migrations,
    --ccf.migrationyearceil as year,
    ccf.year,
    cfrom.countryname as countrynamefrom, 
    cto.countryname as countrynameto,
    cfrom.region as regionfrom,
    cto.region as regionto,
    cfrom.incomelevel as incomelevelfrom,
    cto.incomelevel as incomelevelto,
    cfrom.gdp_per_capita as gdp_per_capitafrom,
    cto.gdp_per_capita as gdp_per_capitato,
    cfrom.population as populationfrom,
    cto.population as populationto,
    cfrom.iso3code as iso3codefrom,
    cto.iso3code as iso3codeto,
    cfrom.padded_population_of_researchers as paddedpopfrom,
    cto.padded_population_of_researchers as paddedpopto,
    cfrom.number_of_inmigrations as number_of_inmigrationsfrom,
    cto.number_of_inmigrations as number_of_inmigrationsto,
    cfrom.number_of_outmigrations as number_of_outmigrationsfrom,
    cto.number_of_outmigrations as number_of_outmigrationsto,
    cfrom.avg_paddedpop as avg_paddedpopfrom,    --avg(cfrom.paddedpop) OVER (PARTITION BY cfrom.countrycode) as avg_paddedpopfrom,
    cto.avg_paddedpop as avg_paddedpopto,        --avg(cto.paddedpop) OVER (PARTITION BY cto.countrycode) as avg_paddedpopto,
    CASE WHEN avg_paddedpopfrom>avg_paddedpopto THEN avg_paddedpopto ELSE avg_paddedpopfrom END AS avg_paddedpop_min,
    100000000.0 *ccf.n_migrations/(cto.number_of_inmigrations * cfrom.number_of_outmigrations) as normalized_migration1,
    100000000000.0 * ccf.n_migrations/(paddedpopfrom * paddedpopto) as normalized_migration2,
from dfccf as ccf
LEFT JOIN dfsmiggdppop as cfrom ON cfrom.{col_countrycode} = ccf.migrationfrom AND cfrom.year = ccf.year--migrationyearceil
LEFT JOIN dfsmiggdppop as cto ON cto.{col_countrycode} = ccf.migrationto AND cto.year = ccf.year--migrationyearceil
WHERE cfrom.{col_countrycode} NOT NULL AND cto.{col_countrycode} NOT NULL
 AND ccf.year >1997 and ccf.year <2019
"""
dfccf2 = con.execute(query).df()
print(dfccf2.columns)
dfccf2[dfccf2.avg_paddedpop_min>200000]
#%%
# Save enriched flow data to parquet file:
q = f"""Copy dfccf2 To '{fn_flows_enriched}';"""
con.execute(q)

q = f"""Copy dfccf2 To '{fn_flows_enriched[:-8]}.csv' (HEADER, DELIMITER ',');"""
con.execute(q)

# %%
