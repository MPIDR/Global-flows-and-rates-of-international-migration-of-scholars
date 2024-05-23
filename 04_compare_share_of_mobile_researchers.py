#


#%%
import pandas as pd
import numpy as np
import duckdb
import pickle
#%%
import buckaroo
#%%

# Load the data: 
# * orcid employment history (computed by us from the ORCID data, ~800MB, not in this repository due to its size)
# * scopus residence countries of all authors by our mode based method (data is restricted by scopus)
# * OpenAlex residence countries of all authors by our mode based method (data is open, but not in this repository due to its size (~4GB))
# email theile@mpidr.de for the data


fn_orcid = r"G:\bib\orcid\read_orcid_prod6.parquet"

fn_scopus = r"G:\bib\scopus_2023_10\run_2024_03_14\sc2310_dfcorrected.pickle"
fn_scopus = r"G:\bib\scopus_2023_10\run_2024_03_14\author_stats_3.parquet"
fn_scopus = r"G:\bib\scopus_2023_10\authorshiprec202310.parquet"

fn_oa = r"G:\bib\openalex_2023_03\run_2023_02_28\oa2302_dfcorrected.pickle"
fn_oa = r"G:\bib\openalex_2023_03\run_2023_02_28\oa2302_authorshiprecords.sorted.parquet_predicted_country_filtered.parquet"

q = f"""SELECT * FROM '{fn_orcid}' LIMIT 5"""
duckdb.query(q).df()
# %%
# load scopus:
# dfscopus = pd.read_pickle(fn_scopus)

q = f"""SELECT * FROM '{fn_oa}' LIMIT 50"""
duckdb.query(q).df()
#%%
dfscopus.head()
#%%
dfscopus.shape
# %%
dfoa = pd.read_pickle(fn_oa)
dfoa.head()

# %%
dfoa.shape
# %%
# use SQL to count the share of mobile researchers
# (authors with more than one country in their residence history)
q = f"""SELECT scopus_author_id, pubyear, countrycode,
    1.0/count(DISTINCT countrycode) OVER (PARTITION BY scopus_author_id, pubyear) as fraction_of_countries_per_year,
    count(DISTINCT countrycode) OVER (PARTITION BY scopus_author_id) as number_of_countries_per_author,
    --number of years:
    count(DISTINCT pubyear) OVER (PARTITION BY scopus_author_id) as number_of_active_years,

    FROM '{fn_scopus}'
    WHERE pubyear > 1996 AND pubyear < 2025 AND countrycode IS NOT NULL
    GROUP BY scopus_author_id, pubyear, countrycode
    --ORDER BY scopus_author_id, pubyear
"""
dfsco2 = duckdb.query(q).df()
#%%
dfsco2.head(5000)
#%%
# use SQL to group by country and pubyear
q = f"""SELECT countrycode, pubyear, 
    count(DISTINCT scopus_author_id) as number_of_authors,
    SUM(fraction_of_countries_per_year) as share_of_authors,
    sum(CASE WHEN number_of_countries_per_author > 1 THEN 1 ELSE NULL END) as number_of_mobile_authors,
    SUM(CASE WHEN number_of_countries_per_author > 1 THEN fraction_of_countries_per_year ELSE 0 END) as sum_share_of_mobile_authors,
    (sum(CASE WHEN number_of_countries_per_author > 1 THEN fraction_of_countries_per_year ELSE 0 END) / sum(fraction_of_countries_per_year)) as share_of_mobile_authors,
    (sum(CASE WHEN number_of_countries_per_author > 1 THEN fraction_of_countries_per_year ELSE 0 END) / sum(fraction_of_countries_per_year)) as share_of_mobile_authors
    
    FROM dfsco2
    WHERE pubyear > 1996 AND pubyear < 2025 AND countrycode IS NOT NULL
    GROUP BY countrycode, pubyear
"""
dfsco3 = duckdb.query(q).df()
dfsco3.head()
#%%
dfsco2.head(2005).tail(18)
#%%
# Same thing for OpenAlex (scopus_author_id->author_id)
q = f"""SELECT author_id, pubyear, countrycode,
    1.0/count(DISTINCT countrycode ) OVER (PARTITION BY author_id, pubyear) as fraction_of_countries_per_year,
    count(DISTINCT countrycode) OVER (PARTITION BY author_id) as number_of_countries_per_author,

    FROM '{fn_oa}'
    WHERE pubyear > 1996 AND pubyear < 2025 AND countrycode IS NOT NULL
    GROUP BY author_id, pubyear, countrycode
"""
dfoa2 = duckdb.query(q).df()
dfoa2.head()
#%%
q =  f"""SELECT countrycode, pubyear, 
    count(DISTINCT author_id) as number_of_authors,
    SUM(fraction_of_countries_per_year) as share_of_authors,
    sum(CASE WHEN number_of_countries_per_author > 1 THEN 1 ELSE NULL END) as number_of_mobile_authors,
    SUM(CASE WHEN number_of_countries_per_author > 1 THEN fraction_of_countries_per_year ELSE 0 END) as sum_share_of_mobile_authors,
    (sum(CASE WHEN number_of_countries_per_author > 1 THEN fraction_of_countries_per_year ELSE 0 END) / sum(fraction_of_countries_per_year)) as share_of_mobile_authors
    
    FROM dfoa2
    WHERE pubyear > 1996 AND pubyear < 2025 AND countrycode IS NOT NULL
    GROUP BY countrycode, pubyear
"""
dfoa3 = duckdb.query(q).df()


# %%
dfoa3
# %%
#now the same thing with ORCID employment history:
fnorcid = r"G:\bib\orcid\orcid2403_employment_history_in_asr_form.parquet"
# q = f"""SELECT * FROM  '{fn}'"""
q = f"""SELECT orchid as author_id,
 year as pubyear,
 organizationcountry as countrycode
 FROM '{fnorcid}'
 """
 
dforcid = duckdb.query(q).df()
dforcid.head(100)
# %%

# Same thing for OpenAlex (scopus_author_id->author_id)
q = f"""SELECT author_id, pubyear, countrycode,
    1.0/count(DISTINCT countrycode ) OVER (PARTITION BY author_id, pubyear) as fraction_of_countries_per_year,
    count(DISTINCT countrycode) OVER (PARTITION BY author_id) as number_of_countries_per_author,

    FROM dforcid
    WHERE pubyear > 1996 AND pubyear < 2025 AND countrycode IS NOT NULL
    GROUP BY author_id, pubyear, countrycode
"""
dforcid2 = duckdb.query(q).df()
dforcid2.head(100)
#%%
q =  f"""SELECT countrycode, pubyear, 
    count(DISTINCT author_id) as number_of_authors,
    SUM(fraction_of_countries_per_year) as share_of_authors,
    sum(CASE WHEN number_of_countries_per_author > 1 THEN 1 ELSE NULL END) as number_of_mobile_authors,
    SUM(CASE WHEN number_of_countries_per_author > 1 THEN fraction_of_countries_per_year ELSE 0 END) as sum_share_of_mobile_authors,
    (sum(CASE WHEN number_of_countries_per_author > 1 THEN fraction_of_countries_per_year ELSE 0 END) / sum(fraction_of_countries_per_year)) as share_of_mobile_authors
    
    FROM dforcid2
    WHERE pubyear > 1996 AND pubyear < 2025 AND countrycode IS NOT NULL
    GROUP BY countrycode, pubyear
"""
dforcid3 = duckdb.query(q).df()
dforcid3
# %%
# Merge the three dataframes:
# %%
fn_countries = r"./data_input/dfcountries.csv"
q = f"""
SELECT
dfcountries.iso3code as countrycode,dfcountries.iso2code,dfcountries.name as countryname,
dfsco3.pubyear as year,
dfsco3.share_of_authors as number_of_authors_scopus,
dfsco3.share_of_mobile_authors as share_of_mobile_authors_scopus,
dfsco3.sum_share_of_mobile_authors as n_mobile_authors_scopus,
dfoa3.share_of_authors as number_of_authors_openalex,
dfoa3.share_of_mobile_authors as share_of_mobile_authors_openalex,
dfoa3.sum_share_of_mobile_authors as n_mobile_authors_openalex,
dforcid3.share_of_authors as number_of_authors_orcid,
dforcid3.share_of_mobile_authors as share_of_mobile_authors_orcid,
dforcid3.sum_share_of_mobile_authors as n_mobile_authors_orcid

FROM dfsco3
LEFT JOIN '{fn_countries}' as dfcountries on dfsco3.countrycode = dfcountries.iso3code
Join dfoa3 on dfcountries.iso2code = dfoa3.countrycode and dfsco3.pubyear = dfoa3.pubyear
LEFT JOIN dforcid3 on dfcountries.iso2code = dforcid3.countrycode and dfsco3.pubyear = dforcid3.pubyear
--LEFT JOIN dfoa3 on dfcountries.iso2code = dfoa3.countrycode and dfsco3.pubyear = dfoa3.pubyear
WHERE dfsco3.pubyear > 1997 AND dfsco3.pubyear < 2020
ORDER BY (AVG(dfsco3.share_of_authors) OVER (Partition by dfoa3.countrycode)) DESC, year

"""
dfmerged = duckdb.query(q).df()
# %%
dfmerged.head(1000)
# %%
# Save the merged dataframe:
dfmerged.to_csv(r"./data_processed/3way_comparison.csv")
# %%
# Plot the data:
import plotly.express as px
#%%
p1 = ["share_of_mobile_authors_openalex", "share_of_mobile_authors_scopus"]
p2 = ["share_of_mobile_authors_scopus","share_of_mobile_authors_orcid"]
p3 = ["share_of_mobile_authors_openalex", "share_of_mobile_authors_orcid"]
p = [p1, p2, p3]
i=0
fig = px.scatter(dfmerged[dfmerged["number_of_authors_scopus"]>1000], x=p[i][0], y=p[i][1], opacity=0.55, color="year", hover_data=["countrycode", "year", p[i][0], p[i][1]])
fig
# %%
dfmerged[["share_of_mobile_authors_openalex","share_of_mobile_authors_scopus",
      "share_of_mobile_authors_orcid"]][dfmerged["number_of_authors_scopus"]>1000].corr(method = 'kendall').style.background_gradient(cmap='coolwarm')
# %%
# now the number of authors:
p1 = ["number_of_authors_openalex", "number_of_authors_scopus"]
p2 = ["number_of_authors_scopus","number_of_authors_orcid"]
p3 = ["number_of_authors_openalex", "number_of_authors_orcid"]
p = [p1, p2, p3]
i=2
fig = px.scatter(dfmerged[dfmerged["number_of_authors_scopus"]>1000], x=p[i][0], y=p[i][1], opacity=0.55, color="year", hover_data=["countrycode", "year", p[i][0], p[i][1]])
fig
# %%
dfmerged[["number_of_authors_openalex","number_of_authors_scopus",
      "number_of_authors_orcid"]][dfmerged["number_of_authors_scopus"]>1000].corr(method = 'kendall').style.background_gradient(cmap='coolwarm')
# %%
#%%
p1 = ["n_mobile_authors_openalex", "n_mobile_authors_scopus"]
p2 = ["n_mobile_authors_scopus","n_mobile_authors_orcid"]
p3 = ["n_mobile_authors_openalex", "n_mobile_authors_orcid"]
p = [p1, p2, p3]
i=2
fig = px.scatter(dfmerged[dfmerged["number_of_authors_scopus"]>1000], x=p[i][0], y=p[i][1], opacity=0.55, color="year", hover_data=["countrycode", "year", p[i][0], p[i][1]])
fig
# %%
# save as pdf
import plotly
plotly.io.write_image(fig, r"./plots/3way_comparison.pdf")

# %%
dfmerged[["number_of_authors_openalex","number_of_authors_scopus",
      "number_of_authors_orcid"]][dfmerged["number_of_authors_scopus"]>1000].corr(method = 'kendall').to_latex(r"./data_processed/3way_comparison_number_of_authors.tex",
                  float_format="{:.2f}".format, caption="Kendall's tau correlation coefficients for the number of authors in OpenAlex, Scopus, and ORCID by country and year")
# %%
dfmerged[["n_mobile_authors_openalex","n_mobile_authors_scopus",
      "n_mobile_authors_orcid"]][dfmerged["number_of_authors_scopus"]>1000].corr(method = 'kendall').to_latex(r"./data_processed/3way_comparison_number_of_authors.tex",
                  float_format="{:.2f}".format, caption="Kendall's tau correlation coefficients for the number of mobile authors in OpenAlex, Scopus, and ORCID")
# %%
dfmerged[["share_of_mobile_authors_openalex","share_of_mobile_authors_scopus",
      "share_of_mobile_authors_orcid"]][dfmerged["number_of_authors_scopus"]>1000].corr(method = 'kendall').to_latex(r"./data_processed/3way_comparison_number_of_authors.tex",
                  float_format="{:.2f}".format, caption="Kendall's tau correlation coefficients for the number of mobile authors in OpenAlex, Scopus, and ORCID")
# %%
# create latex table:
dfmerged[["countryname","year","number_of_authors_openalex","number_of_authors_scopus","number_of_authors_orcid"]][dfmerged["number_of_authors_scopus"]>1000].to_latex(r"./data_processed/3way_comparison_number_of_authors.tex")