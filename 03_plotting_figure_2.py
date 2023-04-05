import pandas as pd
import os
import plotnine as gg

# data/code DIR
data_dir = os.path.join('N:\\', 'Theile', 'bibliometry', 'SMD', 'figures for scientific data', 'selected', 'manuscript_replication_data')

# v8
oascp = pd.read_csv(os.path.join(data_dir, 'v8_dfmerged_openalex_scopus_country.csv'), dtype='str')


# calculate a correlation between OpenAlex and Scopus yearly population rates
correlation_res_pop = (
    oascp
    .dropna(subset=['countrycode', 'year', 'paddedpop_openalex', 'paddedpop_scopus'])
    .astype({'paddedpop_openalex':float, 'paddedpop_scopus':float, 'year':float})
    # limit years to 1998 - 2018 (inclusive)
    .query('year > 1997 & year < 2019')
    .sort_values(by=['countrycode', 'year'])
    .groupby('countrycode')
    [['paddedpop_openalex', 'paddedpop_scopus']]
    # change correlation method here to kendall or spearman
    .corr(method='kendall')
    .reset_index()
    .drop_duplicates(subset='countrycode')
    .rename(columns={'paddedpop_scopus': 'pop_kendal_corr'})
    [['countrycode', 'pop_kendal_corr']]
    .sort_values(by=['pop_kendal_corr'])
)


# join correlation results back to table to plot
oascp = oascp.merge(correlation_res_pop, how='left', on='countrycode')

# plot it as boxplot with jitter

FIG_2_1_kendal_pop = (
    gg.ggplot((
        oascp
        .drop_duplicates(subset=['countrycode', 'pop_kendal_corr'])
        .dropna(subset=['region'])
    ), gg.aes(x='factor(region)', y='pop_kendal_corr'))
    + gg.geom_jitter(height = 0, color='#d7d7d2')
    + gg.geom_boxplot(color='blue', fill=None)
    + gg.scale_y_continuous(limits=(-1, 1), breaks=[-1, -0.75, -0.50, -0.25, 0, 0.25, 0.50, 0.75, 1])
    + gg.labs(x="Countries based on region", y="Kendal Tau Correlation", title = 'Kendal tau correlation between \n\n Scopus and OpenAlex populations')
    + gg.theme_bw()
    + gg.theme(axis_text_x=gg.element_text(hjust=1, size=10, angle=45),
               figure_size=(5, 5))
)

gg.ggplot.save(FIG_2_1_kendal_pop, os.path.join(
    data_dir, 'FIG_2_1_kendal_pop.pdf'), limitsize=False)


# calculate a correlation between OpenAlex and Scopus yearly netmigration rates
correlation_res_netmig = (
    oascp
    .dropna(subset=['countrycode', 'year', 'netmigrate_openalex', 'netmigrate_scopus'])
    .astype({'netmigrate_openalex':float, 'netmigrate_scopus':float, 'year':float})
    # limit years to 1998 - 2018 (inclusive)
    .query('year > 1997 & year < 2019')
    .sort_values(by=['countrycode', 'year'])
    .groupby('countrycode')
    [['netmigrate_openalex', 'netmigrate_scopus']]
    # change correlation method here to kendall or spearman
    .corr(method='kendall')
    .reset_index()
    .drop_duplicates(subset='countrycode')
    .rename(columns={'netmigrate_scopus': 'nmr_kendal_corr'})
    [['countrycode', 'nmr_kendal_corr']]
    .sort_values(by=['nmr_kendal_corr'])
)

# join correlation results back to table to plot
oascp = oascp.merge(correlation_res_netmig, how='left', on='countrycode')


# plot it as boxplot with jitter

FIG_2_2_kendal_netmig = (
    gg.ggplot((
        oascp
        .drop_duplicates(subset=['countrycode', 'nmr_kendal_corr'])
        .dropna(subset=['region'])
    ), gg.aes(x='factor(region)', y='nmr_kendal_corr'))
    + gg.geom_jitter(height = 0, color='#d7d7d2')
    + gg.geom_boxplot(color='blue', fill=None)
    + gg.scale_y_continuous(limits=(-1, 1), breaks=[-1, -0.75, -0.50, -0.25, 0, 0.25, 0.50, 0.75, 1])
    + gg.labs(x="Countries based on region", y="Kendal Tau Correlation", title = 'Kendal tau correlation between \n\n Scopus and OpenAlex net migration rates')
    + gg.theme_bw()
    + gg.theme(axis_text_x=gg.element_text(hjust=1, size=10, angle=45),
               figure_size=(5, 5))
)

gg.ggplot.save(FIG_2_2_kendal_netmig, os.path.join(
    data_dir, 'FIG_2_2_kendal_netmig.pdf'), limitsize=False)

