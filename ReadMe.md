## Data and code for performing analyses and plotting figures for "Bilateral flows and rates of international migration of scholars for 210 countries and areas for the period 1998-2020"

**Information on related working paper**

**Title**: Bilateral flows and rates of international migration of scholars for 210 countries and areas for the period 1998-2020

**Abstract**: 
A lack of comprehensive migration data is a major barrier for understanding the causes and consequences of migration processes, including for specific groups like high-skilled migrants. We leverage large-scale bibliometric data from Scopus and OpenAlex to trace the global movements of scholars. Based on our empirical validations, we develop pre-processing steps and offer best practices for the measurement and identification of migration events. We have prepared a publicly accessible dataset that shows a high level of correlation between the counts of scholars in Scopus and OpenAlex for most countries.
Although OpenAlex has more extensive coverage of non-Western countries, the highest correlations with Scopus are observed in Western countries. We share aggregated yearly estimates of international migration rates and of bilateral flows for 210 countries and areas worldwide for the period 1998-2020 and describe the data structure and usage notes. We expect that the publicly shared dataset will enable researchers to further study the causes and the consequences of migration of scholars to forecast the future mobility of global academic talent.

**Citation** (APA style): Aliakbar Akbaritabar, Tom Theile, and Emilio Zagheni (2023) Global flows and rates of international migration of scholars, MPIDR Working Paper. [https://dx.doi.org/10.4054/MPIDR-WP-2023-018](https://dx.doi.org/10.4054/MPIDR-WP-2023-018)

**DOI**: [https://dx.doi.org/10.4054/MPIDR-WP-2023-018](https://dx.doi.org/10.4054/MPIDR-WP-2023-018)

**Maintainer of the repository:** Tom Theile

**Authors of article:** Aliakbar Akbaritabar, Tom Theile, and Emilio Zagheni.

**Main repository page:** https://github.com/MPIDR/Global-flows-and-rates-of-international-migration-of-scholars/

The data used were extracted from two Bibliometric Databases, Scopus and OpenAlex. More description can be found on [https://scholarlymigration.org/](https://scholarlymigration.org/). In addition, it is enriched by merging it with [World Bank data](http://api.worldbank.org/v2/country/all).


| File name              | Description                                                                                                                                              |
|---------------|---------------------------------------------------------|
| ReadMe.md              | This file in Markdown format.                                                                                                                            |
| [scopus_2023_V1_scholarlymigration_country_enriched.csv](https://raw.githubusercontent.com/MPIDR/Global-flows-and-rates-of-international-migration-of-scholars/master/data_processed/scopus_2023_V1_scholarlymigration_country_enriched.csv) | Country level yearly dataset on international emigration, immigration, net migration rates and other variables based on Scopus.               |
| [scopus_2023_V1_scholarlymigration_countryflows_enriched.csv](https://raw.githubusercontent.com/MPIDR/Global-flows-and-rates-of-international-migration-of-scholars/master/data_processed/scopus_2023_V1_scholarlymigration_countryflows_enriched.csv) | Country level yearly bilateral flow of scholarly migration based on Scopus.                                                                                                      |
|[openalex_2023_V1_scholarlymigration_country_enriched.csv](https://raw.githubusercontent.com/MPIDR/Global-flows-and-rates-of-international-migration-of-scholars/master/data_processed/openalex_2023_V1_scholarlymigration_country_enriched.csv) | Country level yearly dataset on international emigration, immigration, net migration rates and other variables based on OpenAlex.               |
| [openalex_2023_V1_scholarlymigration_countryflows_enriched.csv](https://raw.githubusercontent.com/MPIDR/Global-flows-and-rates-of-international-migration-of-scholars/master/data_processed/openalex_2023_V1_scholarlymigration_countryflows_enriched.csv) | Country level yearly bilateral flow of scholarly migration based on OpenAlex.                                                                                                      |
| 01_prepare_enrich_data.py | Source code (Python >=3.9) for downloading World Bank data and merging with Scopus and OpenAlex data. Authors: Tom Theile (<https://github.com/tomthe>)                                                    |
| 02_merge_openalex_and_scopus.py | Source code (Python >=3.9) for merging Scopus and OpenAlex data. Authors: Tom Theile (<https://github.com/tomthe>)                                                    |
| 03_plotting.py | Source code (Python >=3.9) for plotting Figures 2-5. Authors: Tom Theile, Aliakbar Akbaritabar (<https://github.com/tomthe>, <https://github.com/akbaritabar>)                                                    |
| FIGURES\\ | Folder with plotted figures in PDF format.                                                                   |
| data_input\\ | Folder with input data (aggregated migration events of scholars).                                                               |
| data_processed\\ | Folder with processed/enriched data. Produced by scripts 01 and 02                                                     |

We added all data files as plain text csv-files (with header line) and as binary [parquet](https://parquet.apache.org/) files. The contents of the csv and parquet files are the same.


## License

The output data in the folder ./data_processed is licensed under a Creative Commons Attribution 4.0 International License. See license.cc-by.md

The code and everything else is licensed under the GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007. See license.agpl.md

(c) by Aliakbar Akbaritabar, Tom Theile, and Emilio Zagheni
