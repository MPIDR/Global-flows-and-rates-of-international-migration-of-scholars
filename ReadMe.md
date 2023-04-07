## Data and code for performing analyses and plotting figures for "Global flows and rates of international migration of scholars"

**Information on related working paper**

**Title**: Global flows and rates of international migration of scholars

**Abstract**: 
Lack of reliable and comprehensive migration data is one of the major reasons that prevents advancements in our understanding of the causes and consequences of migration processes, including for specific groups like high-skilled migrants. We leverage large-scale bibliometric data from Scopus and OpenAlex to trace the global movements of a specific group of innovators: scholars. We developed pre-processing steps and offered best practices for the measurement and identification of migration events from bibliometric data. Our results show a high level of correlation between the count of scholars in Scopus and OpenAlex for most countries. While the magnitude of observed migration events in OpenAlex is larger than in Scopus, the bilateral flows among top pairs of origin and destination countries are consistent in the two databases. Even though OpenAlex has a higher coverage of non-Western countries, the highest correlations with Scopus are observed in Western countries. We share our aggregated estimates of international migration rates, and bilateral flows, at the country level, and expect that our estimates will enable researchers to improve our understanding of the causes and consequences of migration of scholars, and to forecast the future mobility of global academic talent.

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
