## Data and code for performing analyses and plotting figures for "Global flows and rates of international migration of scholars"

**Related working paper**, 

**Title**: Global flows and rates of international migration of scholars

**Citation** (APA style): Aliakbar Akbaritabar, Tom Theile, and Emilio Zagheni (2023) Global flows and rates of international migration of scholars, MPIDR Working Paper.

**DOI**: [XXXXXXXXXX-XXXXXXXXXX](XXXXXXXXXX-XXXXXXXXXX)

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
| 03_plotting.py | Source code (Python v3.9) for plotting Figures 2-5. Authors: Tom Theile, Aliakbar Akbaritabar (<https://github.com/tomthe>, <https://github.com/akbaritabar>)                                                    |
| FIGURES\\ | Folder with plotted figures in different graphical formats.                                                                   |
| data_input\\ | Folder input data (aggregated migration events of scholars).                                                               |
| data_processed\\ | Folder with processed/enriched data. Produced by scripts 01 and 02                                                     |

We added all data files as plain text csv-files (with header line) and as binary [parquet](https://parquet.apache.org/) files. The contents of the csv and parquet files are the same.
