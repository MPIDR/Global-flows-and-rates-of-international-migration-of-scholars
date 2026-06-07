## Data and code for performing analyses and plotting figures for "Bilateral flows and rates of subnational and international migration of scholars worldwide by gender and field"

**Information on related working paper**

**Title**: Bilateral flows and rates of subnational and international migration of scholars worldwide by gender and field

**Abstract**: 
Subnational migration is often more intensive than international migration. These systems of migration are interrelated; however, the literature studies internal and international migration separately. The main reason is that integrated data on both of these migration systems are rare. In the case of scholarly migration, in addition to differences by migration type, trends substantially differ by gender and field of science. We prepare and publicly share the second version of the Scholarly Migration Database (SMD 2.0) including global bilateral flows and rates of subnational and international migration of scholars disaggregated by gender and field of science. We leverage large-scale bibliometric data from Scopus and OpenAlex to construct an integrated history of internal and international movements of scholars at the individual level. We then aggregate these trends to the province level (GeoNames admin 1 regions). Based on our adversarial collaboration and empirical validations using independent and reproducible scientific pipelines, we develop processing steps and offer best practices for the measurement and identification of migration events. We share aggregated yearly estimates of migration rates and of bilateral flows for 2,067 unique subnational regions worldwide in Scopus and 2,080 in OpenAlex for the period 1998-2024. We describe the data structure and provide usage notes. Given the breadth of research using the first version of our database, we expect that the publicly shared second version will further enable researchers to study the causes and the consequences of gender and field differences in the migration of scholars.

**Citation** (APA style): Aliakbar Akbaritabar, Tom Theile, and Emilio Zagheni (2026) Bilateral flows and rates of subnational and international migration of scholars worldwide by gender and field, MPIDR Working Paper. [https://dx.doi.org/10.4054/MPIDR-WP-2026-029](https://dx.doi.org/10.4054/MPIDR-WP-2026-029)

**DOI**: [https://dx.doi.org/10.4054/MPIDR-WP-2026-029](https://dx.doi.org/10.4054/MPIDR-WP-2026-029)

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
