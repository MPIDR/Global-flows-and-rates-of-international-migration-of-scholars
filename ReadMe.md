## Data and code for performing analyses and plotting figures for "Global flows and rates of international migration of scholars"

**Related working paper**, 

**Title**: Global flows and rates of international migration of scholars

**Citation** (APA style): Aliakbar Akbaritabar, Tom Theile, and Emilio Zagheni (2023) Global flows and rates of international migration of scholars, MPIDR Working Paper.

**DOI**: [XXXXXXXXXX-XXXXXXXXXX](XXXXXXXXXX-XXXXXXXXXX)

**Maintainer of the repository:** Tom Theile

**Authors of article:** Aliakbar Akbaritabar, Tom Theile, and Emilio Zagheni.

**Main repository page:** https://github.com/MPIDR/Global-flows-and-rates-of-international-migration-of-scholars/edit/master/ReadMe.md

The data used were extracted from two Bibliometric Databases, Scopus and OpenAlex, more description on [https://scholarlymigration.org/](https://scholarlymigration.org/). In addition, it is enriched by merging with World Bank data from these three links:
[http://api.worldbank.org/v2/country/all](http://api.worldbank.org/v2/country/all)
[https://api.worldbank.org/v2/country/all/indicator/NY.GDP.PCAP.CD?format=json&strdate=1995-01-01&per_page=30000](https://api.worldbank.org/v2/country/all/indicator/NY.GDP.PCAP.CD?format=json&strdate=1995-01-01&per_page=30000), and [https://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL?format=json&strdate=1995-01-01&per_page=30000](https://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL?format=json&strdate=1995-01-01&per_page=30000)


| File name              | Description                                                                                                                                              |
|---------------|---------------------------------------------------------|
| ReadMe.md              | This file in Markdown format.                                                                                                                            |
| scopus_2023_V1_scholarlymigration_country_enriched.csv | Country level yearly dataset on international emigration, immigration, net migration rates and other variables based on Scopus.               |
| scopus_2023_V1_scholarlymigration_countryflows_enriched.csv | Country level yearly bilateral flow of scholarly migration based on Scopus.                                                                                                      |
| openalex_2023_V1_scholarlymigration_country_enriched.csv | Country level yearly dataset on international emigration, immigration, net migration rates and other variables based on OpenAlex.               |
| openalex_2023_V1_scholarlymigration_countryflows_enriched.csv | Country level yearly bilateral flow of scholarly migration based on OpenAlex.                                                                                                      |
| 01_prepare_enrich_data.py | Source code (Python >=3.9) for downloading World Bank data and merging with Scopus and OpenAlex data. **Authors: Tom Theile** (<https://github.com/tomthe>)                                                    |
| 02_merge_openalex_and_scopus.py | Source code (Python >=3.9) for merging Scopus and OpenAlex data. **Authors: Tom Theile** (<https://github.com/tomthe>)                                                    |
| 03_plotting.py | Source code (Python v3.9) for plotting Figures 2-5. **Authors: Tom Theile, Aliakbar Akbaritabar** (<https://github.com/tomthe>, <https://github.com/akbaritabar>)                                                    |
| FIGURES\\ | Folder with plotted figures in different graphical formats.                                                                                              |
