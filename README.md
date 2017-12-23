# OPEND_NSWAddresses

This OPEND subproject retrieves all NSW addresses from PSMA G-NAF (https://data.gov.au/dataset/geocoded-national-address-file-g-naf)

To load the data into PostGres database, this tool was used -> https://github.com/minus34/gnaf-loader
Please note that I have to comment out some lines in the loader program to omit other processing

Once loaded in the database, run gnafassembler.py to prepare the assembled addresses and attributes. 

This work is licensed under https://creativecommons.org/licenses/by/4.0/
