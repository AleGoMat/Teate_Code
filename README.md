# Teate_Code

Data Cleaning Basic EDA: we take the 20 monthly datasets Teate sent us, concat them, delete duplicates, delete useless variables, 
corrected formats for various fields, conducted grouped analysis of important variables, and export the final dataset.

SendtoRDS: Here we send the resulting big data set to an RDS instance. 

read from rds and more eda: Here we test if the data is already in RDS and continue cleaning and exploring

Barchart_supplier.py: This code takes the first 10 suppliers (by volume of sales) per city, per month, and creates a bar chart that shows the top 10 suppliers
along the months. 

geomap1.py: This code reads data from RDS and keeps only the columns need to build the main map in the dashboard. 

treemap.py: This code reads data from RDS and builds the necessary dataset to produce the treemap visualization in the dashboard. 

FBProphet Medellin: We separate the data into 7 different datasets (7 different product categories), and fit the fbprophet model for eac one of them, 
then we calculate the avg yhat for all 7 models, then we export these results, along with the real values. 

FBProphet_Cali:We separate the data into 7 different datasets (7 different product categories), and fit the fbprophet model for eac one of them, 
then we calculate the avg yhat for all 7 models, then we export these results, along with the real values.

fb_modelplot_nofilter.py: Here we read the resulting data from the last 2 codes (from RDS) and build an example plot.
