# atmos
Mostly python package to handle a wide variety of code for atmospheric science applications. 

The environment.yml should have all the necessary packages to run the code. However, to do regridding with 
xesmf, I needed to add the code following this issue to make the input_dims stuff work: 
https://github.com/pangeo-data/xESMF/issues/362