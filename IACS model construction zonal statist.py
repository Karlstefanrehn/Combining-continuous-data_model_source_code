## IACS model construction zonal statistics  

# the step of taking each field for each year  - over the 2020 grid from the IACS
# This  and rastarizing it to subseqeuntly count the majority of crops in that field.

#Imports 
import sys
import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt 
import seaborn as sns
from qgis.core import QgsVectorLayer, QgsApplication, QgsVectorFileWriter, QgsCoordinateReferenceSystem, QgsProject
from osgeo import gdal
import os        

# Starting a QGIS application
qgishome = r'C:\OSGeo4W64\apps\qgis\\'
app = QgsApplication([], True)
QgsApplication.setPrefixPath(qgishome, True)
QgsApplication.initQgis()

#processing
sys.path.append(r'C:\OSGeo4W64\apps\qgis\python\plugins')
import processing
from processing.core.Processing import Processing
Processing.initialize()
from processing.tools import *



# Takning MULTUBLOCK into refactored shapes - then Rasterized and saved as tifs in outpu 
# paths - fp 
multi_fp = r''
refactor_fp = r''
crop_raster_fp = r''

crop_list = []
for item3 in os.listdir(multi_fp):
        if item3[-3:]=='shp':
                crop_list.append(item3)
                
                processing.run("native:refactorfields",
                               {'INPUT': multi_fp + item3,
                                'FIELDS_MAPPING':[{'expression': '\"GEOGRAFISK\"','length': 13,'name': 'GEOGRAFISK','precision': 0,'type': 10},
                                                  {'expression': '\"KUND_LAN\"','length': 15,'name': 'KUND_LAN','precision': 0,'type': 10},
                                                  {'expression': '\"GRODKOD\"','length': 15,'name': 'GRODKOD','precision': 0,'type': 2},
                                                  {'expression': '\"GRODBESKRI\"','length': 254,'name': 'GRODBESKRI','precision': 0,'type': 10},
                                                  {'expression': '\"AREAL\"','length': 30,'name': 'AREAL','precision': 15,'type': 6},
                                                  {'expression': '\"OBJECTID\"','length': 10,'name': 'OBJECTID','precision': 0,'type': 4},
                                                  {'expression': '\"SKIFTESBET\"','length': 43,'name': 'SKIFTESBET','precision': 0,'type': 10}],
                                                'OUTPUT':refactor_fp + item3[:-12] + '_crops.shp'})

                
refac_to_rast_fp = r''
crop_inp =[]
for item4 in os.listdir(refac_to_rast_fp):
        if item4[-3:]=='shp':
                crop_inp.append(item4)
                
                processing.run("gdal:rasterize",
                               {'INPUT':refac_to_rast_fp + item4,
                                'FIELD':'GRODKOD',
                                'BURN':0,
                                'UNITS':1,
                                'WIDTH':2,
                                'HEIGHT':2,
                                'EXTENT':'329050.994900000,368703.983900000,6474320.006000000,6527911.730000000 [EPSG:3006]',
                                'NODATA':0,
                                'OPTIONS':'',
                                'DATA_TYPE':5,
                                'INIT':None,
                                'INVERT':False,
                                'EXTRA':'',
                                'OUTPUT':crop_raster_fp +item4[:-4] +'.tif'})
                

print(' Good here ')



# Taking the created shapes and coverting to pkl and CSV files in order to extract the row of information with the majority crop code per field and year
def main_import(): 
    
    global directory
    global outfolder
    global outfolderpkl

    directory = r''
    outfolder = r''
    outfolderpkl = r''
    
    

    def forloop1():
        directory = r''
        outfolder = r''
        outfolderpkl = r''
        
        zon_shapes = []
        
        for filename in os.listdir(directory):
            
            if filename[-3:]=='shp': 
                zon_shapes.append(filename)

                df = gpd.read_file(directory + filename)
                dff = pd.DataFrame(df)
                
                dff.to_pickle(outfolderpkl + filename[:-4] + '.pkl')
                
                dff.to_csv(outfolderpkl + filename[:-4] + '.csv', sep=';')
                
    #forloop1()




'''
                                        CROP FOR EACH YEAR CODE - Here rasters, majority analysis 
'''


def raster_calc_majority_import (): 
    
  #All_Zonals  
    
    directory = r''
    outfolder = r'' # This is suppose to be the dataframe with all years of GRODKOD - #  RESULT 
    outfolderpkl = r''
     
    zon_shapes = []
    
    for filename in os.listdir(directory):
        
        if filename[-3:]=='shp': 
           #print(os.path.join(directory, filename))
            zon_shapes.append(filename)
            #print(zon_shapes)
    
    global eachShape
    for eachShape in zon_shapes:

        path = r'' + eachShape
        dfg = gpd.read_file(path)
        dfgn_ = dfg.rename(columns={'_majority': 'maj2003'})
        
        dfgn_[['maj2003']] = dfgn_[['maj2003']].astype('float')
        
        
        print(dfgn_.head())
        
        
        df_.to_pickle(outfolderpkl + '/' + eachShape[:-20] + ".pkl")
       
        print(eachShape)
      
raster_calc_majority_import()



def majority_df ():
    
    df_pkl_path = r''
    df_pkl_path_test = r''
                                                                # This works now 
    #df_2020 = pd.read_pickle(df_pkl_path + '2020.pkl')
    #df_2019 = pd.read_pickle(df_pkl_path + '2019.pkl')
    #df_2018 = pd.read_pickle(df_pkl_path + '2018.pkl')
    #df_2017 = pd.read_pickle(df_pkl_path + '2017.pkl')
    df_2003 = pd.read_pickle(df_pkl_path + 'MULTI.JORDBRUKSBLOCK_GRODKOD2003_GVshprefac.shp_tes.pkl')
    #
    # ETC.....
  
    
    # compile the list of dataframes you want to merge
    data_frames =   [  df_2003,
                     
                     
                     
                     ]
    
    global df_new
    df_new = df_2003.copy()
    
    print(df_new)
    
    test_path = r''
    df_test = gpd.read_file(test_path)
    dataframetest= pd.DataFrame(df_test)
    
    print(dataframetest)

majority_df()



def data_extract():
    
    dfshp = df_new.copy()
    df_shp = pd.DataFrame(dfshp)

    df_shp_short = df_shp.rename(columns={'GRODKOD2020': 2020})

    df_shp_short.drop(['GRODBESKRI', 'KUND_LAN'],axis=1, inplace=True)
    print(df_shp_short)

    ''' SAVING it as a pickle'''
    def pickle_save():
        
        #df_shp_short.to_pickle(r"C:\pHD\scr_model\output\pkl\majority_crops.pkl")
        majority_crops = pd.read_pickle(r".pkl")
        print(majority_crops)
        majority_crops.info()
        global mcd
        majority_crops_data = majority_crops.copy()
        mcd = majority_crops_data
        #mcd.drop(['GEOGRAFISK','AREAL','OBJECTID','SKIFTESBET','geometry'],axis=1, inplace=True)
        
    pickle_save()

    
    def shapefile_save():
    
        ''' Inserting all values from MCD to the base- shapefile with all polygons. '''
        
        sp = r''
        base_data20 = gpd.read_file(sp)
        
       # base_data20.columns = base_data20.columns.map(str)
       
        base_data20["2020"] = mcd[2020]
        base_data20["2019"] = mcd[2019]
        base_data20["2018"] = mcd[2018]
        base_data20["2017"] = mcd[2017]
        base_data20["2016"] = mcd[2016]
        base_data20["2015"] = mcd[2015]
        base_data20["2014"] = mcd[2014]
        base_data20["2013"] = mcd[2013]
        base_data20["2012"] = mcd[2012]
        base_data20["2011"] = mcd[2011]
        base_data20["2010"] = mcd[2010]
        base_data20["2009"] = mcd[2009]
        base_data20["2008"] = mcd[2008]
        base_data20["2007"] = mcd[2007]
        base_data20["2006"] = mcd[2006]
        base_data20["2005"] = mcd[2005]
        base_data20["2004"] = mcd[2004]
        base_data20["2003"] = mcd[2003]
        #                                                       
        base_data20.to_file("majority_crop_sweden.shp")
        
        print("Shapefile successfully saved")
    #shapefile_save()
data_extract()
