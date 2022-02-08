import arcpy
import pandas as pd
import geo





def main():
  
    geodatabase = r"C:\Users\hahnef\Documents\ArcGIS\Projects\Historic Bridges\Historic Bridges.gdb"

    arcpy.env.workspace = geodatabase
    arcpy.env.OverwriteOutput= True
    arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(4326)

    # can reproject the dataframe to differenct coordinate system or keep as is.

    #dataframe = geo.create_df(geodatabase,"merged_bridges","SHAPE@")
    #dataframe = geo.create_df(geodatabase,"merged_bridges","SHAPE@",reproject=True,projection_number=4326)
    

    #print(dataframe)
    

    # Be mindful of what the geometry type of your dataframe is. can turn anything into a table. but cant turn an initial point to a line.

    #geo.add_df_to_dbase(geodatabase,"test_framework_reproj",dataframe,"POLYGON")
    #geo.add_df_to_dbase(geodatabase,"test_framework_2",dataframe,"POINT")
    #geo.add_df_to_dbase(geodatabase,"test_framework_2",dataframe,"POLYLINE")
    #geo.add_df_to_dbase(geodatabase,"test_framework_table",dataframe,"TABLE")

    # can copy from one gdb to another without overwriting another file.

    #geo.custom_replicate(r"L:\EnterpriseGIS\Planning\Staging\DBConnections\GISTrans_PlanningAsGISTrans_Planning_O.sde", r"C:\Users\hahnef\Documents\ArcGIS\Projects\Historic Bridges\replicate_output.gdb")

main()