import pandas as pd
import arcpy
import numpy
import math

# When the column names in the pandas dataframe must be in the form. field_name,field_type,field_length,field_alias no spaces after comma. system fields come in without that stuff
                                                                    # field_type can be String,Integer,Double,Date
                                                                    # date must be in the form 1/1/2001. no zeros


def getfieldnames(path):
    field_names = []
    fields = arcpy.ListFields(path)
    for field in fields:
        if field.name == "Shape":
            field_names.append(field.name)
        elif field.name == "OBJECTID":
            field_names.append(field.name)
        elif field.name == "Shape_Length":
            field_names.append(field.name)
        elif field.name == "Shape_Area":
            field_names.append(field.name)
        else:
            field_names.append(field.name+","+str(field.type)+","+str(field.length)+","+field.aliasName)
    return field_names

def create_FC(workspace_path,pandas_table,fc_or_table_name,space_ref,geometry_type):
    if geometry_type =="TABLE":
        arcpy.management.CreateTable(workspace_path, fc_or_table_name, out_alias=fc_or_table_name)
    else:
        arcpy.management.CreateFeatureclass(workspace_path, fc_or_table_name, geometry_type=geometry_type, has_m="DISABLED", 
                                        has_z="DISABLED", spatial_reference=space_ref,out_alias=fc_or_table_name)
    # check for what datatype to assign fields and create name based on pd df                                                           
    for i in pandas_table.columns:
        column_name = i.split(",")
        if i == "OBJECTID":
            pass
        elif i == "Shape":
            pass
        elif i == "Shape_Length":
            pass
        elif i == "Shape_Area":
            pass
        else:
            if column_name[1]=="String":
                arcpy.management.AddField(fc_or_table_name, field_name=column_name[0], field_type="TEXT",field_length=column_name[2],field_alias=column_name[3])
            elif column_name[1]=="Integer":
                arcpy.management.AddField(fc_or_table_name, field_name=column_name[0], field_type="LONG",field_length=column_name[2],field_alias=column_name[3])
            elif column_name[1]=="Double":
                arcpy.management.AddField(fc_or_table_name, field_name=column_name[0], field_type="DOUBLE",field_length=column_name[2],field_alias=column_name[3])
            elif column_name[1]=="Date":
                arcpy.management.AddField(fc_or_table_name, field_name=column_name[0], field_type="DATE",field_length=column_name[2],field_alias=column_name[3])


def add_df_to_dbase(geodatabase,desired_layer_name,pandas_df,geometry_type):
    print("adding " + desired_layer_name + " to "+ geodatabase)
    create_FC(geodatabase,pandas_df,desired_layer_name,arcpy.SpatialReference(4326),geometry_type)
    new_field_order = getfieldnames(geodatabase + '\\'+desired_layer_name)
    
    pandas_df = pandas_df.reindex(columns=new_field_order)
    count=0
    alist=[]
    shape_length_count = 0
    shape_area_count=0
    objectid_count = 0
    for col in pandas_df.columns:
        column=col.split(",")
        if col== "Shape":
            alist.append("Shape@")
        elif col == "Shape_Length":
            shape_length_count = count
            alist.append("Shape_Length")
        elif col == "Shape_Area":
            shape_area_count = count
            alist.append("Shape_Area")
        elif col== "OBJECTID":
            alist.append("OBJECTID")
        else:
            alist.append(column[0])
        count+=1
    cursor = arcpy.da.InsertCursor(desired_layer_name,alist)
    # create dynamic tuple for for insert row
    for row in pandas_df.itertuples():
        alist2 = []
        for i in range(count):
            if i == shape_length_count:
                alist2.append("filler")
            elif i == shape_area_count:
                alist2.append("filler")
            else:
                alist2.append(row[i+1])

        cursor.insertRow(tuple(alist2))
    del cursor

# geom type shape@, shapeXY etc...
def create_df(geodatabase,fc,geom_type,reproject=False,projection_number=None):
    path = geodatabase + '\\'+ fc
    field_names = []
    fields = arcpy.ListFields(path)
    for field in fields:
        if field.name == "Shape":
            field_names.append(field.name)
        elif field.name == "OBJECTID":
            field_names.append(field.name)
        elif field.name == "Shape_Length":
            field_names.append(field.name)
        elif field.name == "Shape_Area":
            field_names.append(field.name)
        else:
            field_names.append(field.name+","+str(field.type)+","+str(field.length)+","+field.aliasName)
    adict = {}
    for i in field_names:
        names = i.split(",")
        if i== "Shape":
            if reproject:
                adict[i]=[row[0].projectAs(arcpy.SpatialReference(projection_number)) for row in arcpy.da.SearchCursor(fc,geom_type)]
            else:
                adict[i]=[row[0] for row in arcpy.da.SearchCursor(fc,geom_type)]
        else:
            adict[i]=[row[0] for row in arcpy.da.SearchCursor(fc,names[0])]

    df = pd.DataFrame(adict,columns=field_names)
    return df

def create_df_reproject(geodatabase,fc,geom_type,projection_num):
    path = geodatabase + '\\'+ fc
    field_names = []
    fields = arcpy.ListFields(path)
    for field in fields:
        field_names.append(field.name)
    adict = {}
    for i in field_names:
        if i == "Shape":
            adict[i]=[row[0].projectAs(arcpy.SpatialReference(projection_num)) for row in arcpy.da.SearchCursor(fc,geom_type)]
        else:
            adict[i]=[row[0] for row in arcpy.da.SearchCursor(fc,i)]

    df = pd.DataFrame(adict,columns=field_names)
    return df

def create_data_dict(geodatabase,fc,geom_type):
    path = geodatabase + '\\'+ fc
    field_names = []
    fields = arcpy.ListFields(path)
    for field in fields:
        field_names.append(field.name)
    adict = {}
    for i in field_names:
        if i == "Shape":
            adict[i]=[row[0] for row in arcpy.da.SearchCursor(fc,geom_type)]
        else:
            adict[i]=[row[0] for row in arcpy.da.SearchCursor(fc,i)]
    return adict

def custom_replicate(input_gdb,output_gdb):
    arcpy.env.workspace = input_gdb
    arcpy.env.OverwriteOutput= True
    #arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(4326)
    input_data = None
    
    for dirpath, dirnames, filenames in arcpy.da.Walk(input_gdb,datatype='Any'):
        input_data = filenames

    output_names_in_gdb = None
    for dirpath, dirnames, filenames in arcpy.da.Walk(output_gdb,datatype='Any'):
        output_names_in_gdb = filenames

    for name in input_data:
        arcpy.env.workspace = input_gdb
        arcpy.env.OverwriteOutput= True
        if input_gdb.split(".")[-1]=="sde":
            if name.split(".")[-1] in output_names_in_gdb:
                pass
        elif name in output_names_in_gdb:
            pass
        else:
            describe = arcpy.Describe(name)
            dataframe = create_df(input_gdb,name,"SHAPE@")
            if input_gdb.split(".")[-1]=="sde":
                name = name.split(".")[-1]
            try:
                
                if describe.shapeType == "Point":
                    arcpy.env.workspace = output_gdb
                    arcpy.env.OverwriteOutput= True
                    add_df_to_dbase(output_gdb,name,dataframe,"POINT")
                elif describe.shapeType == "Polyline":
                    arcpy.env.workspace = output_gdb
                    arcpy.env.OverwriteOutput= True
                    add_df_to_dbase(output_gdb,name,dataframe,"POLYLINE")
                elif describe.shapeType == "Polygon":
                    arcpy.env.workspace = output_gdb
                    arcpy.env.OverwriteOutput= True
                    add_df_to_dbase(output_gdb,name,dataframe,"POLYGON")
            except AttributeError:
                arcpy.env.workspace = output_gdb
                arcpy.env.OverwriteOutput= True
                add_df_to_dbase(output_gdb,name,dataframe,"TABLE")
            
            #create_df(input_gdb,name,"Shape@")
