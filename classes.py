import pandas as pd
import arcpy
from scipy.spatial import KDTree
import numpy
import json
from arcgis.features import FeatureLayer
from arcgis.gis import GIS
import os

class ReadGeopandas:
    # geom type is like Shape@ or original
    def __init__(self,gdb_path,table_or_feature_class_name,geom_type="Shape@"):
        self.gdb_path = gdb_path
        self.table_or_feature_class_name = table_or_feature_class_name
        self.geom_type = geom_type

    def create_df_from_sde(self,reproject=False,projection_number=None):
        arcpy.env.workspace=self.gdb_path
        arcpy.env.OverwriteOutput= True
        path = self.gdb_path + '\\'+ self.table_or_feature_class_name
        field_names = []
        fields = arcpy.ListFields(path)
        for field in fields:
            field_names.append(field.name)
        adict = {}
        for i in field_names:
            if i == "Shape":
                if reproject:
                    adict[i]=[row[0].projectAs(arcpy.SpatialReference(projection_number)) for row in arcpy.da.SearchCursor(self.table_or_feature_class_name,self.geom_type)]
                else:
                    adict[i]=[row[0] for row in arcpy.da.SearchCursor(self.table_or_feature_class_name,self.geom_type)]
            else:
                adict[i]=[row[0] for row in arcpy.da.SearchCursor(self.table_or_feature_class_name,i)]

        df = pd.DataFrame(adict,columns=field_names)
        return df

    def create_df_cad_dwg(self):
        return True

    def create_df_from_kmz_or_kml(self,kmz_or_kml_path):
        arcpy.conversion.KMLToLayer(kmz_or_kml_path,r"memory")
        if ".kmz" in kmz_or_kml_path:
            gdb_name = kmz_or_kml_path.split("\\")[-1].replace(".kmz",".gdb")
        elif ".kml" in kmz_or_kml_path:
            gdb_name = kmz_or_kml_path.split("\\")[-1].replace(".kml",".gdb")
        virtual_gdb = "memory\\" + gdb_name
        for dirpath, dirnames, filenames in arcpy.da.Walk(virtual_gdb,datatype=['FeatureClass','Table']):
            for file in filenames: 
                feature_class_name = file
                break
        df = ReadGeopandas(virtual_gdb,feature_class_name).create_df_from_sde(reproject=True,projection_number=4326)
        #os.rmdir(r"memory")
        return df

    def create_df_from_dbf(self,dbf_path):
        field_names = []
        fields = arcpy.ListFields(dbf_path)
        for field in fields:
            field_names.append(field.name)
        arr = arcpy.da.TableToNumPyArray(dbf_path,tuple(field_names))
        df = pd.DataFrame(arr,columns = field_names)
        return df
           

    def create_df_from_feature_service(self,portal_url,username,password,rest_url):
        gis = GIS(portal_url,username,password)
        layer_object=FeatureLayer(rest_url, gis)
        df = layer_object.query(where='1=1', out_fields='*', time_filter=None, geometry_filter=None, return_geometry=True, return_count_only=False, return_ids_only=False, return_distinct_values=False, return_extent_only=False, group_by_fields_for_statistics=None, statistic_filter=None, result_offset=None, result_record_count=None, object_ids=None, distance=None, units=None, max_allowable_offset=None, out_sr=4326, geometry_precision=None, gdb_version=None, order_by_fields=None, out_statistics=None, return_z=False, return_m=False, multipatch_option=None, quantization_parameters=None, return_centroid=False, return_all_records=True, result_type=None, historic_moment=None, sql_format=None, return_true_curves=False, return_exceeded_limit_features=None, as_df=True, datum_transformation=None)
        df.rename(columns = {'SHAPE':"Shape"}, inplace = True)
        return df

    def create_df_from_geojson(self,live_json_feed=False,json_path=None):
        with open(json_path) as f:
            data = json.load(f)
        
        data = data["features"]
        point_features = []
        line_features = []
        polygon_features = []

        for item in data:
            if item["geometry"]['type']=="Point":
                point_features.append(item)
            elif item["geometry"]['type']=="LineString":
                line_features.append(item)
            elif item["geometry"]['type']=="Polygon":
                polygon_features.append(item)
        
        point_dict = {}
        line_dict = {}
        polygon_dict = {}

        point_dict["Shape"]=[]
        for feature in point_features:
            for k,v in feature["properties"].items():
                point_dict[k]=[]
            break

        for feature in point_features:
            point_dict["Shape"].append(feature["geometry"]["coordinates"])
            for k,v in feature["properties"].items():
                point_dict[k].append(v)

        line_dict["Shape"]=[]
        for feature in line_features:
            for k,v in feature["properties"].items():
                line_dict[k]=[]
            break

        for feature in line_features:
            line_dict["Shape"].append(feature["geometry"]["coordinates"])
            for k,v in feature["properties"].items():
                line_dict[k].append(v)

        polygon_dict["Shape"]=[]
        for feature in polygon_features:
            for k,v in feature["properties"].items():
                polygon_dict[k]=[]
            break

        for feature in polygon_features:
            polygon_dict["Shape"].append(feature["geometry"]["coordinates"])
            for k,v in feature["properties"].items():
                polygon_dict[k].append(v)
            
            

        point_dataframe = pd.DataFrame(point_dict)
        line_dataframe = pd.DataFrame(line_dict)
        polygon_dataframe = pd.DataFrame(polygon_dict)
        
        return point_dataframe,line_dataframe,polygon_dataframe


class WriteGeopandas:
    def __init__(self,geodatabase,desired_layer_name,pandas_df,geometry_type):
        self.geodatabase = geodatabase
        self.desired_layer_name = desired_layer_name
        self.pandas_df = pandas_df
        # POINT, POLYLINE,POLYGON,TABLE
        self.geometry_type = geometry_type

    def append_df_to_fc(self,truncate_first=False,is_versioned=False):
        arcpy.env.workspace = self.geodatabase
        arcpy.env.OverwriteOutput = True

        if truncate_first:
            arcpy.management.DeleteRows(self.desired_layer_name)

        if is_versioned:
            edit = arcpy.da.Editor(self.geodatabase)
            edit.startEditing(False, True)
            edit.startOperation()
            for i in self.pandas_df.columns:
                if i == "OBJECTID":
                    pass
                elif i == "Shape":
                    pass
                elif i == "Shape_Length":
                    pass
                elif i == "Shape_Area":
                    pass
                else:
                    arcpy.management.AddField(self.desired_layer_name, field_name=i, field_type="TEXT",field_length=5000)
        
            field_names = []
            fields = arcpy.ListFields(self.geodatabase + '\\'+self.desired_layer_name)
            for field in fields:
                field_names.append(field.name)
            new_field_order = field_names
            pandas_df = self.pandas_df.reindex(columns=new_field_order)
            count=0
            alist=[]
            shape_length_count = 0
            for col in pandas_df.columns:
                if col == "Shape":
                    alist.append("Shape@")
                elif col == "Shape_Length":
                    shape_length_count = count
                    alist.append("Shape_Length")
                elif col == "OBJECTID":
                    alist.append("OBJECTID")
                else:
                    alist.append(col)
                count+=1
            cursor = arcpy.da.InsertCursor(self.desired_layer_name,alist)
            # create dynamic tuple for for insert row
            for row in pandas_df.itertuples():
                alist2 = []
                for i in range(count):
                    if i == shape_length_count:
                        alist2.append("filler")
                    else:
                        alist2.append(row[i+1])

                cursor.insertRow(tuple(alist2))
            del cursor

            edit.stopOperation()
            edit.stopEditing(True)

        else:

            for i in self.pandas_df.columns:
                if i == "OBJECTID":
                    pass
                elif i == "Shape":
                    pass
                elif i == "Shape_Length":
                    pass
                elif i == "Shape_Area":
                    pass
                else:
                    arcpy.management.AddField(self.desired_layer_name, field_name=i, field_type="TEXT",field_length=5000)
        
            field_names = []
            fields = arcpy.ListFields(self.geodatabase + '\\'+self.desired_layer_name)
            for field in fields:
                field_names.append(field.name)
            new_field_order = field_names
            pandas_df = self.pandas_df.reindex(columns=new_field_order)
            count=0
            alist=[]
            shape_length_count = 0
            for col in pandas_df.columns:
                if col == "Shape":
                    alist.append("Shape@")
                elif col == "Shape_Length":
                    shape_length_count = count
                    alist.append("Shape_Length")
                elif col == "OBJECTID":
                    alist.append("OBJECTID")
                else:
                    alist.append(col)
                count+=1
            cursor = arcpy.da.InsertCursor(self.desired_layer_name,alist)
            # create dynamic tuple for for insert row
            for row in pandas_df.itertuples():
                alist2 = []
                for i in range(count):
                    if i == shape_length_count:
                        alist2.append("filler")
                    else:
                        alist2.append(row[i+1])

                cursor.insertRow(tuple(alist2))
            del cursor


    def write_fc_df_to_sde_force(self):
        arcpy.env.workspace = self.geodatabase
        arcpy.env.OverwriteOutput= True
        if self.geometry_type =="TABLE":
            arcpy.management.CreateTable(self.geodatabase, self.desired_layer_name, out_alias=self.desired_layer_name)
        else:
            arcpy.management.CreateFeatureclass(self.geodatabase, self.desired_layer_name, self.geometry_type, has_m="DISABLED", 
                                        has_z="DISABLED", spatial_reference=arcpy.SpatialReference(4326),out_alias=self.desired_layer_name)

        for i in self.pandas_df.columns:
            if i == "OBJECTID":
                pass
            elif i == "Shape":
                pass
            elif i == "Shape_Length":
                pass
            elif i == "Shape_Area":
                pass
            else:
                arcpy.management.AddField(self.desired_layer_name, field_name=i, field_type="TEXT",field_length=5000)
        
        field_names = []
        fields = arcpy.ListFields(self.geodatabase + '\\'+self.desired_layer_name)
        for field in fields:
            field_names.append(field.name)
        new_field_order = field_names
        pandas_df = self.pandas_df.reindex(columns=new_field_order)
        count=0
        alist=[]
        shape_length_count = 0
        for col in pandas_df.columns:
            if col == "Shape":
                alist.append("Shape@")
            elif col == "Shape_Length":
                shape_length_count = count
                alist.append("Shape_Length")
            elif col == "OBJECTID":
                alist.append("OBJECTID")
            else:
                alist.append(col)
            count+=1
        cursor = arcpy.da.InsertCursor(self.desired_layer_name,alist)
        # create dynamic tuple for for insert row
        for row in pandas_df.itertuples():
            alist2 = []
            for i in range(count):
                if i == shape_length_count:
                    alist2.append("filler")
                else:
                    alist2.append(row[i+1])

            cursor.insertRow(tuple(alist2))
        del cursor

    def write_fc_df_to_sde_custom(self):
        arcpy.env.workspace = self.geodatabase
        arcpy.env.OverwriteOutput= True
        if self.geometry_type =="TABLE":
            arcpy.management.CreateTable(self.geodatabase, self.desired_layer_name, out_alias=self.desired_layer_name)
        else:
            arcpy.management.CreateFeatureclass(self.geodatabase, self.desired_layer_name, self.geometry_type, has_m="DISABLED", 
                                        has_z="DISABLED", spatial_reference=arcpy.SpatialReference(4326),out_alias=self.desired_layer_name)

        for i in self.pandas_df.columns:
            if i == "OBJECTID":
                pass
            elif i == "Shape":
                pass
            elif i == "Shape_Length":
                pass
            elif i == "Shape_Area":
                pass
            else:
                print("For Field:",i)
                field_type = input("Enter Field Type: Choices(TEXT,FLOAT,DOUBLE,SHORT,LONG,DATE,BLOB,RASTER,GUID)")
                field_length = input("Enter Field Length:")
                field_alias = input("Enter Field Alias:")
                arcpy.management.AddField(self.desired_layer_name, field_name=i, field_type=field_type,field_length=field_length,field_alias = field_alias)
        
        field_names = []
        fields = arcpy.ListFields(self.geodatabase + '\\'+self.desired_layer_name)
        for field in fields:
            field_names.append(field.name)
        new_field_order = field_names
        pandas_df = self.pandas_df.reindex(columns=new_field_order)
        count=0
        alist=[]
        shape_length_count = 0
        for col in pandas_df.columns:
            if col == "Shape":
                alist.append("Shape@")
            elif col == "Shape_Length":
                shape_length_count = count
                alist.append("Shape_Length")
            elif col == "OBJECTID":
                alist.append("OBJECTID")
            else:
                alist.append(col)
            count+=1
        cursor = arcpy.da.InsertCursor(self.desired_layer_name,alist)
        # create dynamic tuple for for insert row
        for row in pandas_df.itertuples():
            alist2 = []
            for i in range(count):
                if i == shape_length_count:
                    alist2.append("filler")
                else:
                    alist2.append(row[i+1])

            cursor.insertRow(tuple(alist2))
        del cursor

class Geoprocessing:
    def __init__(self,input_dataframe=None):
        self.input_dataframe = input_dataframe


    def reproject_in_place(self,geodatabase,layer,projection_num):
        arcpy.env.workspace = geodatabase
        arcpy.env.OverwriteOutput= True
        reprojected_geometry=[row[0].projectAs(arcpy.SpatialReference(projection_num)) for row in arcpy.da.SearchCursor(layer,"SHAPE@")]
        update_cursor = arcpy.da.UpdateCursor(layer,"SHAPE@")
        with update_cursor as cursor:
            index=0
            for row in cursor:
                row[0]=reprojected_geometry[index]
                cursor.updateRow(row)
                index+=1

    def update_column(self,geodatabase,layer,column_name,attribute,replaced_attribute):
        arcpy.env.workspace = geodatabase
        arcpy.env.OverwriteOutput= True
        update_cursor = arcpy.da.UpdateCursor(layer,column_name)
        with update_cursor as cursor:
            for row in cursor:
                if row[0]==attribute:
                    row[0]=replaced_attribute
                    cursor.updateRow(row)

    

    def truncate(self,geodatabase,layer,is_versioned=False):
        arcpy.env.workspace = geodatabase
        arcpy.env.OverwriteOutput= True
        if is_versioned:
            edit = arcpy.da.Editor(self.geodatabase)
            edit.startEditing(False, True)
            edit.startOperation()
            arcpy.management.DeleteRows(layer)
            edit.stopOperation()
            edit.stopEditing(True)
        else:
            arcpy.management.DeleteRows(layer)



    def custom_spatial_join(self,geodatabase_with_join_layer,join_layer,list_of_fields_to_put_in_target_layer):
        input = self.input_dataframe
        joined_layer = ReadGeopandas(geodatabase_with_join_layer,join_layer).create_df_from_sde(reproject=True,projection_number=4326)
        for field in list_of_fields_to_put_in_target_layer:
            if field not in joined_layer.columns:
                raise Exception("This field is not in the joined layer. Please check again")
        for field in list_of_fields_to_put_in_target_layer:
            input[field]=None
        num_rows=input.shape[0]
        joined_layer_copy = joined_layer.copy(deep=True)
        joined_lat = []
        joined_lon = []
        for shape in joined_layer_copy["Shape"]:
            joined_lat.append(shape.getPart(0).Y)
            joined_lon.append(shape.getPart(0).Y)
        joined_lat = numpy.array(joined_lat)
        joined_lon = numpy.array(joined_lon)
        stack_dist = numpy.column_stack((joined_lon,joined_lat))
        tree = KDTree(stack_dist)

        index = 0

        for row in input.itertuples(index=False):
            # grab the line
            geom = input.at[index,"Shape"]
            lon = geom.getPart(0).X
            lat = geom.getPart(0).Y
            point = numpy.array([lon,lat])
            dist, ind = tree.query(point,k=1)
            for field in list_of_fields_to_put_in_target_layer:
                attribute = joined_layer_copy[field].iloc[ind]
                input.at[index,field] = attribute

            index+=1
            print(index,"of",num_rows)
        return input
        


    def lrs_integration_point_to_point(self,geodatabase_with_lrs,lrs_point_layer):
        input = self.input_dataframe
        lrs_joined = ReadGeopandas(geodatabase_with_lrs,lrs_point_layer).create_df_from_sde(reproject=True,projection_number=4326)

        input["LRS_ID"]=None
        num_rows = input.shape[0]
        lrs_layer_copy = lrs_joined.copy(deep=True)
        lrs_layer_copy['HUND_LATITUDE'] = lrs_layer_copy['HUND_LATITUDE'].astype(float)
        lrs_layer_copy['HUND_LONGITUDE'] = lrs_layer_copy['HUND_LONGITUDE'].astype(float)
        lrs_lat = lrs_layer_copy["HUND_LATITUDE"].to_numpy()
        lrs_lon = lrs_layer_copy["HUND_LONGITUDE"].to_numpy()
        stack_dist = numpy.column_stack((lrs_lon,lrs_lat))
        tree = KDTree(stack_dist)

        index = 0

        for row in input.itertuples(index=False):
            # grab the line
            geom = input.at[index,"Shape"]
            lon = geom.getPart(0).X
            lat = geom.getPart(0).Y
            point = numpy.array([lon,lat])
            dist, ind = tree.query(point,k=1)
            lrs_shape = lrs_layer_copy.at[ind,"Shape"].getPart(0)
            lrs_point = arcpy.Point(lrs_shape.X,lrs_shape.Y)
            point_with_m = arcpy.Point(lon,lat)
            lrs_pg= arcpy.PointGeometry(lrs_point,arcpy.SpatialReference(4326))
            gtfs_pg = arcpy.PointGeometry(point_with_m,arcpy.SpatialReference(4326))
            lrs_id = lrs_layer_copy["ROUTE_ID"].iloc[ind]
            input.at[index,"LRS_ID"] = lrs_id

            index+=1
            print(index,"of",num_rows)
        return input

    def ardi_creator(self,geodatabase_with_lrs,lrs_point_layer):
        lrs_joined = ReadGeopandas(geodatabase_with_lrs,lrs_point_layer).create_df_from_sde(reproject=True,projection_number=4326)
        lrs_joined["BEGIN_POINT"]=lrs_joined["BEGIN_POINT"].astype(float)

        unique_list = lrs_joined["ROUTE_ID"].unique()

        adict = {
            "Shape":[],
            "ROUTE_ID":[],
            "CARDINAL_DIRECTION":[],
            "STREET_NAME":[],
            "DATM_TOWN":[],
            "YEAR":[],
            "PRI_SEC_STATUS":[],
            "BEGIN_POINT":[],
            "END_POINT":[]
            }

        lrs_length = len(unique_list)
        index=0

        for lrs in unique_list:
            #df = lrs_joined[lrs_joined["ROUTE_ID"]==lrs].sort_values(by=["BEGIN_POINT"],inplace=True,ascending=True)
            df = lrs_joined[lrs_joined["ROUTE_ID"]==lrs]
            newdf = df.sort_values(by=["BEGIN_POINT"],ascending=True)
            newdfnew = newdf.reset_index()
            df_shape = newdfnew.shape[0]
            df_ind = 0
            for _ in newdfnew.itertuples():
                if df_ind+1 == df_shape-1:
                    break
                else:
                    point1 = newdfnew.at[df_ind,"Shape"]
                    point2 = newdfnew.at[df_ind+1,"Shape"]
                    a1,d1=point1.angleAndDistanceTo(point2,'GEODESIC')
                    feet_dist = d1*3.28084
                    if feet_dist > 300:
                        pass
                    else:
                        point1 = newdfnew.at[df_ind,"Shape"].getPart(0)
                        point2 = newdfnew.at[df_ind+1,"Shape"].getPart(0)
                        array = arcpy.Array()
                        array.add(point1)
                        array.add(point2)
                        polyline = arcpy.Polyline(array,arcpy.SpatialReference(4326),has_m=True)
                        adict["Shape"].append(polyline)
                        adict["ROUTE_ID"].append(newdfnew.at[df_ind,"ROUTE_ID"])
                        adict["CARDINAL_DIRECTION"].append(newdfnew.at[df_ind,"CARDINAL_DIRECTION"])
                        adict["STREET_NAME"].append(newdfnew.at[df_ind,"STREET_NAME"])
                        adict["DATM_TOWN"].append(newdfnew.at[df_ind,"DATM_TOWN"])
                        adict["YEAR"].append(newdfnew.at[df_ind,"YEAR"])
                        adict["PRI_SEC_STATUS"].append(newdfnew.at[df_ind,"PRI_SEC_STATUS"])
                        adict["BEGIN_POINT"].append(newdfnew.at[df_ind,"BEGIN_POINT"])
                        adict["END_POINT"].append(newdfnew.at[df_ind+1,"BEGIN_POINT"])

                df_ind+=1
            print("on",index,"of",lrs_length)
            index+=1

        

        storage_df = pd.DataFrame(adict,columns=["Shape","ROUTE_ID","CARDINAL_DIRECTION","STREET_NAME","DATM_TOWN","YEAR","PRI_SEC_STATUS","BEGIN_POINT","END_POINT"])
        return storage_df
                                                                                    # for lrs geometry turn this to false
    def lrs_integration(self,geodatabase_with_lrs,lrs_point_layer,use_original_geometry = True):
        #lrs_layer_dataframe = ReadGeopandas(geodatabase_with_lrs,lrs_point_layer).create_df_from_sde(reproject=True,projection_number=4326)
        lrs_layer_dataframe = ReadGeopandas(geodatabase_with_lrs,lrs_point_layer).create_df_from_sde()
        self.input_dataframe["LRS_ID"]=None
        num_rows_gtfs = self.input_dataframe.shape[0]
        lrs_layer_copy = lrs_layer_dataframe.copy(deep=True)
        lrs_layer_copy['HUND_LATITUDE'] = lrs_layer_copy['HUND_LATITUDE'].astype(float)
        lrs_layer_copy['HUND_LONGITUDE'] = lrs_layer_copy['HUND_LONGITUDE'].astype(float)
        lrs_lat = lrs_layer_copy["HUND_LATITUDE"].to_numpy()
        lrs_lon = lrs_layer_copy["HUND_LONGITUDE"].to_numpy()
        stack_dist = numpy.column_stack((lrs_lon,lrs_lat))
        tree = KDTree(stack_dist)

        adict = {i:[] for i in self.input_dataframe.columns}
        column_names = [i for i in self.input_dataframe.columns]
    
        index = 0

        for row in self.input_dataframe.itertuples(index=False):
            # grab the line
            geom = self.input_dataframe.at[index,"Shape"]
            
        
            list_of_arrays = []
            for array in [geom.getPart(j) for j in range(len(geom.getPart()))]:
                list_of_points = []
                for apoint in array:
                    list_of_points.append(apoint)
                list_of_arrays.append(list_of_points)
            # iterate through all the points that make up the line
            #ids_test = []
            for array in list_of_arrays:
                line_info_dict={}
                for point in array:
                    gtfs_lon = point.X
                    gtfs_lat = point.Y
                    gtfs_point = numpy.array([gtfs_lon,gtfs_lat])
                    dist, ind = tree.query(gtfs_point,k=1)
                    lrs_shape = lrs_layer_copy.at[ind,"Shape"].getPart(0)
                    lrs_point = arcpy.Point(lrs_shape.X,lrs_shape.Y)
                    point_with_m = arcpy.Point(gtfs_lon,gtfs_lat)
                    lrs_pg= arcpy.PointGeometry(lrs_point,arcpy.SpatialReference(4326))
                    gtfs_pg = arcpy.PointGeometry(point_with_m,arcpy.SpatialReference(4326))
                    lrs_id = lrs_layer_copy["ROUTE_ID"].iloc[ind]
                    a1,d1=lrs_pg.angleAndDistanceTo(gtfs_pg,'GEODESIC')
                    feet_dist = d1*3.28084
                #ids_test.append(lrs_id)
                    if feet_dist > 100:
                        line_info_dict[point_with_m]="off_network"
                    else:
                        if use_original_geometry:
                            line_info_dict[point_with_m]=lrs_id
                        else:
                            line_info_dict[lrs_point]=lrs_id
            #print(ids_test)
                values = list(line_info_dict.values())
            
    
                new_line_info_dict = {}
                for key, value in line_info_dict.items():
                    if values.count(value)>0:
                    # if distance > 30: new_lline_info_dict[key]="off network" else below
                        new_line_info_dict[key]=value
                    else:
                    # if distance > 30: new_lline_info_dict[key]="off network" else below
                        new_line_info_dict[key]="sus"
            # separate dict into 2 lists (points and lrs_ids for indexing purposes)
                points = []
                lrs_ids = []
            
                for k, v in new_line_info_dict.items():
                    points.append(k)
                    lrs_ids.append(v)
            #print(lrs_ids)
            #print("-------------------------------------------")
            # if the lrs_id is labeled "sus". turn the value into the previous lrs_id
                for i in range(len(lrs_ids)):
                    if i == 0:
                    #pass
                    # if the first value is sus. keep searching until one that isnt sus is found
                        if lrs_ids[i] == "sus":
                            lrs_ids[i]=lrs_ids[i+1]
                            if lrs_ids[i+1] == "sus":
                                for j in range(len(lrs_ids)):
                                    if lrs_ids[j] != "sus":
                                        lrs_ids[i] = lrs_ids[j]
                                        break
                        else:
                            pass
                # if any value is sus. take the previous value as the true value
                    elif lrs_ids[i]=="sus":
                        lrs_ids[i]=lrs_ids[i-1]
            # make sure points doesnt have alternating values (cant have a line with only 1 point) turns in between value to outside value
                for i in range(len(lrs_ids)):
                    if i == 0:
                        pass
                    elif i == len(lrs_ids)-1:
                        if lrs_ids[len(lrs_ids)-1] != lrs_ids[i-1]:
                            lrs_ids[len(lrs_ids)-1]=lrs_ids[i-1]
                    elif lrs_ids[i] != lrs_ids[i+1] and lrs_ids[i]!=lrs_ids[i-1]:
                        lrs_ids[i]=lrs_ids[i-1]
        
            # check for pesky first index as lone wolf, everything else is taken care of. 
                if len(lrs_ids)>1 and lrs_ids[0]!=lrs_ids[1]:
                    lrs_ids[0]=lrs_ids[1]

            # split points and lines list based on indices of changing lrs id
                new_points = []
                new_lrs_ids = []
                change_count = 0
                for i in range(len(lrs_ids)):
                    if lrs_ids[i]!=lrs_ids[i-1]:
                        new_lrs_ids.append(lrs_ids[change_count:i])
                        new_points.append(points[change_count:i])
                        change_count = i
                    elif i == len(lrs_ids)-1:
                        new_lrs_ids.append(lrs_ids[change_count:i+1])
                        new_points.append(points[change_count:i+1])
                if len(new_lrs_ids[0])==0:
                    new_lrs_ids.pop(0)
                    new_points.pop(0)
            # iterate over the list of lists and create lines and add the data to the dictionary
                for line in range(len(new_lrs_ids)):
                    array = arcpy.Array()
                # for consistency. add the last point of the last line. 
                    if line != 0:
                        array.add(new_points[line-1][-1])
                # add the points to arcpy array
                    for point in new_points[line]:
                        array.add(point)
                    polyline =arcpy.Polyline(array,arcpy.SpatialReference(4326))
                    the_lrs_id = new_lrs_ids[line][0]
                    if the_lrs_id == "sus":
                        the_lrs_id = "Check for Review" 
                    for name in column_names:
                        if name == "Shape":
                            adict["Shape"].append(polyline)
                        elif name == "LRS_ID":

                            adict["LRS_ID"].append(the_lrs_id)
                        else:
                            adict[name].append(getattr(row,name))
        

            print("ON INDEX",index, "OUT OF",num_rows_gtfs)
        
            index+=1
            
    
        new_storage_df =pd.DataFrame(adict,columns=[i for i in self.input_dataframe.columns])
        return new_storage_df

    def lrs_integration_gpal(self,geodatabase_with_lrs,lrs_point_layer,use_original_geometry = True):
        lrs_layer_dataframe = ReadGeopandas(geodatabase_with_lrs,lrs_point_layer).create_df_from_sde()
        lrs_layer_dataframe["HUND_LATITUDE"]=None
        lrs_layer_dataframe["HUND_LONGITUDE"]=None
        count = 0 
        for row in lrs_layer_dataframe.itertuples(index=False):
            lrs_layer_dataframe.at[count,"HUND_LATITUDE"]=lrs_layer_dataframe.at[count,"Shape"].getPart(0).Y
            lrs_layer_dataframe.at[count,"HUND_LONGITUDE"]=lrs_layer_dataframe.at[count,"Shape"].getPart(0).X
            count+=1

        self.input_dataframe["LRS_ID"]=None
        num_rows_gtfs = self.input_dataframe.shape[0]
        lrs_layer_copy = lrs_layer_dataframe.copy(deep=True)
        lrs_layer_copy['HUND_LATITUDE'] = lrs_layer_copy['HUND_LATITUDE'].astype(float)
        lrs_layer_copy['HUND_LONGITUDE'] = lrs_layer_copy['HUND_LONGITUDE'].astype(float)
        lrs_lat = lrs_layer_copy["HUND_LATITUDE"].to_numpy()
        lrs_lon = lrs_layer_copy["HUND_LONGITUDE"].to_numpy()
        stack_dist = numpy.column_stack((lrs_lon,lrs_lat))
        tree = KDTree(stack_dist)

        adict = {i:[] for i in self.input_dataframe.columns}
        column_names = [i for i in self.input_dataframe.columns]
    
        index = 0

        for row in self.input_dataframe.itertuples(index=False):
            # grab the line
            geom = self.input_dataframe.at[index,"Shape"]
            line_info_dict={}
            # iterate through all the points that make up the line
       
            for point in geom.getPart(0):
                gtfs_lon = point.X
                gtfs_lat = point.Y
                gtfs_point = numpy.array([gtfs_lon,gtfs_lat])
                dist, ind = tree.query(gtfs_point,k=1)
                lrs_shape = lrs_layer_copy.at[ind,"Shape"].getPart(0)
                lrs_point = arcpy.Point(lrs_shape.X,lrs_shape.Y)
                point_with_m = arcpy.Point(gtfs_lon,gtfs_lat)
                lrs_pg= arcpy.PointGeometry(lrs_point,arcpy.SpatialReference(4326))
                gtfs_pg = arcpy.PointGeometry(point_with_m,arcpy.SpatialReference(4326))
                lrs_id = lrs_layer_copy["ROUTE_ID"].iloc[ind]
                a1,d1=lrs_pg.angleAndDistanceTo(gtfs_pg,'GEODESIC')
                feet_dist = d1*3.28084
                if feet_dist > 100:
                    line_info_dict[point_with_m]="off_network"
                else:
                    if use_original_geometry:
                        line_info_dict[point_with_m]=lrs_id
                    else:
                        line_info_dict[lrs_point]=lrs_id
      
            values = list(line_info_dict.values())
 
            new_line_info_dict = {}
            for key, value in line_info_dict.items():
                if values.count(value)>1:
                    # if distance > 30: new_lline_info_dict[key]="off network" else below
                    new_line_info_dict[key]=value
                else:
                    # if distance > 30: new_lline_info_dict[key]="off network" else below
                    new_line_info_dict[key]="sus"
            # separate dict into 2 lists (points and lrs_ids for indexing purposes)
            points = []
            lrs_ids = []
            for k, v in new_line_info_dict.items():
                points.append(k)
                lrs_ids.append(v)
            # if the lrs_id is labeled "sus". turn the value into the previous lrs_id
            for i in range(len(lrs_ids)):
                if i == 0:
                    #pass
                    # if the first value is sus. keep searching until one that isnt sus is found
                    if lrs_ids[i] == "sus":
                        lrs_ids[i]=lrs_ids[i+1]
                        if lrs_ids[i+1] == "sus":
                            for j in range(len(lrs_ids)):
                                if lrs_ids[j] != "sus":
                                    lrs_ids[i] = lrs_ids[j]
                                    break
                    else:
                        pass
                # if any value is sus. take the previous value as the true value
                elif lrs_ids[i]=="sus":
                    lrs_ids[i]=lrs_ids[i-1]
            # make sure points doesnt have alternating values (cant have a line with only 1 point) turns in between value to outside value
            for i in range(len(lrs_ids)):
                if i == 0:
                    pass
                elif i == len(lrs_ids)-1:
                    if lrs_ids[len(lrs_ids)-1] != lrs_ids[i-1]:
                        lrs_ids[len(lrs_ids)-1]=lrs_ids[i-1]
                elif lrs_ids[i] != lrs_ids[i+1] and lrs_ids[i]!=lrs_ids[i-1]:
                    lrs_ids[i]=lrs_ids[i-1]
        
            # check for pesky first index as lone wolf, everything else is taken care of. 
            if len(lrs_ids)>1 and lrs_ids[0]!=lrs_ids[1]:
                lrs_ids[0]=lrs_ids[1]

            # split points and lines list based on indices of changing lrs id
            new_points = []
            new_lrs_ids = []
            change_count = 0
            for i in range(len(lrs_ids)):
                if lrs_ids[i]!=lrs_ids[i-1]:
                    new_lrs_ids.append(lrs_ids[change_count:i])
                    new_points.append(points[change_count:i])
                    change_count = i
                elif i == len(lrs_ids)-1:
                    new_lrs_ids.append(lrs_ids[change_count:i+1])
                    new_points.append(points[change_count:i+1])
            if len(new_lrs_ids[0])==0:
                new_lrs_ids.pop(0)
                new_points.pop(0)
            # iterate over the list of lists and create lines and add the data to the dictionary
            for line in range(len(new_lrs_ids)):
                array = arcpy.Array()
                # for consistency. add the last point of the last line. 
                if line != 0:
                    array.add(new_points[line-1][-1])
                # add the points to arcpy array
                for point in new_points[line]:
                    array.add(point)
                polyline =arcpy.Polyline(array,arcpy.SpatialReference(4326))
                the_lrs_id = new_lrs_ids[line][0]
                if the_lrs_id == "sus":
                    the_lrs_id = "Check for Review" 
                for name in column_names:
                    if name == "Shape":
                        adict["Shape"].append(polyline)
                    elif name == "LRS_ID":

                        adict["LRS_ID"].append(the_lrs_id)
                    else:
                        adict[name].append(getattr(row,name))
        

            print("ON INDEX",index, "OUT OF",num_rows_gtfs)
        
            index+=1
        #if index == 100:
          #  break
    
        new_storage_df =pd.DataFrame(adict,columns=[i for i in self.input_dataframe.columns])
        return new_storage_df

class Convert:
    def __init__(self,input_dataframe):
        self.input_dataframe = input_dataframe


    def geojson_df_to_feature_class_df(self,input_spatial_reference_num,input_feature_type):
        # find if it's a POINT, POLYLINE, or POLYGON
        if input_feature_type == "POINT":
            for count,shape in enumerate(self.input_dataframe["Shape"]):
                point = arcpy.Point(shape[0],shape[1])
                point_geom = arcpy.PointGeometry(point,arcpy.SpatialReference(input_spatial_reference_num))
                self.input_dataframe.at[count,"Shape"]=point_geom
            return self.input_dataframe
        elif input_feature_type == "POLYLINE":
            for count,shape in enumerate(self.input_dataframe["Shape"]):
                array = arcpy.Array()
                for point in shape:
                    array.add(arcpy.Point(point[0],point[1]))
                polyline = arcpy.Polyline(array,arcpy.SpatialReference(input_spatial_reference_num))
                self.input_dataframe.at[count,"Shape"]=polyline
            return self.input_dataframe
        elif input_feature_type == "POLYGON":
            for count,shape in enumerate(self.input_dataframe["Shape"]):
                array = arcpy.Array()
                for line in shape:
                    point_array = arcpy.Array()
                    for point in line:
                        point = arcpy.Point(point[0],point[1])
                        point_array.add(point)
                    array.add(point_array)
                polygon = arcpy.Polygon(array,arcpy.SpatialReference(input_spatial_reference_num))
                        
                self.input_dataframe.at[count,"Shape"]=polygon
            return self.input_dataframe

    def feature_service_df_to_feature_class_df(self,input_spatial_reference_num,input_feature_type):
        
        if input_feature_type == "POINT":
            self.input_dataframe = self.input_dataframe.astype(str)
            for count,shape in enumerate(self.input_dataframe["Shape"]):
                shape = json.loads(shape)
                point = arcpy.Point(shape['x'],shape['y'])
                point_geom = arcpy.PointGeometry(point,arcpy.SpatialReference(input_spatial_reference_num))
                self.input_dataframe.at[count,"Shape"]=point_geom
            self.input_dataframe.drop('objectid', axis=1, inplace=True)
            return self.input_dataframe
        elif input_feature_type == "POLYLINE":
            self.input_dataframe = self.input_dataframe.astype(str)
            for count,shape in enumerate(self.input_dataframe["Shape"]):
                shape = json.loads(shape)
                array = arcpy.Array()
                for point in shape['paths'][0]:
                    array.add(arcpy.Point(point[0],point[1]))
                polyline = arcpy.Polyline(array,arcpy.SpatialReference(input_spatial_reference_num))
                self.input_dataframe.at[count,"Shape"]=polyline
            return self.input_dataframe
        elif input_feature_type == "POLYGON":
            drop_list = []
            for count,shape in enumerate(self.input_dataframe["Shape"]):
                if type(shape) == float:
                    drop_list.append(count)
            self.input_dataframe.drop(index=drop_list,inplace=True)
            self.input_dataframe = self.input_dataframe.astype(str)
            self.input_dataframe.reset_index(inplace=True)
            for count,shape in enumerate(self.input_dataframe["Shape"]):
                
                shape = json.loads(shape)
               
                array = arcpy.Array()
                for line in shape["rings"]:
                    point_array = arcpy.Array()
                    for point in line:
                        point = arcpy.Point(point[0],point[1])
                        point_array.add(point)
                    array.add(point_array)
                polygon = arcpy.Polygon(array,arcpy.SpatialReference(input_spatial_reference_num))
               
                self.input_dataframe.at[count,"Shape"]=polygon
            return self.input_dataframe


#Geoprocessing().truncate(r"C:\Users\hahnef\Documents\ArcGIS\Projects\Historic Bridges\Historic Bridges.gdb","frame_test",is_versioned=False)
#Geoprocessing().update_column(r"C:\Users\hahnef\Documents\ArcGIS\Projects\Historic Bridges\Historic Bridges.gdb","frame_test","Name","#00325","testing123")
#Geoprocessing().reproject_in_place(r"C:\Users\hahnef\Documents\ArcGIS\Projects\Historic Bridges\Historic Bridges.gdb","frame_test",4326)

#TRUNCATE TABLE FIRST METHOD
#df = ReadGeopandas(r"C:\Users\hahnef\Documents\ArcGIS\Projects\Historic Bridges\Historic Bridges.gdb","Historic_Bridges").create_df_from_sde()
#WriteGeopandas(r"C:\Users\hahnef\Documents\ArcGIS\Projects\Historic Bridges\Historic Bridges.gdb","frame_test",df,"POINT").write_fc_df_to_sde_force()
#df = ReadGeopandas(r"C:\Users\hahnef\Documents\ArcGIS\Projects\Historic Bridges\Historic Bridges.gdb","frame_test").create_df_from_sde()
#WriteGeopandas(r"C:\Users\hahnef\Documents\ArcGIS\Projects\Historic Bridges\Historic Bridges.gdb","frame_test",df,"POINT").append_df_to_fc(truncate_first=True,is_versioned=False)


#df = ReadGeopandas("","").create_df_cad_dwg()

#df = ReadGeopandas("","").create_df_from_kmz_or_kml(r"C:\Users\hahnef\Downloads\Historic_Bridge_Inventory-1991.kmz")
#WriteGeopandas(r"C:\Users\hahnef\AppData\Local\Temp\ArcGISProTemp30516\13ce41b9-0708-4c52-bb43-6d576d05e84b\Default.gdb","zee_points",df,"POINT").write_fc_df_to_sde_force()

#print(ReadGeopandas("","").create_df_from_dbf(r"C:\Users\hahnef\Downloads\Connecticut.dbf"))

# polygon drops empty geometry when converting

#point_feature_service = ReadGeopandas("","").create_df_from_feature_service("https://arcgis-stg.dot.ct.gov/portalarcgisstg/","","https://arcgis-stg.dot.ct.gov/arcgisstg/rest/services/Hosted/ExistingDCFC2orLess/FeatureServer/1")
#polyline_feature_service = ReadGeopandas("","").create_df_from_feature_service("https://arcgis-stg.dot.ct.gov/portalarcgisstg/","","https://arcgis-stg.dot.ct.gov/arcgisstg/rest/services/Datamart/State_Routes_and_Local_Roads/FeatureServer/0")
#polygon_feature_service = ReadGeopandas("","").create_df_from_feature_service("https://arcgis-stg.dot.ct.gov/portalarcgisstg/","","https://arcgis-stg.dot.ct.gov/arcgisstg/rest/services/Historic_Bridges/FeatureServer/0")
#converted_point = Convert(point_feature_service).feature_service_df_to_feature_class_df(4326,"POINT")
#converted_line = Convert(polyline_feature_service).feature_service_df_to_feature_class_df(4326,"POLYLINE")
#converted_polygon = Convert(polygon_feature_service).feature_service_df_to_feature_class_df(4326,"POLYGON")
#WriteGeopandas(r"C:\Users\hahnef\Documents\ArcGIS\Projects\LRS_Test\LRS_Test.gdb","point_fc_fs",converted_point,"POINT").write_fc_df_to_sde_force()
#WriteGeopandas(r"C:\Users\hahnef\Documents\ArcGIS\Projects\LRS_Test\LRS_Test.gdb","line_fc_fs",converted_line,"POLYLINE").write_fc_df_to_sde_force()
#WriteGeopandas(r"C:\Users\hahnef\Documents\ArcGIS\Projects\LRS_Test\LRS_Test.gdb","polygon_fc_fs",converted_polygon,"POLYGON").write_fc_df_to_sde_force()


#point,line,polygon=ReadGeopandas("","").create_df_from_geojson(json_path="geojson_example.json")
#converted_point = Convert(point).geojson_df_to_feature_class_df(4326,"POINT")
#converted_line = Convert(line).geojson_df_to_feature_class_df(4326,"POLYLINE")
#converted_polygon = Convert(polygon).geojson_df_to_feature_class_df(4326,"POLYGON")
#WriteGeopandas(r"C:\Users\hahnef\Documents\ArcGIS\Projects\LRS_Test\LRS_Test.gdb","point_fc",converted_point,"POINT").write_fc_df_to_sde_force()
#WriteGeopandas(r"C:\Users\hahnef\Documents\ArcGIS\Projects\LRS_Test\LRS_Test.gdb","line_fc",converted_line,"POLYLINE").write_fc_df_to_sde_force()
#WriteGeopandas(r"C:\Users\hahnef\Documents\ArcGIS\Projects\LRS_Test\LRS_Test.gdb","polygon_fc",converted_polygon,"POLYGON").write_fc_df_to_sde_force()


#df = ReadGeopandas(r"C:\Users\hahnef\Documents\ArcGIS\Projects\LRS_Test\LRS_Test.gdb","bridgeport_stops").create_df_from_sde(reproject=True,projection_number=4326)
#geoprocessing_object=Geoprocessing(df).custom_spatial_join(r"C:\Users\hahnef\Documents\ArcGIS\Projects\LRS_Test\LRS_Test.gdb","state_n_local_point",["ROUTE_ID","CARDINAL_DIRECTION"])
#WriteGeopandas(r"C:\Users\hahnef\Documents\ArcGIS\Projects\LRS_Test\LRS_Test.gdb","framework_test_2000",geoprocessing_object,"POINT").write_df_to_sde_force()

#df = ReadGeopandas(r"C:\Users\hahnef\Documents\ArcGIS\Projects\LRS_Test\LRS_Test.gdb","bridgeport_stops").create_df_from_sde(reproject=True,projection_number=4326)
#geoprocessing_object=Geoprocessing(df)
#new_df = geoprocessing_object.lrs_integration_point_to_point(r"C:\Users\hahnef\Documents\ArcGIS\Projects\LRS_Test\LRS_Test.gdb","state_n_local_point")
#WriteGeopandas(r"C:\Users\hahnef\Documents\ArcGIS\Projects\LRS_Test\LRS_Test.gdb","framework_test_1000",new_df,"POINT").write_df_to_sde_force()



#create_dataframe = ReadGeopandas(r"C:\Users\hahnef\Documents\ArcGIS\Projects\Historic Bridges\Historic Bridges.gdb","test")
#df = create_dataframe.create_df_from_sde()
#geoprocessing_object = Geoprocessing(df)
#lrs_dataframe = geoprocessing_object.lrs_integration(r"C:\Users\hahnef\Documents\ArcGIS\Projects\LRS_Test\LRS_Test.gdb","state_n_local_point",use_original_geometry=False)

#another_gp_object = Geoprocessing()
#ardi_dataframe = another_gp_object.ardi_creator(r"C:\Users\hahnef\Documents\ArcGIS\Projects\LRS_Test\LRS_Test.gdb","state_n_local_point")
#print(ardi_dataframe)


#write = WriteGeopandas(r"C:\Users\hahnef\Documents\ArcGIS\Projects\Historic Bridges\Historic Bridges.gdb","class_test",df,"TABLE")
#write.write_df_to_sde_force()
#write.write_df_to_sde_custom()

#create_dataframe = ReadGeopandas(r"C:\Users\hahnef\Documents\ArcGIS\Projects\LRS_Test\LRS_Test.gdb","CTDOT_PublicTrans_Bus_GTFSBusRoutes")
#df = create_dataframe.create_df_from_sde(reproject=True,projection_number=4326)
#geoprocessing_object = Geoprocessing(df)
#lrs_dataframe = geoprocessing_object.lrs_integration(r"C:\Users\hahnef\Documents\ArcGIS\Projects\LRS_Test\LRS_Test.gdb","state_n_local_point",use_original_geometry=False)
#WriteGeopandas(r"C:\Users\hahnef\Documents\ArcGIS\Projects\LRS_Test\LRS_Test.gdb","CTDOT_PublicTrans_Bus_GTFSBusRoutes_LRS_geom",lrs_dataframe,"POLYLINE").write_fc_df_to_sde_force()

#create_dataframe = ReadGeopandas(r"C:\Users\hahnef\Documents\ArcGIS\Projects\LRS_Test\LRS_Test.gdb","CTDOT_PublicTrans_Bus_GTFSBusRoutes")
#df = create_dataframe.create_df_from_sde(reproject=True,projection_number=4326)
#geoprocessing_object = Geoprocessing(df)
#lrs_dataframe = geoprocessing_object.lrs_integration_gpal(r"C:\Users\hahnef\Documents\ArcGIS\Projects\LRS_Test\LRS_Test.gdb","state_n_local_line_gpal",use_original_geometry=False)
#WriteGeopandas(r"C:\Users\hahnef\Documents\ArcGIS\Projects\LRS_Test\LRS_Test.gdb","CTDOT_PublicTrans_Bus_GTFSBusRoutes_LRS_geom_gpal",lrs_dataframe,"POLYLINE").write_fc_df_to_sde_force()

