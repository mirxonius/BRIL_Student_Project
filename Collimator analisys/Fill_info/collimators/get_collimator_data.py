import pytimber as timber

import os

import numpy as np
import datetime, calendar, time,pytz

import pandas as pd
import json


class Collimator_Dataholder():

    def __init__(self, name:str = None, beam:str = None,
                orientation:str = None, before_ip:bool = True,
                centre_distance:float = None) -> None:
        """
        Arguments:
        
        name: collimator name e.g. 'TCL', 'TCTP'
        
        beam: 1 or 2, depending on which beam it is collimating
        
        orientation: 'V' or 'H' indicating vertical and horizontal orentation respectively, or None if it's a TCL
        
        before_ip: boolean variable that determines the collimator  
        
        position relative to the IP
        
        centre_distance: collimator centre distance from interaction point 5
        """
        
        self.name = name
        self.beam = beam
        self.orientation = orientation
        self.before_ip = before_ip
        self.centre_distance = centre_distance
        self._variable_names = set()
        self.json_serialized = False

    @property
    def variable_names(self):
        return list(self._variable_names)


    def __str__(self) -> str:
        return self.name



    def check_variable_format(self,variables:list):
        """
        Returns: variables - list of compatible variable names 
        """
        name_length = len(self.name)
        formated_variables = []
        for i,var in enumerate(variables):
            if var[:name_length] != self.name:
                #print("NAME: ",self.name)
                #print("",self.beam)
                #print(var)
                formated_variables.append(
                    self.name + ".B" + str(self.beam) + var)

            
        return formated_variables


    def get_data(self,variables:list,interval:tuple,check_format = True, 
    timezone_shift:int = 2,source = "nxcals"):
        """
        varibles: list of strings containing variable names

        interval: tuple formated as (start,stop)
        start and stop can be integers or datetime objects

        check_format: boolean if set to True will preform variable name 
        formatting that is compatible with database querying. If set to Ture
        will call function check_variable_format(). Set to True by default.

        timezone_shift: relative time shift from UTC timezone

        Returns:
        self.data: dictionary cointaining variable names as keys and 
        their values as dict values.

        """
        if not hasattr(self,"data_base"):
            self.data_base = timber.LoggingDB(source = source)

        start,stop = interval

        if False:#timezone_shift:
            start = start + timezone_shift*3600#datetime.timedelta(hours = timezone_shift)
            stop = stop + timezone_shift*3600#datetime.timedelta(hours = timezone_shift)
        
        if check_format:
            #Also adds formated variable names into self.variable_names set.
            formated_variables = self.check_variable_format(variables)
            self.data = self.data_base.get(formated_variables,start,stop)
            
            for var in formated_variables:
                self._variable_names.add(var)
        
        else:
            self.data = self.data_base.get(variables,start,stop)
            for var in variables:
                self._variable_names.add(var)

        self.json_serialized = False
        return self.data

    def shift_data_timezone(self,hour_shift: int)->None:
        """
        Shifts timestamps in all data

        Arguments:
        
        hour_shift: number of hours datatimestamps should be shifted by, can be 
        positive or negative.

        Returns: None
      """
        if not hasattr(self,"data"):
            print("No data loaded!")
            return
        
        hour_in_sec = 3600
        for var in self.variable_names:
            self.data[var] = list(self.data[var])
            self.data[var][0] = self.data[var][0] + hour_shift*hour_in_sec
        

    def flatten(self):
        
        for var in self.variable_names:
            values = []
            times = []
            N = len(self.data[var][0])
            for i in range(0,N-1):
                values.append(self.data[var][1][i])
                values.append(self.data[var][1][i])
                times.append(self.data[var][0][i])
                times.append(self.data[var][0][i+1])

            if self.data[var][0].size != 0:   
                values.append(self.data[var][1][-1])
                times.append(self.data[var][0][-1])
            
            self.data[var] = (times.copy(),values.copy())
        self.json_serialized = True


    def jsonify_data(self):
        if self.json_serialized:
            return self.data

        placeholder_data = {}
        for key in self.data.keys():
            time_var, value_var = self.data[key]
            placeholder_data[key] = (time_var.tolist(), value_var.tolist())
        return placeholder_data                

    def save_data(self,file_name:str = None):
        """
        jsonifys data and stores it in a json file
        
            Arguments:
        
        file_name: assumed format: 'path_to_file.json'

        NOTE: This method assumes the following data structure
        in self.data = {'key_name':(np.array, np.array), . . . }
        
        """
        if not hasattr(self,"data"):
            print("No data is loaded!")
            return -1


        #To jsonify the data we use a placeholder
        #dict to store it before loading, this uses
        #more memory than simply casting the self.data
        #dict elements to lists. We do this to 
        #preserve the numpy stucture in self.data in case the 
        #user plans to use the data for further analysis
        placeholder_data = {}
        for key in self.data.keys():
            time_var, value_var = self.data[key]
            placeholder_data[key] = (time_var.tolist(), value_var.tolist())
        
        with open(file_name,'w') as file:
            print("Saving data to file")
            json.dump(placeholder_data,file)



class Multi_Collimator():
    """
    Dataholder for multiple collimator data
    """
    def __init__(self, collimators:pd.DataFrame) -> None:
        """
        Arguments:
        collimators: pandas DataFrame contaning enough information to build
        a Collimator_Dataholder object
        Data frame should enclude the following columns: 'name', 'beam', 'orientation', 'before_ip' (optional: 'centre_distance')
        """
        self.collimator_info = collimators
        self.name_and_beam = []
        for idx,row in self.collimator_info.iterrows():
            self.name_and_beam.append(row["name"] +'.B' +str(row["beam"]))

        self.coll_dataholders = self.create_collimator_dataholders()

    

    def create_collimator_dataholders(self):
        """
        Creates a list of ollimator_Dataholder objects for each
        collimator in self.collimators
        """

        collimator_list = list()
        for (idx,row) in self.collimator_info.iterrows():
            collimator_list.append(
                Collimator_Dataholder(row["name"],
                                      row["beam"],
                                      row["orientation"],
                                      row["before_ip"],
                )
            )
        return collimator_list
        

    def get_data(self,variables:list,interval:tuple,
    hour_shift:int = 0, jsonify:bool = True):
        self.data  = dict()
        for collimator,(idx,coll_info) in zip(self.coll_dataholders,self.collimator_info.iterrows()):
            data = collimator.get_data(variables,interval=interval)
            collimator.shift_data_timezone(hour_shift = hour_shift)
            collimator.flatten()
            data = collimator.jsonify_data()
            self.data.update({self.name_and_beam[idx] : data})

        return self.data
    
    def save_data(self,file_name)->None:
        jsonified = {}

        for key,collim in zip(self.data.keys(),self.coll_dataholders):
            jsonified[key] = collim.jsonify_data()


        with open(file_name,'w') as f:
            json.dump(jsonified,f)


class Fill_Collimator_Monitor():
    """
    Object used for easier data collecton for multiple
    collimators and multiple fills
    """

    def __init__(self,fill_dict:dict,multi_collimator:Multi_Collimator,file_list:list = None) -> None:
        """
        Arguments:
        
        fill_dict: dictionary where keys are fill numbers and values are
        tuples containg datetime objects corresponding to fill start and fill
        end time; both as datetime objects

        multi_collimator: Multi_Collimator object gatheres data of interest
        
        file_list: list of paths or strings that correspond to json filles that 
        the collimator data should be written to.
        If is left as none Fill_Collimator_Monitor preforms data saving automatically
        """
        self.fill_dict = fill_dict
        self.multi_collimator = multi_collimator
        if file_list is None:
            self.file_list = list()
        else:
            self.file_list = file_list

        self.data_gathered = False
        self.data_dict = dict() #keys: fills, values: collimator data

    @property
    def fills(self):
        return list(self.fill_dict.keys())
    
    @property
    def intervals(self):
        return list(self.fill_dict.values())


    def get_data(self,variables:list,hour_shift:int = 0):
        """
        Arguments:
        
        variables: list of variables that should be queried
        
        hour_shift: pytimber timezone correction
        """

        for fill in self.fills:
            print("Gathering data for fill {}".format(fill))
            self.data_dict[fill] = self.multi_collimator.get_data(variables=variables,
                                            interval = self.fill_dict[fill],
                                            hour_shift = hour_shift,jsonify = True)
          

        self.data_gathered = True
        return self.data_dict

    def make_fill_filename(self):
        """
        Returns: list of file_names
        """
        file_names = []
        for fill in self.fills:
            file_names.append(
                "collimator_data_" + fill + '.json'
            )

        return file_names

    def save_data(self,file_list:list = None):
        """
        Arguments:
        file_list: provides a list of paths where the 
        data should be saved, if not None this overrides
        the file_list given in the object constructor.
        """
        if not self.data_gathered:
            print("No data gathered")
            return -1
        

        if file_list is None and len(self.file_list) == 0:
            #Adds fill filesnames to an empty list
            self.file_list = self.file_list + self.make_fill_filename() 
        elif file_list is not None:
            #Otherwise the self.file_list is full and it is used
            self.file_list = file_list

        for fill,file in zip(self.fills,self.file_list):
            print("Writing data for fill {}".format(fill))
            with open(file,'w') as output_file:
                json.dump(self.data_dict[fill],output_file)
                            



#####################################
#####################################
#####################################
if __name__ == "__main__":
    

    collimators = [
       ("TCTPH.4L5",1) ,
        ("TCTPV.4L5",1),
        ("TCL.4R5",1),
        ("TCL.5R5",1),
        ("TCL.6R5",1),
        ("TCTPH.4R5",2),
        ("TCTPV.4R5",2),
        ("TCL.4L5",2),
        ("TCL.5L5",2),
        ("TCL.6L5",2)
    ]

    collimator_info = pd.DataFrame()

    for name,B in collimators:
        if "H" in name:
            orient = "H"
        elif "V" in name:
            orient = "V"
        else:
            orient = None
        
        if "TCL" in name:
            before_ip = False
        else:
            before_ip = True

        collimator_info = pd.concat([collimator_info,
                    pd.DataFrame({
                        "name":[name],
                        "beam":[B],
                        "orientation":[orient],
                        "before_ip": [before_ip]}
                    )
        ],ignore_index=True, axis = 0)


    all_collimators = Multi_Collimator(collimator_info)

    variables = [
        ":SET_LD",
        #":SET_LU",
        ":SET_RD",
        #":SET_RU"
        ]
    interval_7825 = (
        calendar.timegm(time.strptime('2022-06-21 07:44:32', '%Y-%m-%d %H:%M:%S')),
        calendar.timegm(time.strptime('2022-06-21 21:49:41', '%Y-%m-%d %H:%M:%S'))
        #datetime.datetime(2022,6,21,7,44,32),
        #datetime.datetime(2022,6,21,21,49,41)

    )

    interval_7963 = (

        calendar.timegm(time.strptime('2022-07-11 00:30:00', '%Y-%m-%d %H:%M:%S')),
        calendar.timegm(time.strptime('2022-07-11 12:28:5', '%Y-%m-%d %H:%M:%S'))
        #datetime.datetime(2022,7,11,0,30,0),
        #datetime.datetime(2022,7,11,12,28,5)
    )

    interval_7882 = (
        calendar.timegm(time.strptime('2022-06-28 07:32:13', '%Y-%m-%d %H:%M:%S')),
        calendar.timegm(time.strptime('2022-06-28 19:15:53', '%Y-%m-%d %H:%M:%S'))
        #datetime.datetime(2022,6,28,7,32,13,),
        #datetime.datetime(2022,6,28,19,15,53,)
    )

    interval_7884 = (
        calendar.timegm(time.strptime('2022-06-28 22:24:38', '%Y-%m-%d %H:%M:%S')),
        calendar.timegm(time.strptime('2022-06-29 00:34:25', '%Y-%m-%d %H:%M:%S'))
        #datetime.datetime(2022,6,28,22,24,38,),
        #datetime.datetime(2022,6,29,0,34,56,)
    )
    interval_7885 = (
        calendar.timegm(time.strptime('2022-06-29 00:34:25', '%Y-%m-%d %H:%M:%S')),
        calendar.timegm(time.strptime('2022-06-29 02:51:39', '%Y-%m-%d %H:%M:%S'))
        #datetime.datetime(2022,6,29,0,34,25,),  
        #datetime.datetime(2022,6,29,2,51,39,)
    )
    interval_7886 = (
        calendar.timegm(time.strptime('2022-06-29 02:51:39', '%Y-%m-%d %H:%M:%S')),
        calendar.timegm(time.strptime('2022-06-29 14:05:15', '%Y-%m-%d %H:%M:%S'))
        #datetime.datetime(2022,6,29,2,51,39,),
        #datetime.datetime(2022,6,29,14,5,15,)
    )
    interval_7889 = (
        calendar.timegm(time.strptime('2022-06-29 22:13:31', '%Y-%m-%d %H:%M:%S')),
        calendar.timegm(time.strptime('2022-06-30 03:31:48', '%Y-%m-%d %H:%M:%S'))
        #datetime.datetime(2022,6,29,22,13,31,tzinfo = pytz.UTC),
        #datetime.datetime(2022,6,30,3,31,48,tzinfo = pytz.UTC)
    )
    interval_7890 = (
        calendar.timegm(time.strptime('2022-06-30 01:31:45', '%Y-%m-%d %H:%M:%S')),
        calendar.timegm(time.strptime('2022-06-30 14:29:51', '%Y-%m-%d %H:%M:%S'))
        #datetime.datetime(2022,6,30,1,31,45,),
        #datetime.datetime(2022,6,30,14,29,51,)
    )

    interval_8091 = (
        calendar.timegm(time.strptime('2022-08-04 18:12:00', '%Y-%m-%d %H:%M:%S')),
        calendar.timegm(time.strptime('2022-08-04 21:48:1', '%Y-%m-%d %H:%M:%S'))
        #    datetime.datetime(2022,8,4,18,12,00,),
        #    datetime.datetime(2022,8,4,21,48,1,)

    )




    interval_8120 =(
        calendar.timegm(time.strptime('2022-08-12 15:9:13', '%Y-%m-%d %H:%M:%S')),
        calendar.timegm(time.strptime('2022-08-12 23:01:52', '%Y-%m-%d %H:%M:%S'))
        #datetime.datetime(2022,8,12,15,9,13,),
        #datetime.datetime(2022,8,12,23,1,52,)
    )

    interval_8099 = (
        calendar.timegm(time.strptime('2022-08-06 01:47:07', '%Y-%m-%d %H:%M:%S')),
        calendar.timegm(time.strptime('2022-08-06 04:35:18', '%Y-%m-%d %H:%M:%S'))
        
        #datetime.datetime(2022,8,6,1,47,5,tzinfo = pytz.UTC),
        #datetime.datetime(2022,8,6,4,35,18,tzinfo = pytz.UTC)
    )



    interval_8112 = (
        calendar.timegm(time.strptime("2022-08-09 01:05:06", '%Y-%m-%d %H:%M:%S')),
        calendar.timegm(time.strptime("2022-08-09 14:19:05", '%Y-%m-%d %H:%M:%S'))  

    )

    interval_8068 = (
        calendar.timegm(time.strptime("2022-07-29 19:52:15", '%Y-%m-%d %H:%M:%S')),
        calendar.timegm(time.strptime("2022-07-30 02:21:42", '%Y-%m-%d %H:%M:%S'))  

    )

###################
    interval_7872 = (
        calendar.timegm(time.strptime("2022-06-26 09:49:38", '%Y-%m-%d %H:%M:%S')),
        calendar.timegm(time.strptime("2022-06-26 11:29:58", '%Y-%m-%d %H:%M:%S'))   
    ) 

    interval_7874 = (
        calendar.timegm(time.strptime("2022-06-26 13:15:30", '%Y-%m-%d %H:%M:%S')),
        calendar.timegm(time.strptime("2022-06-26 17:44:12", '%Y-%m-%d %H:%M:%S'))   
    ) 

    interval_7875 = (
        calendar.timegm(time.strptime("2022-06-26 17:44:12", '%Y-%m-%d %H:%M:%S')),
        calendar.timegm(time.strptime("2022-06-26 20:43:20", '%Y-%m-%d %H:%M:%S'))   
    ) 
    interval_7888 = (
        calendar.timegm(time.strptime("2022-06-29 17:59:47", '%Y-%m-%d %H:%M:%S')),
        calendar.timegm(time.strptime("2022-06-29 22:13:31", '%Y-%m-%d %H:%M:%S'))   
    ) 

###################
    interval_7893 = (
        calendar.timegm(time.strptime("2022-06-30 22:12:33", '%Y-%m-%d %H:%M:%S')),
        calendar.timegm(time.strptime("2022-07-01 01:07:37", '%Y-%m-%d %H:%M:%S'))   
    ) 

    interval_7894 = (
        calendar.timegm(time.strptime("2022-07-01 01:07:37", '%Y-%m-%d %H:%M:%S')),
        calendar.timegm(time.strptime("2022-07-01 04:33:40", '%Y-%m-%d %H:%M:%S'))   
    ) 



    interval_7908 = (
        calendar.timegm(time.strptime("2022-07-03 06:41:02", '%Y-%m-%d %H:%M:%S')),
        calendar.timegm(time.strptime("2022-07-03 11:42:27", '%Y-%m-%d %H:%M:%S'))   
    ) 


    fills = [
            "7872",
            "7874",
            "7875",
            "7888",
            "8068",
            "8112",
            "8099",
            "8120",
            "8091",       
            "7825",
            "7963",
            "7882",
            "7884",
            "7885",
            "7886",
            "7889",
            "7890",
            "7893",
            "7894",
            "7908"
    ]
    
    fill_intervals = [
                    interval_7872,
                    interval_7874,
                    interval_7875,
                    interval_7888,
                    interval_8068,
                    interval_8112,
                    interval_8099,
                    interval_8120,
                    interval_8091,
                    interval_7825,
                    interval_7963,
                    interval_7882,
                    interval_7884,
                    interval_7885,
                    interval_7886,
                    interval_7889,
                    interval_7890, 
                    interval_7893,
                    interval_7894,
                    interval_7908  
                       ]


    
    
    fill_dict = {}
    for key,value in zip(fills,fill_intervals):
        fill_dict[key] = value


    fcm = Fill_Collimator_Monitor(
        fill_dict = fill_dict,
        multi_collimator = all_collimators
    )

    print("GETTING DATA")
    data = fcm.get_data(variables,hour_shift=0)
    print("SAVING DATA")
    fcm.save_data()

