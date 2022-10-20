
import datetime, calendar, time,pytz

import pytimber
import numpy as np
import pandas as pd
import datetime
import json

class Vacuum_dataholer():

    def __init__(self,fill_list, intervals):
        """
        Arguments:
        
        fill_list: list of integer fill numbers
        
        intervals: list of datetime tuples containing start and
        stop time for each fill

        
        """
        self.fill_list = fill_list
        self.intervals = intervals
        self._variables = set()
        self.json_serialized = False
        #keys:fill names values:vacuum varibles for each fll
        self.vac_data_dict = dict()

    @property
    def fill_names(self):
        return [str(fill) for fill in self.fill_list]

    @property
    def variables(self):
        return list(self._variables)

    def append_variables(self,variables):
        for var in variables:
            self._variables.add(var)
        

    def shift_timezone(self,timezone_shift):
        nsec_in_hour = 3600

        for fill in self.fill_names:
            for var in self.variables:  
                self.vac_data_dict[fill][var] = list(self.vac_data_dict[fill][var])
                self.vac_data_dict[fill][var] = self.vac_data_dict[fill][var]
                self.vac_data_dict[fill][var][0] = self.vac_data_dict[fill][var][0] + timezone_shift*nsec_in_hour

    def jsonify(self):
        """
        Turns array datatypes into list
        so they can be stored in json datafile format.
        """
        print(self.fill_names)
        print(self.variables)
        for fill in self.fill_names:
            for var in self.variables:
                #print(type(self.vac_data_dict[fill][var]))
                self.vac_data_dict[fill][var] = (
                    self.vac_data_dict[fill][var][0].tolist(), 
                    self.vac_data_dict[fill][var][1].tolist()
                )

        self.json_serialized = True
    
    def get_data(self,variables,timezone_shift = 0,source = "nxcals"):
        self.append_variables(variables)
        
        if not hasattr(self,"data_base"):
            self.data_base = pytimber.LoggingDB(source = source)
        
        
        for key, interval in zip(self.fill_names,self.intervals):
            start,stop = interval
            print("Getting data for fill: {}".format(key))
            self.vac_data_dict[key] = self.data_base.get(variables,start,stop)
            
        if timezone_shift != 0:
            self.shift_timezone(timezone_shift=timezone_shift)

        #Pytimber data is usually stored as numpy.array which
        #is not an appropreate format for storing in json datafiles...
        self.json_serialized = False

    def save_data(self):
        """
        Saves data for each fill in its own
        json file
        """
        self.jsonify()
        for fill in self.fill_names:
            path = "vacuum_" + fill + ".json"
            with open(path,"w") as f:
                print("Writing data for fill {}.\n".format(fill))
                json.dump(self.vac_data_dict[fill],f)
            


if __name__ == "__main__":

    db = pytimber.LoggingDB(source = "nxcals")

    
    variables = [
    "VGI.183.1L5.X.PR",
    "VGI.183.1R5.X.PR",
    "VGI.220.1L5.X.PR",
    "VGI.220.1R5.X.PR",
    "VGPB.147.5L8.B.PR",
    "VGPB.147.5L8.R.PR",
    "VGPB.183.1L5.X.PR",
    "VGPB.183.1R5.X.PR",
    "VGPB.220.1R5.X.PR",
    "VGPB.220.1L5.X.PR",
    "VGPB.7.4L5.X.PR",
    "VGPB.7.4R5.X.PR"
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

    vac_dh = Vacuum_dataholer(fills,fill_intervals)
    vac_dh.get_data(variables,timezone_shift=0)
    vac_dh.save_data()