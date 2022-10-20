
import pytimber as timber
import numpy as np
import datetime, pytz, calendar,time

import json
import pandas as pd
import os


class Beam_Dataholder():

    def __init__(self,fill:str,interval:tuple,loss_map =False) -> None:
        
        self.fill = fill
        self.interval = interval
        self.variables = set()
        self.beam_data = dict()
        self.json_serialized = False
        self.loss_map = loss_map

    @property
    def data_base(self):
        if hasattr(self,"_data_base"):
            return self._data_base
        else:
            self._data_base = timber.LoggingDB(source = "nxcals")
            return self._data_base

    @property
    def variable_list(self):
        return list(self.variables)

    def get_data(self,variables:list,shift:int = 0):
        for var in variables:
            self.variables.add(var)
        start,stop = self.interval
        if shift != 0:
            #start = start + datetime.timedelta(hours = shift)
            #stop = start + datetime.timedelta(hours = shift)
            self.beam_data = self.data_base.get(variables,start,stop)
            self.shift_timezone(shift=shift)
        else:
            self.beam_data = self.data_base.get(variables,start,stop)

        print(self.beam_data.keys())
        self.beam_data["start"] = start 
        self.beam_data["stop"] = stop

        self.json_serialized = False
        

    def jsonify(self):  
        for var in self.beam_data.keys():
            print(self.beam_data[var])
            if var == "start" or var == "stop":
                continue
            else:
             self.beam_data[var] = (
                self.beam_data[var][0].tolist(),
                self.beam_data[var][1].tolist()
            )

           
        
        self.json_serialized = True

    def shift_timezone(self,shift:int = 0):
        for var in self.variable_list:
            self.beam_data[var] = list(self.beam_data[var])
            self.beam_data[var][0] = self.beam_data[var][0] + shift*3600

    def save_data(self,path = None):
        if path is None:
            path = self.make_path()

        if not self.json_serialized:
            self.jsonify()

        with open(path,"w") as f:
            json.dump(self.beam_data,f)


    def make_path(self):
        if self.loss_map:
            return "loss_map_data_" + str(self.fill) + ".json"
        return "beam_data_" + str(self.fill) + ".json"


class Multi_Beam():
    
    def __init__(self, fill_dict:dict,loss_map = False):
        self.fill_dict = fill_dict
        self.fills = list( fill_dict.keys() )
        self.intervals = list( fill_dict.values())
        self.data_dict = dict()
        self.dataholders = {}
        self.loss_map = loss_map
        for fill,interval in zip(self.fills, self.intervals):
            self.dataholders[fill] = Beam_Dataholder(fill,interval,loss_map = self.loss_map)
    
    def get_fill_data(self,variables,shift:int = 0):
        
        for fill in self.fills:
            print("Getting data for fill {}".format(fill))
            self.dataholders[fill].get_data(variables,shift=shift)

    def save_fill_data(self,path = None):
        
        for fill in self.fills:
            print("Saving data for fill " + fill)
            self.dataholders[fill].save_data(path)

if __name__ == "__main__":
      
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
            #"7872",
            #"7874",
            #"7875",
            #"7888",
            "7825",
            "7882",
            "7884",
            #"7885",
            "7886",
            "7889"
    ]
    
    fill_intervals = [
                    #interval_7872,
                    #interval_7874,
                    #interval_7875,
                    #interval_7888,
                    interval_7825,
                    interval_7882,
                    interval_7884,
                    #interval_7885,
                    interval_7886,
                    interval_7889
                       ]

 
    fill_dict = {}
    for key,value in zip(fills,fill_intervals):
        fill_dict[key] = value

    multibeam = Multi_Beam(fill_dict=fill_dict,loss_map = False)
    
    variables = [
        "LHC.BCCM.B1.A:BEAM_ENERGY",
        "LHC.BCCM.B1.B:BEAM_ENERGY", 
        "LHC.BCCM.B2.A:BEAM_ENERGY",
        "LHC.BCCM.B2.B:BEAM_ENERGY",
        "LHC.CISA.CCR:BEAM_MODE",
        #"LHC.BCTDC.A6R4.B1:BEAM_INTENSITY",
        #"LHC.BCTDC.A6R4.B2:BEAM_INTENSITY",
        "BLMTI.04L5.B1I10_TCTPH.4L5.B1:LOSS_RS09",
        "BLMTI.04R5.B2I10_TCTPH.4R5.B2:LOSS_RS09",
        "BLMTI.04L5.B2E10_TCL.4L5.B2:LOSS_RS09",
        "BLMTI.04R5.B1E10_TCL.4R5.B1:LOSS_RS09",
        "BLMTI.04L5.B1I10_TCTPV.4L5.B1:LOSS_RS09",
        "BLMTI.04R5.B2I10_TCTPV.4R5.B2:LOSS_RS09",
        "BLMTI.05R5.B1E10_TCL.5R5.B1:LOSS_RS09",
        "BLMTI.05L5.B2E10_TCL.5L5.B2:LOSS_RS09",
        "BLMTI.06L5.B2E10_TCL.6L5.B2:LOSS_RS09",
        "BLMTI.06R5.B1E10_TCL.6R5.B1:LOSS_RS09"
    ]


    multibeam.get_fill_data(variables=variables,shift = 0)
    
    multibeam.save_fill_data()