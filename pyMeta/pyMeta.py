'''YG Developed at March 20, 2018 for using dict orgnize metadata.'''

import pickle as pkl
import shutil
from time import gmtime, strftime
import os
import h5py
import numpy as np

flatten_nestlist = lambda l: [item for sublist in l for item in sublist]
"""a function to flatten a nest list
e.g., flatten( [ ['sg','tt'],'ll' ]   )
gives ['sg', 'tt', 'l', 'l']
"""
         
 
class H5Data(object):
    '''Dict class desinged for handling h5py file
    '''
    def __init__(self,filename, copy = False ):
        self.filename = filename
        now = strftime("%m_%d_%Y", gmtime())        
        if os.path.isfile( filename ):
            nf = filename[:-4] + '_copy_%s.h5'%now
            if copy:
                shutil.copyfile(filename, nf)
                print('Make copy of this file with filename as: %s.'%nf) 
        else:
            #open(self.filename, 'a').close()
            self.save_data( [1,2,3], 'None' )
        self.all_keys=[]       
        
    def save_multi_data( self, data_list, key_list, force=False):
        for (v,k) in list(zip( data_list, key_list )):
            #print(k,v)
            self.save_data( v, k, force= force )  
            
    def save_data( self, data, key, force=False):
        if isinstance(data, dict):
            self.save_dict( data, key, force=force)
        elif isinstance(data, (np.ndarray, list)):
            self.save_array( data, key, force=force) 
            
    def save_dict( self, dic, key=None, force=False):   
        fout = self.filename 
        try:
            keys = self.get_keys()
        except:
            keys = []
        if key not in keys or force is True:
            with h5py.File(fout, 'a') as hf:
                if key in keys:
                    del hf[key]
                recursively_save_dict_contents_to_group(hf, '/%s/'%key, dic)
                print( 'This dictionary: %s is exported to %s.'%(key, fout))
        else:
            print('This key: %s already exists.'%key)
            
    def save_dict2( self, dic, key, force=False):   
        fout = self.filename        
        try:
            keys = self.get_keys()
        except:
            keys = []
        if key not in keys or force is True:            
            with h5py.File(fout, 'r+') as hf:          
                if key in keys:
                    del hf[key]
                dict_data = hf.create_dataset( key, (1,), dtype='i')
                for k in list(dic.keys()):   
                    print(k)
                    try:
                        dict_data.attrs[k] = dic[k]
                        print(k)
                    except:
                        pass 
            print( 'This dictionary: %s is exported to %s.'%(key, fout))
        else:
            print('This key: %s already exists.'%key)
            
    def save_array(self, array, key, force=False):  
        fout = self.filename        
        try:
            keys = self.get_keys()
        except:
            keys = []
            
        if key not in keys or force is True:            
            with h5py.File(fout, 'a') as hf: 
                if key in keys:
                    del hf[key]
                data = hf.create_dataset(name= key, data = array )
                data.set_fill_value = np.nan 
                print( 'The array: %s is exported to %s.'%(key, fout))
        else:
            print('This key: %s already exists.'%key)
    
    def delete_keys(self, key):
        fout = self.filename 
        with h5py.File(fout,  "a") as hf:
            del hf[key]
            
    def get_keys(self):
        fout = self.filename      
        with h5py.File(fout, 'r') as hf: 
            return  list(hf.keys() ) 
        
    def load_data_notwork(self, key):  
        try:
            return self.load_array(key)
        except:
            return self.load_dict( key)        
        
    def load_array(self, key):
        fout = self.filename      
        with h5py.File(fout, 'r') as hf:             
            return np.array( hf.get( key ) )
        
    def load_dict(self, key):
        fout = self.filename      
        with h5py.File(fout, 'r') as hf:     
            return recursively_load_dict_contents_from_group(hf, '/%s/'%key)        
        
    def load_dict2(self, key):
        fout = self.filename      
        with h5py.File(fout, 'r') as hf: 
            din = hf.get( key  )   
            dout = {}
            for att in din.attrs:        
                dout[att] =  din.attrs[att]             
            return dout  
        

 


def split_slash_str( kstr):
    '''kstr:  string contains / '''
    if kstr[-1]=='/':
        kstr = kstr[:-1]
    return kstr.split('/')
    
def get_subdict_k_val(d,kstr):
    '''Get a sub dict,  key and val of a d in a level contains kstr
       kstr: can be upper_level_key/lower_level_key/more_lower_level_key/... '''
    ks = split_slash_str( kstr)  
    k=split_slash_str( kstr)[-1]
    for i in range(len(ks)-1):        
        d = d[ ks[i] ]
    if k not in list(d.keys()):
        v = None
    else:
        v = d[k]
    return d, k, v     

def get_sub_dict(d, kstr):
    '''Get a sub dict of a d in a level contains kstr
       kstr: can be upper_level_key/lower_level_key/more_lower_level_key/... '''
    ks = split_slash_str( kstr)  
    #k=split_slash_str( kstr)[-1]
    for i in range(len(ks)):        
        d = d[ ks[i] ]       
    return d   

def get_k_val(d, kstr):
    '''Get a sub dict of a d in a level contains kstr
       kstr: can be upper_level_key/lower_level_key/more_lower_level_key/... '''
    return get_subdict_k_val(d,kstr)[2] 

def del_k(d,kstr,verbose=True):
    sub, k, v = get_subdict_k_val(d,kstr)
    try:
        sub.pop(k)
        if verbose:
            print('Key: %s was deleted from the dict.'%k)
    except:
        print('There is no such key=%s in this dict.'%k)

def set_k_val( d, kstr, val,verbose=True):
    '''set value (val) of the key (key_str) for a dict (d) 
    key_str: can be upper_level_key/lower_level_key/more_lower_level_key/...
    '''
    sub, k, v = get_subdict_k_val(d,kstr)
    #verbose=False
    if  v is not None:
        print('This key is not None.')   
        #verbose=True
    if isinstance(v,list):
        if val not in sub[k]:
            sub[k].append(val)
            #sub[k] = flatten_nestlist(sub[k])
        if verbose:
            print('Append %s to this list.'%val)    
    elif isinstance(v,dict):
        sub[k].update(val) 
        if verbose:
            print('Update this sub dict with %s.'%val)      
    else:
        sub[k] = val
        if  v is not None:
            if verbose:
                print('Overwrite this key with  %s.'%val)
        else:
            if verbose:
                print('Create this key with  %s.'%val)            
            
def get_all_keys( d,kstr=None ):
    '''Get all keys for a dict'''
    if kstr is not None:
        d = get_sub_dict(d, kstr)
        print(d)
    if  type(d) is dict:   
        kd = {}
        def recursive_items(  d, ku = ''):
            for key, value in list(d.items()):
                if ku!='':
                    kd[key] = ku +'/'+ key
                else:
                    kd[key] = key
                if type(value) is dict:
                    recursive_items( value, kd[key] )            
        recursive_items(  d, ku = '')    
        return list( kd.values() )
    else:
        print('This (sub) dict is not a dictoinary!!!')
        return None


            
class metadict(object):
    '''Dict class desinged for handling metadata
    '''
    def __init__(self,filename ):
        self.filename = filename
        now = strftime("%m_%d_%Y", gmtime())        
        if os.path.isfile( filename ):
            self.data = self.load()
            nf = filename[:-4] + '_copy_%s.pkl'%now
            shutil.copyfile(filename, nf)
            print('Make copy of this file with filename as: %s.'%nf)
        else:
            self.data = {}
        self.all_keys=[]
    def init_data(self,dicts,force=False):
        if self.data =={}:
            self.data.update( dicts )
        else:
            if force:                
                self.data =  dicts
                print('This dict load from file %s is not Empty. Overwrite!!!'%self.filename)
            else:
                print('This dict load from file %s is not Empty. Can not overwrite!!!'%self.filename)
            
    def load(self):
        return pkl.load( open(self.filename,'rb') ) 
    def save(self):
        print('Save data as %s.'%self.filename)
        pkl.dump( self.data, open(self.filename,'wb') ) 
        
    def get_keys(self, kstr=None):
        '''if kstr is None:
               Get all keys in the first layer of the dict
           else:
               Get all keys in the level containing kstr       
        '''
        if kstr is not None:
            sub = get_sub_dict(self.data, kstr)
        else:
            sub=self.data
        try:
            return list(sub.keys())
        except:
            print('This val is not a dict.')
            return None
    def get_all_keys(self, kstr=None):
        '''Get all keys in the all subdicts'''
        return get_all_keys(self.data) 
    def get_val(self, kstr):
        return get_k_val(self.data, kstr)
       
    def update_k(self, kstr,val,verbose=True):
        set_k_val( self.data, kstr, val,verbose=verbose)
    def remove_k(self,kstr,verbose=True):
        del_k(self.data,kstr,verbose=verbose)
        
        
        

        
            
                
######################################
###Deal with saving dict to hdf5 file
def save_dict_to_hdf5(dic, filename):
    """
    ....
    """
    with h5py.File(filename, 'a') as h5file:
        recursively_save_dict_contents_to_group(h5file, '/', dic)

def load_dict_from_hdf5(filename):
    """
    ....
    """
    with h5py.File(filename, 'r') as h5file:
        return recursively_load_dict_contents_from_group(h5file, '/')
    
def recursively_save_dict_contents_to_group( h5file, path, dic):
    """..."""
    # argument type checking
    if not isinstance(dic, dict):
        raise ValueError("must provide a dictionary")        
        
    if not isinstance(path, str):
        raise ValueError("path must be a string")
    if not isinstance(h5file, h5py._hl.files.File):
        raise ValueError("must be an open h5py file")
    # save items to the hdf5 file
    for key, item in dic.items():
        #print(key,item)
        key = str(key)
        if isinstance(item, list):
            item = np.array(item)
            #print(item)
        if not isinstance(key, str):
            raise ValueError("dict keys must be strings to save to hdf5")
        # save strings, numpy.int64, and numpy.float64 types
        if isinstance(item, (np.int64, np.float64, str, np.float, float, np.float32,int)):
            #print( 'here' )
            h5file[path + key] = item
            if not h5file[path + key].value == item:
                raise ValueError('The data representation in the HDF5 file does not match the original dict.')
        # save numpy arrays
        elif isinstance(item, np.ndarray):            
            try:
                h5file[path + key] = item
            except:
                item = np.array(item).astype('|S9')
                h5file[path + key] = item
            if not np.array_equal(h5file[path + key].value, item):
                raise ValueError('The data representation in the HDF5 file does not match the original dict.')
        # save dictionaries
        elif isinstance(item, dict):
            recursively_save_dict_contents_to_group(h5file, path + key + '/', item)
        # other types cannot be saved and will result in an error
        else:
            #print(item)
            raise ValueError('Cannot save %s type.' % type(item))
            
            
def recursively_load_dict_contents_from_group( h5file, path):
    """..."""
    ans = {}
    for key, item in h5file[path].items():
        if isinstance(item, h5py._hl.dataset.Dataset):
            ans[key] = item.value
        elif isinstance(item, h5py._hl.group.Group):
            ans[key] = recursively_load_dict_contents_from_group(h5file, path + key + '/')
    return ans                    
        
        
        
        
        
        
        
        
                
            