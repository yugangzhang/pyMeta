'''YG Developed at March 20, 2018 for using dict orgnize metadata.'''

import pickle as pkl
import shutil
from time import gmtime, strftime
import os
flatten_nestlist = lambda l: [item for sublist in l for item in sublist]
"""a function to flatten a nest list
e.g., flatten( [ ['sg','tt'],'ll' ]   )
gives ['sg', 'tt', 'l', 'l']
"""
         
            

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
        
        
        

        
            
                
                
        
        
        
        
        
        
        
        
                
            