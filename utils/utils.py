import pandas as pd 
from scipy.stats import ks_2samp 
from typing import Callable , List, Tuple, TypeAlias 
from datetime import datetime, date 


# ! Schema drift --------------------------
def schema_drift(ref_data:pd.DataFrame, new_data:pd.DataFrame): 
  # ? test if columns are the same ? 
  
  df_schemas:pd.DataFrame = pd.concat([ get_df_types(df) for df in [ref_data, new_data] ], axis=1) 
  df_schemas.columns = ['ref_data', 'new_data'] 
  df_schemas['schema_drift'] = df_schemas['ref_data'] != df_schemas['new_data'] 
  df_schemas['castable'] = [ is_castable(ref_data[c], new_data[c]) for c in df_schemas.index ] 
  df_drift = df_schemas[df_schemas['schema_drift']].copy() 
  return df_schemas, df_drift 


# ! Encoding drift ------------------------ 
def encoding_drift(ref_encoding:dict, new_encoding:dict): 
  df_encodings = pd.DataFrame({'ref_encoding':ref_encoding, 'new_encoding':new_encoding}) 
  df_encodings = df_encodings.map(lambda x: {} if pd.isna(x) else x) # Fill missing values with empty dictionary 
  df_encodings['encoding_drift'] = df_encodings['ref_encoding'] != df_encodings['new_encoding'] 
  df_drift = df_encodings[df_encodings['encoding_drift']].copy() 
  return df_encodings, df_drift 


# ! Distributional summary ---------------- 
def distributional_summary(df:pd.DataFrame) -> pd.DataFrame: 
  '''
  Takes a dataframe, returns a distributional summary (similar to describe, but more) 
  
  - type, (numerical, string, date, other) 
  - dtype, (specific dtype) 
  - count, (non-missing values) 
  - missing%, (percentage of missing values) 
  - unique%, (percentage of unique values) 
  - most_freq, (most frequent value) 
  - least_freq, (least frequent value) 
  - min, max, quartiles (for numerical values) 
  '''
  num_cols = list(df.select_dtypes('number').columns) 
  df_num_describe = df[num_cols].describe().drop('count') 

  N = df.shape[0]
  count = df.count()
  df_describe = pd.DataFrame( {'type': get_df_types(df)} ).T 
  df_describe.loc['dtype'] = df.dtypes
  df_describe.loc['count'] = count
  df_describe.loc['missing_p'] = (N-count)/N
  df_describe.loc['unique_p'] = { c:(df[c].value_counts() == 1).sum() for c in df.columns }
  df_describe.loc['unique_p'] = df_describe.loc['unique_p']/df_describe.loc['count'] 
  df_describe.loc['most_freq'] = { c:df[c].value_counts().idxmax() for c in df.columns } 
  df_describe.loc['least_freq'] = { c:df[c].value_counts().idxmin() for c in df.columns } 

  return pd.concat([df_describe, df_num_describe])


# ! Data drift ---------------------------- 


# For Long term, not 
def detect_data_drift(dfa:pd.DataFrame, dfb:pd.DataFrame): 
  '''
  Compares 2 dataframes and detect data drift 
  '''
  # find numerical columns 
  num_cols = dfa.select_dtypes(include="number").columns.intersection(dfb.select_dtypes(include="number").columns) 
  data_drift = {} 
  for c in num_cols: 
    _, pval =  ks_2samp(dfa[c], dfb[c]) 
    data_drift[c] = {"pval":pval, 'drifting':pval<0.01} 
  return data_drift 


# ! Validity --------------- 
#SerieValidityFunc: TypeAlias = Callable[[pd.DataFrame, str], List[bool]] 
SerieValidityMapper: TypeAlias = dict[str, Callable[[pd.DataFrame, str], List[bool]]] 

#def degrees_validity(extracted:pd.DataFrame, validity_map:dict[str, SerieValidityFunc]): 
def degree_validity(extracted:pd.DataFrame, mapper:SerieValidityMapper): 
  N = extracted.shape[0] 
  df_validity = pd.DataFrame( { c:func(extracted, c) for c, func in mapper.items() }).sum() 
  df_validity = df_validity/N 
  return df_validity 


# ! Completeness ----------- 
def degree_completeness(df:pd.DataFrame)-> pd.DataFrame: 
  N = df.shape[0] 
  df_completeness = pd.DataFrame( ((N - pd.isnull(df).astype(int).sum()) / N), columns=['completeness'] ) 
  return df_completeness 



def recursive_serialize(obj):
  """
  Recursively serializes nested objects to JSON-serializable types. 
  """
  if isinstance(obj, dict):
    return {k:recursive_serialize(v) for k, v in obj.items()}
  if isinstance(obj, list) or isinstance(obj, tuple) or isinstance(obj, set):
    return [ recursive_serialize(v) for v in obj ]
  if isinstance(obj, (datetime, date)):
    return obj.isoformat()
  if isinstance(obj, Exception):
    return {
        'type': type(obj).__name__, 
        'message':str(obj) 
      }
  return obj


def get_serie_type(serie:pd.Series) -> str: 
  if pd.api.types.is_datetime64_any_dtype(serie.dropna()): 
    return 'date' 
  if pd.api.types.is_numeric_dtype(serie.dropna()): 
    return 'numerical' 
  if pd.api.types.is_string_dtype(serie.dropna()): 
    return 'string' 
  return str(serie.dtype) 


def get_df_types(df:pd.DataFrame) -> pd.Series: 
  return pd.Series({ c:get_serie_type(df[c]) for c in df.columns}, name='types' ) 


def is_castable(seriea:pd.Series, serieb:pd.Series): 
  if get_serie_type(seriea) == get_serie_type(serieb): 
    return True 
  try:
    serieb.astype(seriea.dtype) 
    return True 
  except (ValueError, TypeError): 
    return False 
