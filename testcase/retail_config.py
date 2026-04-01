import pandas as pd 
from pathlib import Path 

from utils.metaframe import MetaFrame
from utils.utils import SerieValidityMapper
from testcase.common_paths import DATA_RAW, DATA_PROCESS, DATA_FINAL, DATA_REF

from src.factory import ETLFactory 
from src.interface import IComponentContext
import src.validations as vl 
import src.transformations as tf 
from src.components import ( ETLComponent, 
  ExtractorContext, ValidatorContext, TransformerContext, LoaderContext 
)
from src.pipeline import ETLPipelineContext


# ! Extraction config 
def customer_ext_func(ctx:ExtractorContext, deps:list[IComponentContext]) -> MetaFrame: 
  data = pd.read_csv(ctx.source) 
  encoding = {} 
  return MetaFrame(data, encoding) 

def customer_ref_ext_func(ctx:ExtractorContext, deps:list[IComponentContext]) -> MetaFrame: 
  data = pd.read_csv(ctx.source).head() 
  encoding = {} 
  return MetaFrame(data, encoding) 

ext_ops = {'source':f'{DATA_RAW}/customers_0.csv'} 
ext_factory = ETLFactory(ExtractorContext) 
cus_ext = ext_factory.make_component('customer_ext', customer_ext_func, ext_ops) 
cus_ext_ref = ext_factory.make_component('customer_ext_ref', customer_ref_ext_func, ext_ops) 


# ! Validation config 
customer_ref_encoding = { 
  'cid': {},    # Not empty string 
  'fname': {},  # Not empty string 
  'lname': {},  # Not empty string 
  'sex': {"F":"Female", "M":'Male'},    # Not empty string 
  'bday': {},   # correct date format 
  'phone': {},  # regex 
  'email': {},  # regex testing 
  'address': {}, # Not empty string 
  'pcode': {},  # regex testing 
  'region': {}, # must be in list of region 
} 
customer_validity_mapper:SerieValidityMapper = { 
  'cid': lambda df,c:pd.notnull(df[c]),    # Not empty string 
  'fname': lambda df,c:pd.notnull(df[c]),  # Not empty string 
  'lname': lambda df,c:pd.notnull(df[c]),  # Not empty string 
  'sex': lambda df,c:pd.notnull(df[c]),    # Not empty string 
  'bday': lambda df,c:pd.notnull(df[c]),   # correct date format 
  'phone': lambda df,c:pd.notnull(df[c]),  # regex 
  'email': lambda df,c:pd.notnull(df[c]),  # regex testing 
  'address': lambda df,c:pd.notnull(df[c]), # Not empty string 
  'pcode': lambda df,c:pd.notnull(df[c]),  # regex testing 
  'region': lambda df,c:pd.notnull(df[c]), # must be in list of region 
} 


common_ctx = { 'validity_mapper':customer_validity_mapper } 
val_deps = [cus_ext.ctx, cus_ext_ref.ctx] 
val_factory = ETLFactory(ValidatorContext) 
validators = val_factory.make_batch([ 
  ('customer_completeness', vl.validation_degree_completeness, common_ctx, val_deps),
  ('customer_validity', vl.validation_degree_validity, common_ctx, val_deps),
  ('customer_summary', vl.validation_distribution_summary, common_ctx, val_deps),
  ('customer_encoding_drift', vl.validation_encoding_drift, common_ctx, val_deps),
  ('customer_schema_drift', vl.validation_schema_drift, common_ctx, val_deps),
])

# ! Transformer config 
# Configure transformer logger 
transformer_ctx = TransformerContext( 
    name='customer_merge', 
    imputer_mapper={}, 
    options={'on':'cid', 'how':'left'} ) 

cus_merge_trf = ETLComponent[TransformerContext](tf.mock_merge_transform, transformer_ctx, [cus_ext.ctx]) 

# ! Loader config 
def customer_loading_func(ctx:LoaderContext, deps:list[IComponentContext]) -> MetaFrame: 
  # ! use a fixed destination 
  data, *_ = [ d.result.data for d in deps] 
  data.to_csv(ctx.destination, index=False) # ! BUG HERE !
  return None 

ldr_factory = ETLFactory(LoaderContext) 
ops = {'destination':f'{DATA_FINAL}/customers_1.csv'}
cus_ldr = ldr_factory.make_component('customer_loader', customer_loading_func, ops, [cus_merge_trf.ctx]) 

# ! ETLPipelineContext
cus_pipe_ctx = ETLPipelineContext( 
  name='customer pipeline', env='dev', version='0', 
  extractors=[cus_ext, cus_ext_ref], 
  validators=validators, 
  transformers=[cus_merge_trf], 
  loaders=[cus_ldr]) 
