from utils.utils import degree_validity, distributional_summary, degree_completeness, schema_drift, encoding_drift 
from utils.metaframe import MetaFrame 
from src.interface import IComponentContext
from src.components import ValidatorContext


def validation_degree_validity(ctx:ValidatorContext, deps:list[IComponentContext]): 
  data = [ d.result for d in deps ][0].data 
  deg_validity = degree_validity(data, ctx.validity_mapper) 
  return MetaFrame(deg_validity.to_frame(), {}) 

def validation_distribution_summary(ctx:ValidatorContext, deps:list[IComponentContext]): 
  data = [ d.result for d in deps ][0].data 
  summary = distributional_summary(data) 
  return MetaFrame(summary, {}) 

def validation_degree_completeness(ctx:ValidatorContext, deps:list[IComponentContext]): 
  data = [ d.result for d in deps ][0].data 
  df_completeness = degree_completeness(data) 
  return MetaFrame(df_completeness, {})

def validation_schema_drift(ctx:ValidatorContext, deps:list[IComponentContext]): 
  ref, new = [ d.result for d in deps ] 
  _, df_drift =  schema_drift(ref.data, new.data) 
  return MetaFrame(df_drift, {})

def validation_encoding_drift(ctx:ValidatorContext, deps:list[IComponentContext]): 
  ref, new = [ d.result for d in deps ] 
  _, df_drift = encoding_drift(ref.encoding, new.encoding) 
  return MetaFrame(df_drift, {}) 
