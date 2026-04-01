import pandas as pd
from typing import Protocol

from utils.metaframe import MetaFrame
from src.interface import IComponentContext
from src.components import TransformerContext


class TransformerFunc(Protocol): 
  def __call__(self, ctx:TransformerContext, deps:list[IComponentContext]) -> MetaFrame: 
    ...

# ! Test this bit separately **kwargs: Unpack[MergeArgs] 
def merge_transform(ctx:TransformerContext, deps:list[IComponentContext]) -> MetaFrame: 
  mfs = [ d.result for d in deps ] 
  dfa, dfb, *_ = mfs
  encoding = dfa.encoding 
  df = pd.merge(dfa, dfb, **ctx.options) 
  return MetaFrame(df, encoding) 

def mock_merge_transform(ctx:TransformerContext, deps:list[IComponentContext]) -> MetaFrame: 
  dfa, *_ = [ d.result for d in deps ] 
  encoding = dfa.encoding 
  dfmerged = pd.merge(dfa.data, dfa.data, **ctx.options) 
  return MetaFrame(dfmerged, encoding) 
