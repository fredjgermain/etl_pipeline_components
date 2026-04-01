from dataclasses import dataclass, field 

from src.interface import IComponentContext
from src.components import (
  ETLComponent, ExtractorContext, ValidatorContext, TransformerContext, LoaderContext
)

@dataclass
class ETLPipelineContext: 
  name:str = '' 
  env: str = '' 
  version: str = '' 
  
  # Components 
  extractors: list[ETLComponent[ExtractorContext]] = field(default_factory=list) 
  validators: list[ETLComponent[ValidatorContext]] = field(default_factory=list) 
  transformers: list[ETLComponent[TransformerContext]] = field(default_factory=list) 
  loaders: list[ETLComponent[LoaderContext]] = field(default_factory=list) 


@dataclass
class EtlPipeline: 
  ctx: ETLPipelineContext 

  def run_pipeline(self): 
    # ! run the whole pipeline 
    self.run_extractor() 
    self.run_validator() 
    self.run_transformer() 
    self.run_loader()  
 
  # ! run components ------------------------------ 
  def run_node[T:IComponentContext](self, node:ETLComponent[T]): 
    node.execute() 
  
  def run_extractor(self, selector:list[str] = None): 
    components = self.__select_components__(self.ctx.extractors, selector) 
    [ self.run_node(c) for c in components ] 
      
  def run_validator(self,  selector:list[str] = None): 
    components = self.__select_components__(self.ctx.validators, selector) 
    [ self.run_node(c) for c in components ] 

  def run_transformer(self,  selector:list[str] = None): 
    components = self.__select_components__(self.ctx.transformers, selector) 
    [ self.run_node(c) for c in components ] 
      
  def run_loader(self,  selector:list[str] = None): 
    components = self.__select_components__(self.ctx.loaders, selector) 
    [ self.run_node(c) for c in components ] 
  
  
  
  @staticmethod
  def __select_components__[T:IComponentContext](targets:list[ETLComponent[T]], selector:list[str]=None) -> list[ETLComponent[T]]:
    if not selector:
      return [ c for c in targets] 
    return [ c for c in targets if c.ctx.name in selector] 