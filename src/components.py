from dataclasses import dataclass, field 
from typing import Protocol

from utils.metaframe import MetaFrame 
from src.errors import DependencyError 
from src.interface import IComponentContext



@dataclass
class ExtractorContext(IComponentContext): 
  source:str = '' 

@dataclass
class LoaderContext(IComponentContext): 
  destination: str = '' 

@dataclass
class TransformerContext(IComponentContext): 
  imputer_mapper: dict = field(default_factory=dict) 

@dataclass 
class ValidatorContext(IComponentContext): 
  validity_mapper: dict = field(default_factory=dict) 



class ETLComponentFunc[T:IComponentContext](Protocol): 
  def __call__(self, ctx:T, deps:list[IComponentContext]) -> MetaFrame: 
    ...



@dataclass
class ETLComponent[T:IComponentContext]: 
  func:ETLComponentFunc[T] 
  ctx:T 
  deps:list[T] 

  def execute(self): 
    try: 
      # ! raise exception if dependencies are incorrect 
      dep_errors = [ d for d in self.deps if not d.success ] 
      if dep_errors: 
        raise DependencyError(self.ctx.name, dep_errors) 
      self.ctx.result = self.func(self.ctx, self.deps) 
      self.ctx.success = True 
    except DependencyError as e: 
      self.ctx.success = False 
      self.ctx.error = e 
    except Exception as e: 
      self.ctx.success = False 
      self.ctx.error = e # ! wrap exception here with a type of specific type failure ? 
