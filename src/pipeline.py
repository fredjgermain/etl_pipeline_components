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
  components: list[ETLComponent[IComponentContext]] = field(default_factory=list)
  

@dataclass
class EtlPipeline: 
  ctx: ETLPipelineContext 

  # ! run components ------------------------------ 
  def run(self, selector: list[type[IComponentContext] | str] = None): 
    components = self.select_components(selector)
    [c.execute() for c in components]
  
  def select_components( 
    self, 
    selector: list[type[IComponentContext] | str] = None 
  ) -> list[ETLComponent[IComponentContext]]: 
    if not selector: 
      return self.ctx.components 
    
    return [ c 
            for s in selector 
            for c in self.ctx.components 
            if ( isinstance(s,type) and isinstance(c,s) ) or ( isinstance(s,str) and c.ctx.name == s ) 
            ] 
  