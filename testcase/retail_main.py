
from src.pipeline import EtlPipeline, ETLComponent,IComponentContext
#import src.config.customer_config as conf 
from testcase.retail_config import cus_pipe_ctx


def main() -> None:
  """
  Load config 
  init pipeline 
  exec ETL 
  """
  
  # ! Pipeline configs 
  etl = EtlPipeline(ctx = cus_pipe_ctx) 
  
  etl.run_pipeline() 
  components:list[ETLComponent[IComponentContext]] = [*etl.ctx.extractors, *etl.ctx.validators, *etl.ctx.transformers, *etl.ctx.loaders]
  for c in components: 
    print(c.ctx.name, c.ctx.success) # ! loader failed ERRNO 2
    if not c.ctx.success:
      print(c.ctx.error)

  
  # etl.run_extractor() 
  # etl.run_validator() 
  # etl.run_transformer() 
  # etl.run_loader() 
  #etl.write_logs() 

main()