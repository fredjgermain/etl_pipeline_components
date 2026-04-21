
import datetime
import pandas as pd
from dataclasses import dataclass
from typing import Annotated

from data_simulator.entity import Entity 
from data_simulator.context import EntityContext 
from data_simulator.annotations.primaries import (PrimaryKey, CreationTime, ForeignKey, PkCtx) 
from data_simulator.annotations.generator import ( 
  GenCtx, GenNormal, GenUniform, GenFaker, GenPattern, CustomGen, 
  GenCategorical, GenPoisson
) 
from data_simulator.utils import generator 
from data_simulator.simulator import DataSimulator 

from data_validation.annotations import ( Completeness, Uniqueness, ValidCategory )
from data_validation.validation_profile import ValidationProfile


# Region ==================================================
@dataclass
class Region(Entity):
    region_id:  Annotated[int, PrimaryKey()]
    founded_at: Annotated[datetime.datetime, CreationTime(
                    start=datetime.datetime(1998, 1, 1),
                    end=datetime.datetime(2002, 1, 1),
                )]
    name:       Annotated[str,  GenFaker("city")]
    code:       Annotated[str,  GenPattern(r'[A-Z]{2}-\d{3}')]


# Customer ==================================================
def customer_id_fn(seed, ctx:PkCtx) -> pd.Series: 
  return generator.generate_ids(seed, ctx.N) 

def age_group(seed, ctx:GenCtx) -> pd.Series: 
  return ctx.current_data['age'].apply( lambda a: "senior" if a >= 65 else "adult" if a >= 30 else "young" ) 

SEX_CATEGORIES = ['male', 'female']

@dataclass
class Customer(Entity):
    customer_id: Annotated[str, PrimaryKey(fn=customer_id_fn)]
    created_at:  Annotated[datetime.datetime, CreationTime(
                     start=datetime.datetime(2015, 1, 1),
                     end=datetime.datetime(2023, 12, 31),
                 )]
    region_id:   Annotated[int, ForeignKey(Region)]
    email:       Annotated[str, GenFaker("email")]
    sexe:        Annotated[str, GenCategorical(categories=SEX_CATEGORIES)] 
    age:         Annotated[int, GenNormal(min=18, max=90, mean=40, std=15, rounding=0)]
    code:        Annotated[str, GenPattern(r'CUST-[A-Z]{3}-\d{4}')]
    age_group:   Annotated[str, CustomGen(fn=age_group)]
    wage:        Annotated[str, GenPoisson(min=0, mean=50000, std=20000)]


# Customer validation .....................................
class CustomerValidationProfile(ValidationProfile): 
  customer_id:  Annotated[int, Uniqueness(), Completeness(0.0)] 
  sexe:         Annotated[int, ValidCategory(SEX_CATEGORIES), Completeness(0.01)] 
  age:          Annotated[int, Completeness(0.0)] # ! ValidDistribution HERE 



# Transaction ==================================================
@dataclass
class Transaction(Entity):
    transaction_id: Annotated[int, PrimaryKey()]
    created_at:     Annotated[datetime.datetime, CreationTime(
                        start=datetime.datetime(2015, 1, 1),
                        end=datetime.datetime(2024, 12, 31),
                    )]
    customer_id:    Annotated[str,   ForeignKey(Customer)]
    region_id:      Annotated[int,   ForeignKey(Region)]
    amount:         Annotated[float, GenNormal(min=0, mean=150, std=80, rounding=2)]
    quantity:       Annotated[int,   GenUniform(min=1, max=10, rounding=0)]
    ref:            Annotated[str,   GenPattern(r'TXN-\d{8}')]
    # fault injections
    amount_nulls:   Annotated[float, GenNormal(min=0, mean=150, std=80, rounding=2)]
    ref_dupes:      Annotated[str,   GenPattern(r'[A-Z]{3}-\d{4}')]





# Simulation ============================================== 
entities = { 
  Region:      EntityContext(Region,      N=8), 
  Customer:    EntityContext(Customer,    N=200), 
  Transaction: EntityContext(Transaction, N=1000), 
} 

sim = DataSimulator(entities) 
try: 
  sim.simulate() 
  print(sim.get_summary()) 
except:
  print(sim.get_failures()) 

pre_data = sim.get_data(generated=False) 
new_data = sim.get_data(preexisting=False) # ! Get newly generated data





