import os
from src.utils import Config, RandomGenerator

ENTITY_TYPE = "user"
ID_TYPE = "id"
ATTRIBUTE_TYPE = "invite-code"
RELATION_TYPE = "friendship"

config = Config()
random = RandomGenerator(config.random_seed)
os.makedirs(f"{os.getcwd()}/{config.dataset_dir}", exist_ok=True)

with open(f"{os.getcwd()}/{config.dataset_dir}/entities.tql", "w") as output:
    for entity_id in range(1, config.entity_count + 1):
        query = f"""insert $e isa {ENTITY_TYPE}; $e has {ID_TYPE} {entity_id};"""

        for _ in range(config.attributes_per_entity):
            query += f""" $e has {ATTRIBUTE_TYPE} "{random.str(8)}";"""

        output.write(query + "\n")

with open(f"{os.getcwd()}/{config.dataset_dir}/relations.tql", "w") as output:
    for _ in range(config.relation_count):
        query = f"""match $e1 isa {ENTITY_TYPE}; $e1 has {ID_TYPE} {random.int(config.entity_count)};"""
        query += f""" $e2 isa {ENTITY_TYPE}; $e2 has {ID_TYPE} {random.int(config.entity_count)};"""
        query += f""" insert ($e1, $e2) isa {RELATION_TYPE};"""
        output.write(query + "\n")
