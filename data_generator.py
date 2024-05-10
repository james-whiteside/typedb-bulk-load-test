from random import Random
import os

RNG_SEED = 0
RANDOM_CHAR_SET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
DATASET_DIR = "dataset"
ENTITY_COUNT = 524288
ATTRIBUTE_COUNT = 10
RELATION_COUNT = 524288
ENTITY_TYPE = "user"
ID_TYPE = "id"
ATTRIBUTE_TYPE = "invite-code"
RELATION_TYPE = "friendship"


def generate_char(rng: Random) -> str:
    return rng.choice(RANDOM_CHAR_SET)


def generate_str(rng: Random, length: int) -> str:
    return "".join(generate_char(rng) for _ in range(length))


rng = Random(RNG_SEED)
os.makedirs(f"{os.getcwd()}/{DATASET_DIR}", exist_ok=True)

with open(f"{os.getcwd()}/{DATASET_DIR}/entities.tql", "w") as output:
    for entity_id in range(1, ENTITY_COUNT + 1):
        query = f"""insert $e isa {ENTITY_TYPE}; $e has {ID_TYPE} {entity_id};"""

        for _ in range(ATTRIBUTE_COUNT):
            query += f""" $e has {ATTRIBUTE_TYPE} "{generate_str(rng, 8)}";"""

        output.write(query + "\n")

with open(f"{os.getcwd()}/{DATASET_DIR}/relations.tql", "w") as output:
    for _ in range(RELATION_COUNT):
        query = f"""match $e1 isa {ENTITY_TYPE}; $e1 has {ID_TYPE} {rng.randint(1, ENTITY_COUNT)};"""
        query += f""" $e2 isa {ENTITY_TYPE}; $e2 has {ID_TYPE} {rng.randint(1, ENTITY_COUNT)};"""
        query += f""" insert ($e1, $e2) isa {RELATION_TYPE};"""
        output.write(query + "\n")
