from .core import create_tables
from typing import Dict, Any
from datetime import datetime
import json
import logging
import logging.handlers
from pythonjsonlogger import jsonlogger
import duckdb

logger = logging.getLogger()
formatter = jsonlogger.JsonFormatter()


# def run(
#     orchestration_config: Dict[str, Any],
#     c: duckdb.DuckDBPyConnection,
# ):
#     handler = logging.FileHandler(orchestration_config["settings"]["log_path"])
#     logger.addHandler(handler)
#     logger.setLevel(logging.INFO)
#     handler.setFormatter(formatter)
#
#     orchestration_log = {
#         "start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
#         "config": orchestration_config,
#     }
#
#     print(json.dumps(orchestration_config, indent=4))
#
#     if input("PROCEED: Y/[N]").upper() == "Y":
#         completed_components = []
#
#         components_to_build = [
#             key
#             for key in orchestration_config.keys()
#             if key not in ["orchestration", "catalog"]
#         ]
#
#         while set(completed_components) != set(components_to_build):
#             for component in components_to_build:
#                 if "prerequisites" in orchestration_config[component].keys():
#                     if orchestration_config[component]["prerequisites"]:
#                         if set(
#                             [
#                                 prerequisite["name"]
#                                 for prerequisite in orchestration_config[component][
#                                     "prerequisites"
#                                 ]
#                             ]
#                         ).issubset(set(completed_components)) and (
#                             component not in completed_components
#                         ):
#                             with open(
#                                 orchestration_config[component]["config_path"],
#                                 "r",
#                             ) as f:
#                                 config = yaml.safe_load(f)
#
#                             print("\n")
#                             print(component)
#                             print("\n")
#                             logger.removeHandler(handler)
#                             create_tables(config=config, c=c)
#                             logger.addHandler(handler)
#                             completed_components.append(component)
#
#                     elif component not in completed_components:
#                         with open(
#                             orchestration_config[component]["config_path"],
#                             "r",
#                         ) as f:
#                             config = yaml.safe_load(f)
#
#                         print(component)
#                         print("\n")
#                         logger.removeHandler(handler)
#                         create_tables(config=config, c=c)
#                         logger.addHandler(handler)
#                         completed_components.append(component)
#
#                 elif component not in completed_components:
#                     with open(
#                         orchestration_config[component]["config_path"],
#                         "r",
#                     ) as f:
#                         config = yaml.safe_load(f)
#
#                     print("\n")
#                     logger.removeHandler(handler)
#                     create_tables(config=config, c=c)
#                     logger.addHandler(handler)
#                     completed_components.append(component)
#
#     orchestration_log["end_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#     logging.log(logging.INFO, orchestration_log)
#     logging.log(
#         logging.INFO, {"completed": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
#     )
