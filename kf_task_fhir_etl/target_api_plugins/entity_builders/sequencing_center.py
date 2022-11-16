"""
Builds FHIR Organization resources (https://www.hl7.org/fhir/organization.html)
from rows of tabular sequencing center data.
"""
import inspect
from abc import abstractmethod

from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_task_fhir_etl.common.utils import not_none, drop_none, yield_resource_ids

# Build a dictionary mapping KF IDs to sequencing center names
module = constants.SEQUENCING.CENTER
sequencing_center_name = {}
for name, cls in inspect.getmembers(module):
    if inspect.isclass(cls):
        try:
            kf_id = getattr(cls, "KF_ID")
            name = getattr(cls, "NAME")
            sequencing_center_name[kf_id] = name
        except AttributeError:
            pass


class SequencingCenter:
    class_name = "sequencing_center"
    api_path = "Organization"
    target_id_concept = None
    service_id_fields = None

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        return {
            "identifier": not_none(record[CONCEPT.SEQUENCING.CENTER.TARGET_SERVICE_ID])
        }

    @classmethod
    def query_target_ids(cls, host, key_components):
        return list(yield_resource_ids(host, cls.api_path, drop_none(key_components)))

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        sequencing_center_id = record[CONCEPT.SEQUENCING.CENTER.TARGET_SERVICE_ID]
        name = sequencing_center_name.get(sequencing_center_id)

        entity = {
            "resourceType": cls.api_path,
            "id": get_target_id_from_record(cls, record),
            "meta": {
                "profile": [f"http://hl7.org/fhir/StructureDefinition/{cls.api_path}"]
            },
            "identifier": [
                {
                    "use": "official",
                    "system": "https://kf-api-dataservice.kidsfirstdrc.org/sequencing-centers/",
                    "value": sequencing_center_id,
                }
            ],
        }

        # name
        if name:
            entity["name"] = name

        return entity

    @abstractmethod
    def submit(cls, host, body):
        pass
